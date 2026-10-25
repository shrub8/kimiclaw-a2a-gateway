"""Kimiclaw A2A Gateway.

Exposes the local OpenClaw agent over the A2A protocol (v1.0 subset).
Public: agent card at /.well-known/agent.json
Bearer-protected: JSON-RPC at / (message/send, message/stream, tasks/get, tasks/cancel)
"""

import asyncio
import json
import logging
import os
import re
import time
import urllib.request
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from gateway.config import AGENT_TIMEOUT, LOG, TOKEN
from gateway.services.task_store import tasks, start_task  # noqa: E402

app = FastAPI(title="Kimiclaw A2A Gateway", version="0.1.0")

from gateway.models import AGENT_CARD, now, rpc_error, make_agent_message, extract_text  # noqa: E402


# ---------------- auth ----------------

@app.middleware("http")
async def bearer_auth(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    if request.url.path == "/.well-known/agent.json":
        return await call_next(request)
    authz = request.headers.get("authorization", "")
    if authz != f"Bearer {TOKEN}":
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)


# ---------------- helpers ----------------

# ---------------- task store ----------------

# ---------------- routes ----------------

@app.get("/")
async def root():
    return {
        "service": "kimiclaw-a2a-gateway",
        "protocol": "A2A 1.0",
        "agent_card": "/.well-known/agent.json",
        "rpc": "POST / (JSON-RPC 2.0)",
    }


@app.get("/.well-known/agent.json")
async def agent_card():
    return AGENT_CARD


async def sse_events(req_id, queue: asyncio.Queue):
    """Yield SSE events; keepalive-comment every 15s so proxies don't cut idle streams."""
    try:
        while True:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=15)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            if ev is None:
                break
            payload = {"jsonrpc": "2.0", "id": req_id, "result": ev}
            yield f"data: {json.dumps(payload)}\n\n"
            if ev.get("final"):
                break
    except asyncio.CancelledError:
        raise
    except Exception as e:
        LOG.exception("sse error")
        yield f"data: {json.dumps(rpc_error(req_id, -32000, f'stream error: {e}'))}\n\n"


@app.post("/")
async def rpc(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(rpc_error(None, -32700, "parse error"), status_code=400)
    method = body.get("method")
    req_id = body.get("id")
    params = body.get("params") or {}

    if method == "message/send":
        started = await start_task(req_id, params, None)
        if "error" in started:
            return JSONResponse(started)
        task_id = started["taskId"]
        # Non-blocking mode: ?wait=false or params.configuration.blocking=false.
        # Returns the task immediately (A2A polling pattern: client then tracks via
        # tasks/get or the notify webhook). Avoids the Cloudflare ~120s proxy timeout.
        cfg = params.get("configuration") or {}
        wait = request.query_params.get("wait", "true").lower() != "false"
        if cfg.get("blocking") is False:
            wait = False
        if not wait:
            return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": tasks[task_id]["task"]})
        try:
            await asyncio.wait_for(tasks[task_id]["done"].wait(), timeout=AGENT_TIMEOUT + 60)
        except asyncio.TimeoutError:
            return JSONResponse(rpc_error(req_id, -32000, "gateway timeout"))
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": tasks[task_id]["task"]})

    if method == "message/stream":
        queue = asyncio.Queue()
        started = await start_task(req_id, params, queue)
        if "error" in started:
            return JSONResponse(started)
        return StreamingResponse(
            sse_events(req_id, queue), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    if method == "tasks/get":
        task_id = params.get("id") or params.get("taskId")
        entry = tasks.get(task_id)
        if not entry:
            return JSONResponse(rpc_error(req_id, -32001, "task not found"))
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": entry["task"]})

    if method == "tasks/cancel":
        task_id = params.get("id") or params.get("taskId")
        entry = tasks.get(task_id)
        if not entry:
            return JSONResponse(rpc_error(req_id, -32001, "task not found"))
        task = entry["task"]
        if not task.get("final"):
            task["status"] = {"state": "canceled", "timestamp": now()}
            task["final"] = True
            entry["done"].set()
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": task})

    return JSONResponse(rpc_error(req_id, -32601, f"method not found: {method}"))
