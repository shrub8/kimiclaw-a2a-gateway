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

from gateway.config import *  # noqa: F401,F403
from gateway.config import LOG, TOKEN
_notify_fails = 0
_notify_disabled_until = 0.0

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

def _last_json_block(raw: str):
    """Find the last parseable top-level JSON object in a noisy stream."""
    idx = raw.rfind("\n{")
    while idx != -1:
        try:
            return json.loads(raw[idx + 1:])
        except json.JSONDecodeError:
            idx = raw.rfind("\n{", 0, idx)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def parse_agent_reply(raw: str) -> str:
    """Best-effort extraction of reply text from openclaw --json output."""
    raw = raw.strip()
    obj = _last_json_block(raw)
    if obj is None:
        return raw
    if isinstance(obj, dict):
        texts = [p["text"] for p in (obj.get("payloads") or [])
                 if isinstance(p, dict) and isinstance(p.get("text"), str) and p["text"]]
        if texts:
            return "\n".join(texts)
        for key in ("reply", "text", "result", "message", "output", "response"):
            v = obj.get(key)
            if isinstance(v, str) and v.strip():
                return v
    return raw


async def _post_json(url: str, payload: dict):
    """Fire-and-forget POST. Authorization shape per NOTIFY_AUTH_MODE:
    basic -> Basic base64(user:KIMICLAW_TOKEN); bearer -> Bearer token; raw -> token."""
    if NOTIFY_AUTH_MODE == "basic":
        import base64
        cred = base64.b64encode(f"{NOTIFY_AUTH_USER}:{TOKEN}".encode()).decode()
        auth = f"Basic {cred}"
    elif NOTIFY_AUTH_MODE == "bearer":
        auth = f"Bearer {TOKEN}"
    else:
        auth = TOKEN
    def _do():
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": auth},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=NOTIFY_TIMEOUT) as resp:
            return resp.status
    return await asyncio.to_thread(_do)


async def notify(event: str, task_id: str, ctx_id: str, extra: dict | None = None):
    global _notify_fails, _notify_disabled_until
    if not NOTIFY_URL or event not in NOTIFY_EVENTS:
        return
    if time.time() < _notify_disabled_until:
        return
    payload = {
        "event": event,
        "source": "kimiclaw-a2a-gateway",
        "taskId": task_id,
        "contextId": ctx_id,
        "timestamp": now(),
    }
    if extra:
        payload.update(extra)
    try:
        status = await _post_json(NOTIFY_URL, payload)
        _notify_fails = 0
        LOG.info("notify %s task=%s -> %s", event, task_id, status)
    except Exception as e:
        _notify_fails += 1
        LOG.warning("notify %s task=%s failed (%d/%d): %s",
                    event, task_id, _notify_fails, NOTIFY_FAIL_LIMIT, e)
        if _notify_fails >= NOTIFY_FAIL_LIMIT:
            _notify_disabled_until = time.time() + NOTIFY_COOLDOWN_S
            _notify_fails = 0
            LOG.error("notify circuit breaker OPEN: outbound webhooks paused %ds",
                      NOTIFY_COOLDOWN_S)


async def run_agent(session_id: str, text: str) -> str:
    cmd = [
        "openclaw", "agent",
        "--local",
        "--session-id", session_id,
        "--message", text,
        "--json",
        "--timeout", str(AGENT_TIMEOUT),
    ]
    LOG.info("agent run start session=%s len=%d", session_id, len(text))
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=AGENT_TIMEOUT + 30)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError("agent timeout")
    if proc.returncode != 0:
        raise RuntimeError(f"agent exited {proc.returncode}: {err.decode(errors='replace')[-500:]}")
    # openclaw --json emits the result envelope on stderr after log noise
    raw_out = out.decode(errors="replace").strip()
    raw_err = err.decode(errors="replace").strip()
    reply = parse_agent_reply(raw_out) if raw_out else None
    if not reply or reply == raw_out and raw_err:
        parsed = parse_agent_reply(raw_err)
        if parsed != raw_err or not reply:
            reply = parsed
    reply = reply or ""
    LOG.info("agent run done session=%s reply_len=%d", session_id, len(reply))
    return reply


# ---------------- task store ----------------

tasks: dict[str, dict] = {}
store_lock = asyncio.Lock()
agent_sem = asyncio.Semaphore(AGENT_CONCURRENCY)


def prune_tasks():
    t = time.time()
    dead = [k for k, v in tasks.items() if v["expires"] < t]
    for k in dead:
        tasks.pop(k, None)


async def execute_task(task_id: str, ctx_id: str, text: str, queue: asyncio.Queue | None):
    session = "a2a-" + re.sub(r"[^A-Za-z0-9-]", "-", ctx_id)[:64]

    async def emit(ev):
        if queue is not None:
            await queue.put(ev)

    entry = tasks[task_id]
    task = entry["task"]
    started = time.time()

    await notify("task_started", task_id, ctx_id)

    await emit({
        "kind": "status-update", "taskId": task_id, "contextId": ctx_id,
        "final": False, "status": {"state": "working", "timestamp": now()},
    })

    async with agent_sem:
        try:
            reply = await run_agent(session, text)
            state = "completed"
        except Exception as e:
            LOG.exception("agent run failed task=%s", task_id)
            reply = f"agent error: {e}"
            state = "failed"

    status = {"state": state, "message": make_agent_message(reply), "timestamp": now()}
    task["status"] = status
    task["final"] = True
    task.setdefault("history", []).append(status["message"])
    if state == "completed":
        artifact = {
            "artifactId": str(uuid.uuid4()),
            "name": "reply",
            "index": 0,
            "parts": [{"kind": "text", "text": reply}],
        }
        task["artifacts"] = [artifact]
        await emit({
            "kind": "artifact-update", "taskId": task_id, "contextId": ctx_id,
            "final": False, "artifact": artifact,
        })
    await emit({
        "kind": "status-update", "taskId": task_id, "contextId": ctx_id,
        "final": True, "status": status,
    })
    if queue is not None:
        await queue.put(None)
    entry["done"].set()
    await notify(f"task_{state}", task_id, ctx_id, {
        "state": state,
        "reply": reply[:4000],
        "durationMs": int((time.time() - started) * 1000),
    })


async def start_task(req_id, params, queue: asyncio.Queue | None):
    msg = params.get("message") or {}
    if msg.get("role") not in (None, "user"):
        return rpc_error(req_id, -32602, "only user-role messages accepted")
    text = extract_text(msg)
    if not text:
        return rpc_error(req_id, -32602, "empty message")
    if len(text) > MAX_TEXT:
        return rpc_error(req_id, -32602, "message too large")

    task_id = msg.get("taskId") or str(uuid.uuid4())
    ctx_id = msg.get("contextId") or str(uuid.uuid4())

    if LOG_PAYLOADS:
        LOG.info("inbound task_id=%s ctx=%s role=%s text=%r", task_id, ctx_id, msg.get("role"), text[:2000])

    async with store_lock:
        prune_tasks()
        tasks[task_id] = {
            "task": {
                "id": task_id, "contextId": ctx_id, "kind": "task",
                "status": {"state": "submitted", "timestamp": now()},
                "artifacts": [], "history": [],
                "final": False,
            },
            "created": time.time(),
            "expires": time.time() + TASK_TTL,
            "done": asyncio.Event(),
        }

    asyncio.create_task(execute_task(task_id, ctx_id, text, queue))
    return {"taskId": task_id, "contextId": ctx_id}


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
