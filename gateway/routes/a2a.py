"""HTTP routes. Handlers stay thin; logic lives in controllers/services."""
from fastapi import APIRouter, Request

from gateway.controllers.rpc import handle_rpc
from gateway.models import AGENT_CARD

router = APIRouter()


@router.get("/")
async def root():
    return {
        "service": "kimiclaw-a2a-gateway",
        "protocol": "A2A 1.0",
        "agent_card": "/.well-known/agent.json",
        "rpc": "POST / (JSON-RPC 2.0)",
    }


@router.get("/.well-known/agent.json")
async def agent_card():
    return AGENT_CARD


@router.post("/")
async def rpc(request: Request):
    return await handle_rpc(request)
