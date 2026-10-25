"""Wire shapes. AGENT_CARD and the builders return plain dicts so serialization stays byte-identical."""
import json
import uuid
from datetime import datetime, timezone

AGENT_CARD = {
    "protocolVersion": "1.0",
    "name": "Kimiclaw",
    "description": "A2A gateway into the OpenClaw agent (kimi-claw). Research, code, ops, writing, automation.",
    "url": "https://kimiclaw.hoyack.ai/",
    "provider": {"organization": "hoyack"},
    "version": "0.1.0",
    "capabilities": {
        "streaming": True,
        "pushNotifications": False,
        "stateTransitionHistory": False,
    },
    "securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}},
    "security": [{"bearerAuth": []}],
    "defaultInputModes": ["text"],
    "defaultOutputModes": ["text"],
    "skills": [
        {
            "id": "general-agent",
            "name": "General agent",
            "description": "General-purpose agent: research, coding, shell ops, writing, planning, automation.",
            "tags": ["general-purpose", "agent", "openclaw"],
        }
    ],
    "supportsAuthenticatedExtendedCard": False,
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rpc_error(req_id, code, message, data=None):
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def make_agent_message(text: str) -> dict:
    return {
        "role": "agent",
        "parts": [{"kind": "text", "text": text}],
        "messageId": str(uuid.uuid4()),
    }


def extract_text(msg: dict) -> str:
    out = []
    for p in msg.get("parts") or []:
        kind = p.get("kind")
        if kind == "text":
            out.append(p.get("text", ""))
        elif kind == "data":
            out.append(json.dumps(p.get("data")))
        elif kind == "file":
            name = (p.get("file") or {}).get("name", "?")
            out.append(f"[file: {name}]")
    return "\n".join(out).strip()
