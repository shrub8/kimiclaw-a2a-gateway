"""Env-based settings. Names, defaults and the .env fallback match the original app.py."""
import logging
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOG = logging.getLogger("kimiclaw")


def _load_token() -> str:
    tok = os.environ.get("KIMICLAW_TOKEN")
    if tok:
        return tok.strip()
    with open(os.path.join(BASE_DIR, ".env")) as f:
        for line in f:
            if line.startswith("KIMICLAW_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("KIMICLAW_TOKEN not set")


TOKEN = _load_token()
NOTIFY_URL = os.environ.get("GATEWAY_NOTIFY_URL", "").strip()
NOTIFY_AUTH_MODE = os.environ.get("GATEWAY_NOTIFY_AUTH_MODE", "raw").lower()
NOTIFY_AUTH_USER = os.environ.get("GATEWAY_NOTIFY_AUTH_USER", "agent")
NOTIFY_EVENTS = {e.strip() for e in os.environ.get(
    "GATEWAY_NOTIFY_EVENTS", "task_completed,task_failed").split(",") if e.strip()}
NOTIFY_TIMEOUT = 10
NOTIFY_FAIL_LIMIT = int(os.environ.get("GATEWAY_NOTIFY_FAIL_LIMIT", "5"))
NOTIFY_COOLDOWN_S = int(os.environ.get("GATEWAY_NOTIFY_COOLDOWN_S", "900"))
LOG_PAYLOADS = os.environ.get("GATEWAY_LOG_PAYLOADS", "0") == "1"
MAX_TEXT = 200_000
AGENT_TIMEOUT = 560           # seconds, passed to openclaw
AGENT_CONCURRENCY = 3         # concurrent agent turns
TASK_TTL = 24 * 3600          # task retention
