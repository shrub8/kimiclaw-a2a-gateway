"""New in this refactor: liveness and readiness probes (behind the bearer token like every non-card path)."""
import shutil

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/healthz")
async def healthz():
    return {"status": "ok"}


@router.get("/readyz")
async def readyz():
    if shutil.which("openclaw") is None:
        return JSONResponse({"status": "not ready", "reason": "openclaw binary not found on PATH"}, status_code=503)
    return {"status": "ready"}
