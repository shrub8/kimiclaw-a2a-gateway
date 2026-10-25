"""Bearer auth middleware: exact string match, same exemptions as the original."""
from fastapi import Request
from fastapi.responses import JSONResponse

from gateway.config import TOKEN

PUBLIC_PATHS = {"/.well-known/agent.json"}


async def bearer_auth(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    if request.url.path in PUBLIC_PATHS:
        return await call_next(request)
    authz = request.headers.get("authorization", "")
    if authz != f"Bearer {TOKEN}":
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)
