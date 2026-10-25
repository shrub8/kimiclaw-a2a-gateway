"""App factory."""
from fastapi import FastAPI

from gateway.auth import bearer_auth
from gateway.routes.a2a import router


def create_app() -> FastAPI:
    app = FastAPI(title="Kimiclaw A2A Gateway", version="0.1.0")
    app.middleware("http")(bearer_auth)
    app.include_router(router)
    return app


app = create_app()
