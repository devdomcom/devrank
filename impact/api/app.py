from __future__ import annotations

from fastapi import APIRouter, FastAPI

from impact.api.routes.health import router as health_router

API_V1_PREFIX = "/api/v1"


def create_app() -> FastAPI:
    application = FastAPI(
        title="DevRank Impact API",
        description="Engineering impact and quality metrics",
        version="0.1.0",
    )

    v1 = APIRouter(prefix=API_V1_PREFIX)
    v1.include_router(health_router)
    application.include_router(v1)

    return application


app = create_app()
