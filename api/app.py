from __future__ import annotations

from fastapi import APIRouter, FastAPI

from impact.api.routes import health_router, metrics_router

from .handlers import register_exception_handlers

API_V1_PREFIX = "/api/v1"


def create_app() -> FastAPI:
    application = FastAPI(
        title="DevRank Impact API",
        description="Engineering impact and quality metrics",
        version="0.1.0",
    )

    v1 = APIRouter(prefix=API_V1_PREFIX)
    v1.include_router(health_router)
    v1.include_router(metrics_router)
    application.include_router(v1)

    register_exception_handlers(application)

    return application


app = create_app()
