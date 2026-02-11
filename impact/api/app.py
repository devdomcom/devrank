from __future__ import annotations

import os

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from impact.api.handlers import register_exception_handlers
from impact.api.routes import dumps_router, health_router, metrics_router, roles_router

API_V1_PREFIX = "/api/v1"

# CORS origins — configure via DEVRANK_CORS_ORIGINS env var (comma-separated).
# Defaults to permissive in dev; restrict in production.
_CORS_ORIGINS = os.environ.get("DEVRANK_CORS_ORIGINS", "*").split(",")


def create_app() -> FastAPI:
    application = FastAPI(
        title="DevRank Impact API",
        description="Engineering impact and quality metrics",
        version="0.1.0",
    )

    # CORS middleware
    application.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in _CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    v1 = APIRouter(prefix=API_V1_PREFIX)
    v1.include_router(health_router)
    v1.include_router(metrics_router)
    v1.include_router(roles_router)
    v1.include_router(dumps_router)
    application.include_router(v1)

    register_exception_handlers(application)

    return application


app = create_app()
