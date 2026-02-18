"""Application entry point — creates the FastAPI app and mounts all routers.

Root-level routes (infra health, future auth) live here.
Domain-specific routes (metrics, roles, dumps) are mounted from impact.api.
"""
from __future__ import annotations

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import infra_health_router
from config import settings
from impact.api.handlers import register_exception_handlers
from impact.api.routes import dumps_router, metrics_router, roles_router

API_V1_PREFIX = "/api/v1"


def create_app() -> FastAPI:
    application = FastAPI(
        title="DevRank API",
        description="Engineering impact metrics and platform services",
        version="0.1.0",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.get_cors_origins_list(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── App-wide routes (no version prefix) ──────────────────────────────
    application.include_router(infra_health_router)

    # ── Impact domain routes (v1) ────────────────────────────────────────
    v1 = APIRouter(prefix=API_V1_PREFIX)
    v1.include_router(metrics_router)
    v1.include_router(roles_router)
    v1.include_router(dumps_router)
    application.include_router(v1)

    # Register handlers (using ImpactError class pattern + thin root errors.py
    # helper for logging/sanitization; consistent/reusable per Security best
    # practices and lean design).
    register_exception_handlers(application)

    return application


app = create_app()
