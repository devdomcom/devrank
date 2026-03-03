"""Application entry point — creates the FastAPI app and mounts all routers.

Root-level routes (health, auth) live in api/routes/.
Domain-specific routes (metrics, roles, dumps) are mounted from impact.api.
"""
from __future__ import annotations

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.handlers import register_exception_handlers
# Root app routes now include organizations + departments (platform tenancy)
from api.routes import assessments_router, auth_router, departments_router, global_roles_router, infra_health_router, organizations_router, positions_router, scenarios_assessment_router, scenarios_router, users_router
from config import settings
from impact.api.routes import dumps_router, metrics_router

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

    # ── Versioned routes ─────────────────────────────────────────────────
    v1 = APIRouter(prefix=API_V1_PREFIX)
    # Auth (app-level)
    v1.include_router(auth_router)
    # Platform tenancy (organizations + departments + positions; RBAC restricted)
    # Mounted at /api/v1/organizations/, /api/v1/organizations/{org}/departments/,
    # and /api/v1/organizations/{org}/positions/
    v1.include_router(organizations_router)
    v1.include_router(departments_router)
    v1.include_router(positions_router)
    # Platform users management (system admins only)
    v1.include_router(users_router)
    # Platform global roles (no org scope; platform-wide defaults)
    v1.include_router(global_roles_router)
    # Assessments (org-scoped visibility)
    v1.include_router(assessments_router)
    # Scenarios (global + assessment-scoped)
    v1.include_router(scenarios_router)
    v1.include_router(scenarios_assessment_router, prefix="/assessments")
    # Impact domain
    v1.include_router(metrics_router)
    v1.include_router(dumps_router)
    application.include_router(v1)

    # Register exception handlers (AppError base catches all domain errors)
    register_exception_handlers(application)

    return application


app = create_app()
