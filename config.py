"""Centralized configuration — sensible dev defaults, env-var overrides.

Every setting has a hardcoded default that works for local development
(``localhost`` Postgres + Redis on standard ports).  Environment variables
override any default so production / Docker / CI can inject values without
touching code.
"""
from __future__ import annotations

import os

# ── Database ──────────────────────────────────────────────────────────
DATABASE_URL: str = os.environ.get(
    "DEVRANK_DATABASE_URL",
    "postgresql+asyncpg://devrank:devrank@localhost:5432/devrank",
)
DATABASE_URL_SYNC: str = os.environ.get(
    "DEVRANK_DATABASE_URL_SYNC",
    "postgresql://devrank:devrank@localhost:5432/devrank",
)

# ── Redis ─────────────────────────────────────────────────────────────
REDIS_URL: str = os.environ.get(
    "DEVRANK_REDIS_URL",
    "redis://localhost:6379/0",
)

# ── Celery ────────────────────────────────────────────────────────────
CELERY_BROKER_URL: str = os.environ.get(
    "CELERY_BROKER_URL",
    "redis://localhost:6379/0",
)
CELERY_BACKEND_URL: str = os.environ.get(
    "CELERY_BACKEND_URL",
    "redis://localhost:6379/1",
)

# ── API ───────────────────────────────────────────────────────────────
CORS_ORIGINS: list[str] = [
    o.strip()
    for o in os.environ.get("DEVRANK_CORS_ORIGINS", "*").split(",")
]

# ── Security ──────────────────────────────────────────────────────────
ALLOWED_DUMP_DIRS: str = os.environ.get("DEVRANK_ALLOWED_DUMP_DIRS", "")
DEBUG: bool = os.environ.get("DEVRANK_DEBUG", "").lower() in ("1", "true", "yes")

# ── Auth ──────────────────────────────────────────────────────────────
SECRET_KEY: str = os.environ.get(
    "DEVRANK_SECRET_KEY",
    "local-dev-secret-change-in-production",
)
GITHUB_TOKEN: str | None = os.environ.get("GITHUB_TOKEN")
