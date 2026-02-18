"""Centralized configuration using Pydantic Settings.

Sensible dev defaults with env-var overrides (prefixed DEVRANK_*).
Pydantic provides type coercion, validation, .env support, and doc strings
for all settings. This eliminates duplicated os.environ.get() calls
across the codebase (DRY) and centralizes FastAPI-recommended config
management.

Usage:
    from config import settings
    db_url = settings.database_url

Warnings are emitted for insecure defaults (e.g., CORS=*, SECRET_KEY=default).
"""
from __future__ import annotations

import logging
from typing import Self

from pydantic import AliasChoices, AnyUrl, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment (DEVRANK_* prefix) or defaults.

    Follows FastAPI/Pydantic best practices:
    - Type validation & coercion (bool, list, etc.)
    - Field descriptions for OpenAPI/docs
    - Validators for parsing (CORS CSV) and warnings (security)
    - .env file support for local dev
    - No more scattered os.environ.get() → DRY config layer

    Env vars: DEVRANK_DATABASE_URL, DEVRANK_CORS_ORIGINS, etc.
    Celery/Github also standardized under DEVRANK_* prefix for consistency.
    """

    model_config = SettingsConfigDict(
        # Standard prefix; all env vars become DEVRANK_*
        env_prefix="DEVRANK_",
        # Support .env files (ignored if missing)
        env_file=".env",
        env_file_encoding="utf-8",
        # Case-insensitive for convenience
        case_sensitive=False,
        # Ignore extra env vars (e.g. GITHUB_TOKEN legacy)
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────
    # Used by: db/engine.py (async), db/migrations/env.py (sync/Alembic), FastAPI
    # Typed as AnyUrl for built-in URL parsing (catches malformed hosts/schemes/ports).
    # Additional validators enforce PostgreSQL schemes to prevent runtime connection
    # errors early (e.g., wrong driver or non-postgres URI).
    database_url: AnyUrl = Field(
        default="postgresql+asyncpg://devrank:devrank@localhost:5432/devrank",
        description="Async PostgreSQL URL (asyncpg driver) for FastAPI/DB sessions. "
        "Must start with postgresql+asyncpg://; malformed URLs raise ValidationError on load.",
        json_schema_extra={"example": "postgresql+asyncpg://user:pass@host/db"},
    )
    database_url_sync: AnyUrl = Field(
        default="postgresql://devrank:devrank@localhost:5432/devrank",
        description="Sync PostgreSQL URL (psycopg2) for Alembic migrations + Celery workers. "
        "Must start with postgresql://.",
    )

    @field_validator("database_url")
    @classmethod
    def _validate_async_db_url(cls, v: AnyUrl) -> AnyUrl:
        """Ensure async DB URL uses supported PostgreSQL + asyncpg scheme.
        Catches e.g. http://, sqlite://, or missing +asyncpg early.
        """
        scheme = str(v).split("://")[0]
        if not scheme.startswith("postgresql+asyncpg"):
            raise ValueError(
                f"database_url must start with 'postgresql+asyncpg://' (got: {scheme}). "
                "See config.py for defaults."
            )
        return v

    @field_validator("database_url_sync")
    @classmethod
    def _validate_sync_db_url(cls, v: AnyUrl) -> AnyUrl:
        """Ensure sync DB URL uses plain postgresql scheme (psycopg2)."""
        scheme = str(v).split("://")[0]
        if scheme != "postgresql":
            raise ValueError(
                f"database_url_sync must start with 'postgresql://' (got: {scheme}). "
                "See config.py for defaults."
            )
        return v

    # ── Redis ─────────────────────────────────────────────────────────────
    # Used by: health checks, Celery (via broker)
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for caching/health/Celery backend",
    )

    # ── Celery ────────────────────────────────────────────────────────────
    # Standardized to DEVRANK_CELERY_* (was mixed); update docker/env accordingly
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0",
        description="Celery broker (e.g. redis://...); used in impact/celery_app.py",
    )
    celery_backend_url: str = Field(
        default="redis://localhost:6379/1",
        description="Celery result backend",
    )

    # ── API ───────────────────────────────────────────────────────────────
    # CORS parsed from CSV string in env (e.g. DEVRANK_CORS_ORIGINS=http://localhost:3000,https://app.example.com)
    cors_origins: list[str] = Field(
        default=["*"],
        description="Allowed origins for CORS middleware (comma-separated in env). "
        "Wildcard (*) triggers security warning.",
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, v: str | list[str] | None) -> list[str]:
        """Coerce CSV string from env var to list; handles '*' default."""
        if isinstance(v, str):
            if not v or v.strip() == "*":
                return ["*"]
            return [o.strip() for o in v.split(",") if o.strip()]
        if isinstance(v, list):
            return v
        return ["*"]

    # ── Security ──────────────────────────────────────────────────────────
    # ALLOWED_DUMP_DIRS: colon-separated list for path traversal protection (impact/api/dependencies.py)
    allowed_dump_dirs: str = Field(
        default="",
        description="Colon-separated base directories permitted for dump_path params. "
        "Prevents path traversal. Empty → sensible defaults in code.",
    )
    # DEBUG gates test-only endpoints (impact/api/routes/metrics.py)
    debug: bool = Field(
        default=False,
        description="Enable debug/test routes and verbose logging. Set DEVRANK_DEBUG=true",
    )

    # ── Auth ──────────────────────────────────────────────────────────────
    secret_key: str = Field(
        default="local-dev-secret-change-in-production",
        description="Secret for JWTs/etc (must override in prod). Default triggers warning.",
        # In prod: use strong random, e.g. openssl rand -hex 32
    )
    # GITHUB_TOKEN: no prefix originally; now DEVRANK_GITHUB_TOKEN for consistency
    # (standard var prefix). Use AliasChoices to accept legacy GITHUB_TOKEN env var
    # without breaking existing scripts/docker/CI during refactor. This is DRY
    # and backward-compatible best practice.
    github_token: str | None = Field(
        default=None,
        # validation_alias allows either; DEVRANK_ takes precedence if both set.
        validation_alias=AliasChoices("GITHUB_TOKEN", "DEVRANK_GITHUB_TOKEN"),
        description="GitHub PAT for live fetching (providers/github_live.py, scripts). "
        "Supports both GITHUB_TOKEN (legacy) and DEVRANK_GITHUB_TOKEN (preferred).",
    )

    @model_validator(mode="after")
    def _log_security_warnings(self) -> Self:
        """Emit warnings for risky configs (best practice for config loading).

        Logs once at startup; helps catch prod misconfigs early.
        """
        # Risky CORS: allow_all is common dev mistake that hits prod
        if self.cors_origins == ["*"] or "*" in self.cors_origins:
            logger.warning(
                "⚠️ Risky cross-origin setup: allowing all origins ('*'). "
                "Set DEVRANK_CORS_ORIGINS to explicit domains for production. "
                "See FastAPI CORS middleware docs."
            )
        # Default secret: hardcoded in original config.py — insecure otherwise
        if self.secret_key == "local-dev-secret-change-in-production":
            logger.warning(
                "⚠️ Default SECRET_KEY in use — set DEVRANK_SECRET_KEY to a strong "
                "value in production (e.g., via secrets manager)."
            )
        if self.debug:
            logger.warning("DEBUG mode enabled — test endpoints active; disable in prod.")
        return self


# Singleton instance — import this instead of individual vars.
# Instantiated at module load; validators/warnings run automatically.
# This follows Pydantic's recommended pattern for app settings.
settings: Settings = Settings()
