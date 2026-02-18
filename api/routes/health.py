"""App-wide health checks — liveness probe and infrastructure readiness."""
from __future__ import annotations

import time

from fastapi import APIRouter

from api.schemas import HealthResponse, InfraHealthResponse, ServiceHealth

router = APIRouter(tags=["health"])

# Regex to strip credentials from connection strings in error messages
_CRED_RE = None


def _sanitize_error(msg: str) -> str:
    """Remove passwords/hostnames from error strings to avoid leaking secrets."""
    import re
    global _CRED_RE
    if _CRED_RE is None:
        _CRED_RE = re.compile(r"://[^@]+@[^/\s]+")
    return _CRED_RE.sub("://***@***", msg)


@router.get("/health", summary="Liveness probe", response_model=HealthResponse)
def health_check() -> HealthResponse:
    """Instant response, no external calls. For load balancers and uptime monitors."""
    return HealthResponse(status="healthy", service="devrank")


@router.get(
    "/health/infra",
    summary="Infrastructure health (Postgres + Redis)",
    response_model=InfraHealthResponse,
)
def infra_health_check() -> InfraHealthResponse:
    """Check connectivity to PostgreSQL and Redis, return version + latency."""
    pg = _check_postgres()
    rd = _check_redis()
    overall = "healthy" if pg.status == "healthy" and rd.status == "healthy" else "degraded"
    return InfraHealthResponse(status=overall, postgres=pg, redis=rd)


def _check_postgres() -> ServiceHealth:
    try:
        from sqlalchemy import text

        from db.engine import SyncSessionLocal

        start = time.monotonic()
        with SyncSessionLocal() as session:
            row = session.execute(text("SELECT version()")).scalar()
            latency = (time.monotonic() - start) * 1000
        return ServiceHealth(status="healthy", latency_ms=round(latency, 2), version=row)
    except Exception as exc:
        return ServiceHealth(status="unhealthy", error=_sanitize_error(str(exc)))


def _check_redis() -> ServiceHealth:
    client = None
    try:
        import redis

        # REDIS_URL now from Pydantic Settings (DRY, validated; was inline import).
        # Supports DEVRANK_REDIS_URL override.
        from config import settings

        client = redis.from_url(
            settings.redis_url, socket_connect_timeout=3, socket_timeout=3
        )
        start = time.monotonic()
        client.ping()
        latency = (time.monotonic() - start) * 1000
        info = client.info("server")
        version = info.get("redis_version", "unknown")
        return ServiceHealth(status="healthy", latency_ms=round(latency, 2), version=version)
    except Exception as exc:
        return ServiceHealth(status="unhealthy", error=_sanitize_error(str(exc)))
    finally:
        if client is not None:
            client.close()
