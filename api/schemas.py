"""App-wide API schemas (infrastructure, platform-level responses)."""
from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Liveness probe — no external calls, instant response."""
    status: str
    service: str


class ServiceHealth(BaseModel):
    """Health status for a single backing service."""
    status: str
    latency_ms: float | None = None
    version: str | None = None
    error: str | None = None


class InfraHealthResponse(BaseModel):
    """Detailed health check for all backing infrastructure."""
    status: str
    postgres: ServiceHealth
    redis: ServiceHealth
