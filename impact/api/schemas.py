from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel

from impact.domain.models import CanonicalBundle, MetricContext, MetricResult


class Rating(str, Enum):
    """Metric rating levels (from thresholds/role config)."""
    EXCELLENT = "excellent"
    GOOD = "good"
    NEUTRAL = "neutral"
    BAD = "bad"
    DESCRIPTIVE = "descriptive"
    UNKNOWN = "unknown"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class MetricListItem(BaseModel):
    """Summary info for a single metric (used by GET /metrics/)."""
    slug: str
    name: str
    description: str


class MetricResponse(BaseModel):
    """Full metric result with rating (used by compute and single-metric endpoints)."""
    slug: str
    name: str
    description: str
    rating: Rating
    summary: str
    details: dict[str, Any]


class MetricsReport(BaseModel):
    """Atomic report model covering full metrics pipeline output."""
    user_login: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    metrics: list[MetricResponse]
    data_summary: dict[str, Any] | None = None


class ComputeMetricsRequest(BaseModel):
    """Request for metrics computation (triggers Depends chain)."""
    user_login: str
    dump_path: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    metric_slugs: list[str] | None = None  # optional filter


class HealthResponse(BaseModel):
    """DRY response model for health (and future root-level status endpoints)."""
    status: str
    service: str


# Pipeline objects already Pydantic in domain; re-export for API atomicity
__all__ = [
    "Rating",
    "MetricListItem",
    "MetricResponse",
    "MetricsReport",
    "ComputeMetricsRequest",
    "HealthResponse",
    "CanonicalBundle",
    "MetricContext",
    "MetricResult",
]
