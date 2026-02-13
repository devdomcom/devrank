from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

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
    category: str


class MetricResponse(BaseModel):
    """Full metric result with rating (used by compute and single-metric endpoints)."""
    slug: str
    name: str
    description: str
    category: str
    rating: Rating
    summary: str
    details: dict[str, Any]
    continuous_score: float | None = None


class MetricsReport(BaseModel):
    """Atomic report model covering full metrics pipeline output."""
    user_login: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    metrics: list[MetricResponse]
    data_summary: dict[str, Any] | None = None


class ComputeMetricsRequest(BaseModel):
    """Request for metrics computation.

    ``dump_path`` and ``role`` are required.  ``user_login``, ``start_date``,
    and ``end_date`` are inferred from the dump manifest when omitted.
    """
    dump_path: str = Field(examples=["impact/samples/github_live_dump"])
    role: str = Field(examples=["senior_dev"])
    user_login: str | None = Field(None, examples=["msyavuz"])
    start_date: datetime | None = Field(None, examples=["2026-01-19T00:00:00Z"])
    end_date: datetime | None = Field(None, examples=["2026-01-29T00:00:00Z"])
    metric_slugs: list[str] | None = None  # optional filter


class TimeWindow(BaseModel):
    """Time window spec for comparisons (start/end inferred from manifest if omitted)."""
    start_date: datetime | None = Field(None, examples=["2026-01-19T00:00:00Z"])
    end_date: datetime | None = Field(None, examples=["2026-01-29T00:00:00Z"])


class CompareMetricsRequest(BaseModel):
    """Request for metrics comparison across two windows.

    Reuses dump/role patterns from compute; metrics optional (runs all if omitted).
    """
    dump_path: str = Field(examples=["impact/samples/github_live_dump"])
    metrics: list[str] | None = None
    window1: TimeWindow
    window2: TimeWindow
    role: str = Field(examples=["senior_dev"])
    user_login: str | None = Field(None, examples=["msyavuz"])


class HealthResponse(BaseModel):
    """DRY response model for health (and future root-level status endpoints)."""
    status: str
    service: str


class RoleListItem(BaseModel):
    """Summary for available role (used by GET /roles/)."""
    name: str


class RoleResponse(BaseModel):
    """Full role with config (used by GET /roles/{name})."""
    name: str
    config: dict[str, Any]


class MetricComparison(BaseModel):
    """Per-metric comparison (window1 vs window2 + score_delta; reuses MetricResponse fields).

    Deltas always use continuous scores (score2 - score1; higher always = better per breakpoints).
    Raw details kept for full visibility (no direction assumption on raw values).
    """
    slug: str
    name: str
    description: str
    category: str
    rating1: Rating
    rating2: Rating
    summary1: str
    summary2: str
    details1: dict[str, Any]
    details2: dict[str, Any]
    continuous_score1: float | None = None
    continuous_score2: float | None = None
    score_delta: float | None = None  # score2 - score1 (positive = improvement)


class MetricsComparisonReport(BaseModel):
    """Comparison report across two windows (DRY extension of MetricsReport)."""
    user_login: str
    window1: TimeWindow
    window2: TimeWindow
    comparisons: list[MetricComparison]
    role: str
    data_summary: dict[str, Any] | None = None


# Pipeline objects already Pydantic in domain; re-export for API atomicity
__all__ = [
    "Rating",
    "MetricListItem",
    "MetricResponse",
    "MetricsReport",
    "ComputeMetricsRequest",
    "HealthResponse",
    "RoleListItem",
    "RoleResponse",
    "TimeWindow",
    "CompareMetricsRequest",
    "MetricComparison",
    "MetricsComparisonReport",
    "CanonicalBundle",
    "MetricContext",
    "MetricResult",
]
