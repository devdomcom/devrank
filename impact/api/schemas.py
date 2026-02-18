from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
import uuid

from pydantic import BaseModel, Field

from impact.domain.models import CanonicalBundle, MetricContext, MetricResult
# Auth schemas for JWT/RBAC (DRY with FastAPI OAuth2; security utils use these)


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
    signal_type: str = "authored"


class MetricResponse(BaseModel):
    """Full metric result with rating (used by compute and single-metric endpoints)."""
    slug: str
    name: str
    description: str
    category: str
    signal_type: str = "authored"
    rating: Rating
    summary: str
    details: dict[str, Any]
    continuous_score: float | None = None


class GroupScore(BaseModel):
    """Per-category group score (group-averaged scoring)."""
    category: str
    name: str
    score: float
    metric_count: int


class MetricsReport(BaseModel):
    """Atomic report model covering full metrics pipeline output."""
    user_login: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    metrics: list[MetricResponse]
    group_scores: list[GroupScore] | None = None
    overall_score: float | None = None
    data_summary: dict[str, Any] | None = None


class ComputeMetricsRequest(BaseModel):
    """Request for metrics computation.

    ``dump_path`` and ``role`` are required.  ``user_login``, ``start_date``,
    and ``end_date`` are inferred from the dump manifest when omitted.
    """
    dump_path: str = Field(examples=["impact/samples/github_live_dump"])
    role: str = Field(examples=["senior_dev"])
    user_login: str | None = Field(None, examples=["msyavuz"])
    start_date: datetime | None = Field(None, examples=["2026-01-01T00:00:00Z"])
    end_date: datetime | None = Field(None, examples=["2026-01-31T23:59:59Z"])
    metric_slugs: list[str] | None = None  # optional filter


class TimeWindow(BaseModel):
    """Time window spec for comparisons (start/end inferred from manifest if omitted)."""
    start_date: datetime | None = Field(None, examples=["2026-01-01T00:00:00Z"])
    end_date: datetime | None = Field(None, examples=["2026-01-31T23:59:59Z"])


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


# Auth schemas for JWT/RBAC (DRY with security utils; Pydantic for validation)
class Token(BaseModel):
    """OAuth2 token response (standard for /token endpoint)."""

    access_token: str
    token_type: str = "bearer"


class AuthContext(BaseModel):
    """Current authenticated user context from JWT claims + DB lookup.

    Includes roles/perms from RBAC tables (system/app via user_role_assignments)
    for dep checks. Injected via Depends (request-scoped); used in routes/services.
    Follows FastAPI security pattern; no internal details leaked.
    Full for /me endpoint Swagger testing.
    """

    user_id: uuid.UUID
    roles: list[str] = Field(default_factory=list)  # e.g., ['superuser']
    permissions: list[str] = Field(
        default_factory=list
    )  # resource:action e.g., ['system:admin']
    # org_ids, dept_ids for scoping (extend as needed)


# Pipeline objects already Pydantic in domain; re-export for API atomicity
# + auth for JWT/RBAC
__all__ = [
    "Rating",
    "GroupScore",
    "MetricListItem",
    "MetricResponse",
    "MetricsReport",
    "ComputeMetricsRequest",
    "RoleListItem",
    "RoleResponse",
    "TimeWindow",
    "CompareMetricsRequest",
    "MetricComparison",
    "MetricsComparisonReport",
    "CanonicalBundle",
    "MetricContext",
    "MetricResult",
    # Auth
    "Token",
    "AuthContext",
]
