from __future__ import annotations

import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from impact.api.dependencies import build_context, get_metric_context_query
from impact.api.schemas import (
    ComputeMetricsRequest,
    MetricListItem,
    MetricResponse,
    MetricsReport,
    Rating,
)
from impact.config.role_metrics import get_role_config
from impact.domain.models import MetricContext
from impact.metrics import get_metrics
from impact.scripts.generate_report import get_metric_rating

router = APIRouter(tags=["metrics"], prefix="/metrics")

# Cache metric metadata (slug/name/description are static; avoid re-instantiation per request)
_METRIC_LIST_CACHE: list[MetricListItem] | None = None


def _get_metric_list() -> list[MetricListItem]:
    global _METRIC_LIST_CACHE
    if _METRIC_LIST_CACHE is None:
        _METRIC_LIST_CACHE = [
            MetricListItem(slug=slug, name=metric().name, description=metric().description)
            for slug, metric in get_metrics().items()
        ]
    return _METRIC_LIST_CACHE


def _resolve_rating(metric_slug: str, details: dict, role: str = "default") -> Rating:
    """Compute the real rating for a metric result using thresholds + role config."""
    role_config = get_role_config(role)
    rating_str = get_metric_rating(metric_slug, details, role_config)
    try:
        return Rating(rating_str)
    except ValueError:
        return Rating.UNKNOWN


# All endpoints use plain `def` (not async) because the underlying work
# (disk I/O, metric computation) is synchronous. FastAPI automatically runs
# sync endpoints in a thread pool, preventing event-loop blocking.


@router.get("/", summary="List available metrics", response_model=list[MetricListItem])
def list_metrics() -> list[MetricListItem]:
    return _get_metric_list()


@router.post("/compute", response_model=MetricsReport, summary="Compute metrics report")
def compute_metrics(req: ComputeMetricsRequest) -> MetricsReport:
    # Build context directly from the request body (no double-parsing via Depends)
    context = build_context(req.dump_path, req.user_login, req.start_date, req.end_date)

    available = get_metrics()
    slugs = req.metric_slugs or list(available.keys())
    metrics_results = []
    for slug in slugs:
        if slug not in available:
            continue
        metric = available[slug]()
        result = metric.run(context)
        rating = _resolve_rating(slug, result.details)
        metrics_results.append(
            MetricResponse(
                slug=slug,
                name=metric.name,
                description=metric.description,
                rating=rating,
                summary=result.summary,
                details=result.details,
            )
        )
    return MetricsReport(
        user_login=req.user_login,
        start_date=req.start_date,
        end_date=req.end_date,
        metrics=metrics_results,
        data_summary={"metrics_run": len(metrics_results)},
    )


@router.get("/{metric_slug}", response_model=MetricResponse, summary="Resolve single metric")
def resolve_single_metric(
    metric_slug: str,
    context: Annotated[MetricContext, Depends(get_metric_context_query)],
    role: str = Query("default", description="Role for rating thresholds"),
) -> MetricResponse:
    available = get_metrics()
    if metric_slug not in available:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Metric not found")
    metric = available[metric_slug]()
    result = metric.run(context)
    rating = _resolve_rating(metric_slug, result.details, role)
    return MetricResponse(
        slug=metric_slug,
        name=metric.name,
        description=metric.description,
        rating=rating,
        summary=result.summary,
        details=result.details,
    )


# Test-only endpoint — only registered when DEVRANK_DEBUG is set.
if os.environ.get("DEVRANK_DEBUG", "").lower() in ("1", "true", "yes"):

    @router.get("/_test_error")
    def _test_error(error_type: str):
        from impact.exceptions import (
            AdapterError,
            DataValidationError,
            ImpactError,
            ManifestInvalidError,
            ManifestNotFoundError,
            ParseError,
            ProviderError,
            ResponseError,
        )
        exc_map = {
            "DataValidationError": DataValidationError("test validation"),
            "ManifestNotFoundError": ManifestNotFoundError("test not found"),
            "ManifestInvalidError": ManifestInvalidError("test invalid"),
            "ParseError": ParseError("test parse"),
            "ProviderError": ProviderError("test provider"),
            "AdapterError": AdapterError("test adapter"),
            "ResponseError": ResponseError("test response"),
            "ImpactError": ImpactError("test base"),
            "ValueError": ValueError("test metrics/ledger error"),
        }
        if error_type in exc_map:
            raise exc_map[error_type]
        raise ImpactError("unknown test error")
