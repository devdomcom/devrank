from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import ValidationError
from typing import Annotated

from impact.api.dependencies import get_metric_context, get_metric_context_query
from impact.api.schemas import (
    ComputeMetricsRequest,
    MetricResponse,
    MetricsReport,
    Rating,
)
from impact.config.role_metrics import get_role_config
from impact.domain.models import MetricContext
from impact.metrics import get_metrics
from impact.scripts.generate_report import get_metric_rating

router = APIRouter(tags=["metrics"], prefix="/metrics")


@router.get("/", summary="List available metrics")
async def list_metrics() -> list[dict]:
    metrics_dict = get_metrics()
    return [
        {
            "slug": slug,
            "name": metric().name,
            "description": metric().description,
        }
        for slug, metric in metrics_dict.items()
    ]


@router.post("/compute", response_model=MetricsReport, summary="Compute metrics report")
async def compute_metrics(
    # No body param: resolved to dep (proper injection; chain bundle->ledger->context)
    # MetricContext injected via Depends (best practice, no bypass)
    context: Annotated[MetricContext, Depends(get_metric_context)],
    # Body model for request (FastAPI handles; slugs etc for run)
    req: ComputeMetricsRequest = Body(...),
) -> MetricsReport:
    # Run using injected context (pipeline chain)
    available = get_metrics()
    slugs = req.metric_slugs or list(available.keys())
    metrics_results = []
    for slug in slugs:
        if slug not in available:
            continue
        metric = available[slug]()
        result = metric.run(context)
        # Simple rating stub (extend with get_metric_rating for full)
        rating = "neutral"  # placeholder; real uses thresholds
        metrics_results.append(
            MetricResponse(
                slug=slug,
                name=metric.name,
                description=metric.description,
                rating=Rating(rating),  # from schemas
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


# Dynamic individual metric (follow-up to /metrics list; uses query params + dep injection for context/chain)
@router.get("/{metric_slug}", response_model=MetricResponse, summary="Resolve single metric")
async def resolve_single_metric(
    metric_slug: str,
    # Injects MetricContext via dep (reuses bundle->ledger->context chain; accepts report params like dump_path/role)
    context: Annotated[MetricContext, Depends(get_metric_context_query)],
    # Role for rating (from generate_report.py logic)
    role: str = Query("default", description="Role for rating thresholds/config"),
) -> MetricResponse:
    available = get_metrics()
    if metric_slug not in available:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Metric not found")
    metric = available[metric_slug]()
    result = metric.run(context)
    # Full rating using role (reuses generate_report)
    role_config = get_role_config(role)
    rating_str = get_metric_rating(metric_slug, result.details, role_config)
    return MetricResponse(
        slug=metric_slug,
        name=metric.name,
        description=metric.description,
        rating=Rating(rating_str),
        summary=result.summary,
        details=result.details,
    )


# Test-only endpoint to trigger all handlers (for dedicated error test suite coverage)
# Covers errors from metrics pipeline (e.g. ledger/context failures mapped via handlers)
@router.get("/_test_error")
async def _test_error(error_type: str):
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
        "ValueError": ValueError("test metrics/ledger error"),  # e.g. from metrics run
    }
    if error_type in exc_map:
        raise exc_map[error_type]
    raise ImpactError("unknown test error")
