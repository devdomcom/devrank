from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from impact.api.dependencies import build_context, get_metric_context_query
from impact.api.schemas import (
    CompareMetricsRequest,
    ComputeMetricsRequest,
    GroupScore,
    MetricComparison,
    MetricListItem,
    MetricResponse,
    MetricsComparisonReport,
    MetricsReport,
    Rating,
    TimeWindow,
)
from impact.config.categories import compute_group_scores, get_category_name
from impact.config.role_metrics import get_available_roles, get_role_config
from impact.domain.models import MetricContext
from impact.metrics import get_metrics
from impact.scripts.generate_report import get_metric_rating
from impact.thresholds import get_continuous_score

router = APIRouter(tags=["metrics"], prefix="/metrics")

# Cache metric metadata (slug/name/description are static; avoid re-instantiation per request)
_METRIC_LIST_CACHE: list[MetricListItem] | None = None


def _get_metric_list() -> list[MetricListItem]:
    global _METRIC_LIST_CACHE
    if _METRIC_LIST_CACHE is None:
        _METRIC_LIST_CACHE = [
            MetricListItem(slug=slug, name=metric().name, description=metric().description, category=metric().category, signal_type=metric().signal_type)
            for slug, metric in get_metrics().items()
        ]
    return _METRIC_LIST_CACHE


def _validate_role(role: str) -> None:
    """Raise 400 if role YAML doesn't exist."""
    available = get_available_roles()
    if role not in available:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Role '{role}' not found. Available roles: {available}",
        )


def _resolve_rating_and_score(
    metric_slug: str, details: dict, role: str
) -> tuple[Rating, float | None]:
    """Compute rating + continuous_score (DRY extension of existing pattern)."""
    role_config = get_role_config(role)
    rating_str = get_metric_rating(metric_slug, details, role_config)
    try:
        rating = Rating(rating_str)
    except ValueError:
        rating = Rating.UNKNOWN
    score = get_continuous_score(metric_slug, details, role_config)
    return rating, score


# All endpoints use plain `def` (not async) because the underlying work
# (disk I/O, metric computation) is synchronous. FastAPI automatically runs
# sync endpoints in a thread pool, preventing event-loop blocking.


@router.get("/", summary="List available metrics", response_model=list[MetricListItem])
def list_metrics() -> list[MetricListItem]:
    return _get_metric_list()


@router.post("/compute", response_model=MetricsReport, summary="Compute metrics report")
def compute_metrics(req: ComputeMetricsRequest) -> MetricsReport:
    _validate_role(req.role)
    context = build_context(req.dump_path, req.user_login, req.start_date, req.end_date)

    available = get_metrics()
    slugs = req.metric_slugs or list(available.keys())
    if req.metric_slugs:
        unknown = [s for s in req.metric_slugs if s not in available]
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown metric slug(s): {unknown}. Use GET /metrics/ to list valid slugs.",
            )
    metrics_results = []
    for slug in slugs:
        metric = available[slug]()
        result = metric.run(context)
        rating, score = _resolve_rating_and_score(slug, result.details, req.role)
        metrics_results.append(
            MetricResponse(
                slug=slug,
                name=metric.name,
                description=metric.description,
                category=metric.category,
                signal_type=metric.signal_type,
                rating=rating,
                summary=result.summary,
                details=result.details,
                continuous_score=score,
            )
        )
    # Compute group-averaged scores (shared logic)
    pairs = [
        (mr.continuous_score, mr.category)
        for mr in metrics_results
        if mr.continuous_score is not None
    ]
    overall_raw, group_map = compute_group_scores(pairs)
    overall = round(overall_raw, 1) if overall_raw is not None else None
    gs_list = [
        GroupScore(
            category=cat,
            name=get_category_name(cat),
            score=round(score, 1),
            metric_count=sum(1 for mr in metrics_results if mr.category == cat and mr.continuous_score is not None),
        )
        for cat, score in group_map.items()
    ]

    return MetricsReport(
        user_login=context.user_login,
        start_date=context.start_date,
        end_date=context.end_date,
        metrics=metrics_results,
        group_scores=gs_list or None,
        overall_score=overall,
        data_summary={"metrics_run": len(metrics_results)},
    )


@router.get("/{metric_slug}", response_model=MetricResponse, summary="Resolve single metric")
def resolve_single_metric(
    metric_slug: str,
    context: Annotated[MetricContext, Depends(get_metric_context_query)],
    role: str = Query(..., description="Role for rating thresholds"),
) -> MetricResponse:
    _validate_role(role)
    available = get_metrics()
    if metric_slug not in available:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Metric not found")
    metric = available[metric_slug]()
    result = metric.run(context)
    rating, score = _resolve_rating_and_score(metric_slug, result.details, role)
    return MetricResponse(
        slug=metric_slug,
        name=metric.name,
        description=metric.description,
        category=metric.category,
        signal_type=metric.signal_type,
        rating=rating,
        summary=result.summary,
        details=result.details,
        continuous_score=score,
    )


@router.post("/compare", response_model=MetricsComparisonReport, summary="Compare metrics across two time windows")
def compare_metrics(req: CompareMetricsRequest) -> MetricsComparisonReport:
    """Compare metrics in two windows (reuses build/resolve; score_delta always higher=better)."""
    _validate_role(req.role)
    # Build separate contexts for each window (shared dump/user; dates from req)
    context1 = build_context(
        req.dump_path, req.user_login, req.window1.start_date, req.window1.end_date
    )
    context2 = build_context(
        req.dump_path, req.user_login, req.window2.start_date, req.window2.end_date
    )

    available = get_metrics()
    slugs = req.metrics or list(available.keys())
    if req.metrics:
        unknown = [s for s in req.metrics if s not in available]
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown metric slug(s): {unknown}. Use GET /metrics/ to list valid slugs.",
            )
    comparisons = []
    for slug in slugs:
        metric = available[slug]()
        # Run independently for each window
        res1 = metric.run(context1)
        res2 = metric.run(context2)
        # Resolve ratings + scores (role shared; score_delta always higher=better per breakpoints)
        rating1, score1 = _resolve_rating_and_score(slug, res1.details, req.role)
        rating2, score2 = _resolve_rating_and_score(slug, res2.details, req.role)
        score_delta = (score2 - score1) if score1 is not None and score2 is not None else None
        comparisons.append(
            MetricComparison(
                slug=slug,
                name=metric.name,
                description=metric.description,
                category=metric.category,
                rating1=rating1,
                rating2=rating2,
                summary1=res1.summary,
                summary2=res2.summary,
                details1=res1.details,
                details2=res2.details,
                continuous_score1=score1,
                continuous_score2=score2,
                score_delta=score_delta,
            )
        )
    return MetricsComparisonReport(
        user_login=context1.user_login,
        window1=req.window1,
        window2=req.window2,
        comparisons=comparisons,
        role=req.role,
        data_summary={"metrics_compared": len(comparisons)},
    )


# Test-only endpoint — only registered when DEVRANK_DEBUG is set.
from config import DEBUG as _DEBUG
if _DEBUG:

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
