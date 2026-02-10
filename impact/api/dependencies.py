from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Query, status

from impact.api.schemas import ComputeMetricsRequest
from impact.domain.models import CanonicalBundle, MetricContext
from impact.ledger.ledger import Ledger


# Reusable Depends chain for metrics pipeline (bundle -> ledger -> context)
# Follows FastAPI best practices: explicit, typed, composable, error-raising.
def get_canonical_bundle(dump_path: str) -> CanonicalBundle:
    """Load bundle from dump (chain step; errors to HTTP)."""
    from impact.ingestion.dump import DumpIngestion
    try:
        ingestion = DumpIngestion(dump_path)
        return ingestion.ingest()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load bundle: {e}",
        ) from e


def get_ledger(
    bundle: Annotated[CanonicalBundle, Depends(get_canonical_bundle)],
) -> Ledger:
    """Ledger from bundle (reusable chain)."""
    return Ledger(bundle)


def get_metric_context(
    req: ComputeMetricsRequest,  # body auto-injected to dep; triggers chain
) -> MetricContext:
    """Full pipeline context dep (bundle->ledger->context; no bypass in endpoint)."""
    # Explicit chain (sub Depends)
    bundle = get_canonical_bundle(req.dump_path)
    ledger = get_ledger(bundle)
    return MetricContext(
        ledger=ledger,
        user_login=req.user_login,
        start_date=req.start_date,
        end_date=req.end_date,
    )


# Query-based dep for dynamic GET /metrics/{slug} (reuses chain; accepts report params)
def get_metric_context_query(
    dump_path: str = Query(..., description="Dump path for bundle"),
    user_login: str = Query(..., description="User for metrics"),
    start_date: str | None = Query(None, description="ISO start"),
    end_date: str | None = Query(None, description="ISO end"),
) -> MetricContext:
    """MetricContext via query (injects bundle->ledger chain; for individual metric)."""
    bundle = get_canonical_bundle(dump_path)
    ledger = get_ledger(bundle)
    from datetime import datetime
    return MetricContext(
        ledger=ledger,
        user_login=user_login,
        start_date=datetime.fromisoformat(start_date) if start_date else None,
        end_date=datetime.fromisoformat(end_date) if end_date else None,
    )
