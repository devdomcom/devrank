from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException, Query, status

from impact.domain.models import CanonicalBundle, MetricContext
from impact.ledger.ledger import Ledger

# Allowed base directories for dump paths (prevents path traversal).
# Override via DEVRANK_ALLOWED_DUMP_DIRS env var (colon-separated).
_DEFAULT_ALLOWED_BASES = [Path("/tmp"), Path.home() / ".devrank"]
_ENV_BASES = os.environ.get("DEVRANK_ALLOWED_DUMP_DIRS", "")
ALLOWED_DUMP_BASES: list[Path] = (
    [Path(p) for p in _ENV_BASES.split(":") if p.strip()]
    if _ENV_BASES
    else _DEFAULT_ALLOWED_BASES
)


def _validate_dump_path(dump_path: str) -> Path:
    """Resolve and verify dump_path is within an allowed directory."""
    resolved = Path(dump_path).resolve()
    for base in ALLOWED_DUMP_BASES:
        try:
            resolved.relative_to(base.resolve())
            return resolved
        except ValueError:
            continue
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="dump_path must be within an allowed directory. "
        f"Allowed bases: {[str(b) for b in ALLOWED_DUMP_BASES]}",
    )


def _parse_iso_date(value: str | None, field_name: str) -> datetime | None:
    """Parse an ISO date string with a user-friendly error message."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError) as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid date format for '{field_name}': {value!r}. "
            "Expected ISO 8601 (e.g. 2025-01-01T00:00:00).",
        ) from e


def load_manifest(dump_path: str) -> dict:
    """Read and return the dump_manifest.json from a validated dump directory."""
    import json

    validated = _validate_dump_path(dump_path)
    manifest_path = validated / "dump_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Manifest not found at {manifest_path}",
        )
    try:
        return json.loads(manifest_path.read_text())
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid manifest JSON: {e}",
        ) from e


def load_bundle(dump_path: str) -> CanonicalBundle:
    """Load a CanonicalBundle from a validated dump directory."""
    from impact.ingestion.dump import DumpIngestion

    validated = _validate_dump_path(dump_path)
    try:
        return DumpIngestion(str(validated)).ingest()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load bundle from {dump_path}: {e}",
        ) from e


def build_context(
    dump_path: str,
    user_login: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> MetricContext:
    """Full pipeline: validate path -> load manifest -> load bundle -> create context.

    ``user_login``, ``start_date``, and ``end_date`` are inferred from the
    dump manifest when not provided.
    """
    manifest = load_manifest(dump_path)

    if not user_login:
        user_login = manifest.get("user")
    if not user_login:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="user_login is required (not found in request or manifest).",
        )

    if start_date is None and manifest.get("from"):
        start_date = _parse_iso_date(manifest["from"], "manifest.from")
    if end_date is None and manifest.get("to"):
        end_date = _parse_iso_date(manifest["to"], "manifest.to")

    bundle = load_bundle(dump_path)
    ledger = Ledger(bundle)
    return MetricContext(
        ledger=ledger,
        user_login=user_login,
        start_date=start_date,
        end_date=end_date,
    )


# FastAPI dependency for GET endpoints (query-param based)
def get_metric_context_query(
    dump_path: str = Query(..., description="Path to dump directory"),
    user_login: str | None = Query(None, description="GitHub login (inferred from manifest if omitted)"),
    start_date: str | None = Query(None, description="ISO 8601 start date (inferred from manifest if omitted)"),
    end_date: str | None = Query(None, description="ISO 8601 end date (inferred from manifest if omitted)"),
) -> MetricContext:
    """Build MetricContext from query params (used by GET /metrics/{slug})."""
    return build_context(
        dump_path,
        user_login,
        _parse_iso_date(start_date, "start_date"),
        _parse_iso_date(end_date, "end_date"),
    )
