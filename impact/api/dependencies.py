from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import HTTPException, Query, status

from config import settings
from impact.domain.models import CanonicalBundle, MetricContext
from impact.exceptions import ImpactError, ParseError  # For sanitized raising to handlers
from impact.ledger.ledger import Ledger

# Allowed base directories for dump paths (prevents path traversal).
# Always-allowed base directories (prevents path traversal while keeping tests
# and CLI scripts working). Extra dirs can be added via DEVRANK_ALLOWED_DUMP_DIRS.
_DEFAULT_ALLOWED_BASES = [Path("/tmp"), Path.home() / ".devrank", Path.cwd()]
_extra = (
    [Path(p) for p in settings.allowed_dump_dirs.split(":") if p.strip()]
    if settings.allowed_dump_dirs
    else []
)
ALLOWED_DUMP_BASES: list[Path] = _DEFAULT_ALLOWED_BASES + _extra


def validate_dump_path(dump_path: str) -> Path:
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
        detail="dump_path is outside allowed directories.",
    )


def _parse_iso_date(value: str | None, field_name: str) -> datetime | None:
    """Parse an ISO date string with a user-friendly error message.

    Raises ParseError (sanitized by handlers) instead of raw HTTPException
    detail leak.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError) as e:
        # Safe msg; full exc logged in ParseError handler
        raise ParseError(
            f"Invalid date format for '{field_name}': expected ISO 8601",
            # No {value!r} to avoid injection; logged internally
        ) from e


def load_manifest(dump_path: str) -> dict:
    """Read and return the dump_manifest.json from a validated dump directory.

    Uses safe errors that handlers sanitize (no JSON err details leaked).
    """
    import json

    validated = validate_dump_path(dump_path)
    manifest_path = validated / "dump_manifest.json"
    if not manifest_path.exists():
        # HTTP kept for simple (safe detail); caught by ValueError handler
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="dump_manifest.json not found in the provided dump directory.",
        )
    try:
        return json.loads(manifest_path.read_text())
    except json.JSONDecodeError as e:
        # Raise ParseError for consistency with handler sanitization/logging
        raise ParseError("Invalid manifest JSON") from e


def load_bundle(dump_path: str) -> CanonicalBundle:
    """Load a CanonicalBundle from a validated dump directory.

    Wraps any error in ImpactError (base handler sanitizes; full details logged).
    """
    from impact.ingestion.dump import DumpIngestion

    validated = validate_dump_path(dump_path)
    try:
        return DumpIngestion(str(validated)).ingest()
    except Exception as e:
        # Safe msg only; status_code=400 for thin handler (consistent)
        # type(e) logged in root helper
        raise ImpactError("Failed to load dump bundle", status_code=400) from e


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
    start_date: str | None = Query(None, description="ISO 8601 start date", examples=["2026-01-19T00:00:00Z"]),
    end_date: str | None = Query(None, description="ISO 8601 end date", examples=["2026-01-29T00:00:00Z"]),
) -> MetricContext:
    """Build MetricContext from query params (used by GET /metrics/{slug})."""
    return build_context(
        dump_path,
        user_login,
        _parse_iso_date(start_date, "start_date"),
        _parse_iso_date(end_date, "end_date"),
    )
