"""Thin, reusable error logging helper at project root.

Provides centralized logging for full exceptions (traceback/context) without
leaking internals to responses. Complements class-based exceptions (e.g.,
ImpactError) for DRYness -- follows AGENTS.md Security ("Don't leak internal
details") and avoids complex utilities/handlers.

Usage (anywhere: API, DB, scripts):
    from errors import log_error
    ...
    except Exception as e:
        log_error(e, request=request, extra={"context": "db_connect"})
        # Then raise/return sanitized response via exception class

- Thin by design: logging only (no sanitization/JSON -- classes handle that).
- Reusable root artifact (not impact-specific; works for FastAPI, Alembic, etc.).
- No repetition: single logger.exception() call with safe extras.

This enables clean, class-leveraging pattern without handler bloat.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Request  # Optional; graceful for non-API use

logger = logging.getLogger(__name__)


def log_error(
    exc: Exception,
    request: Request | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Log full exception details (traceback + context) -- internals never
    surface to users/responses.

    Args:
        exc: Raw exception (full str/trace logged; truncated for sanity).
        request: Optional FastAPI request for path/method (logged only).
        extra: Log-only dict (e.g., {"field": "..."}; never in client response).

    DRY helper used in ImpactError subclasses/handlers to ensure consistent
    server-side logging without duplicating logger calls.
    """
    # Build safe log context (no secrets/PII)
    log_extra: dict[str, Any] = {
        "error_type": type(exc).__name__,
    }
    if request:
        # Safe metadata only
        log_extra["path"] = request.url.path
        log_extra["method"] = request.method
    if extra:
        log_extra.update(extra)

    # exception() auto-includes traceback; truncate msg to avoid bloat
    logger.exception(
        f"{type(exc).__name__}: {str(exc)[:500]}...",
        extra=log_extra,
    )
