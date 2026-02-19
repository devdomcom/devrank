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
    status_code: int | None = None,
) -> None:
    """Log exception with severity based on status code.

    - 4xx (client errors): WARNING, one-line, no traceback.
    - 5xx / unknown: ERROR with full traceback.

    Args:
        exc: The exception to log.
        request: Optional FastAPI request for path/method context.
        extra: Additional log-only context (never in client response).
        status_code: HTTP status code; inferred from exc if not provided.
    """
    # Infer status code from the exception if not explicitly passed
    if status_code is None:
        status_code = getattr(exc, "status_code", None) or 500

    # Build safe log context (no secrets/PII)
    log_extra: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "status_code": status_code,
    }
    if request:
        log_extra["path"] = request.url.path
        log_extra["method"] = request.method
    if extra:
        log_extra.update(extra)

    msg = f"{type(exc).__name__}: {str(exc)[:500]}"

    if status_code < 500:
        # Expected client error — one-line warning, no traceback
        logger.warning(msg, extra=log_extra)
    else:
        # Unexpected server error — full traceback
        logger.exception(msg, extra=log_extra)
