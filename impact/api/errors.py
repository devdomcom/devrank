"""Centralized error utility for sanitized responses and secure logging.

Follows AGENTS.md/Security best practices:
- **Never leak internals**: No exception class names, tracebacks, stack traces, file paths,
  or implementation details in responses (prevents info disclosure attacks).
- **User-friendly messages**: Always safe, actionable details (e.g., "Invalid input"
  vs. raw Python errors).
- **Full logging**: Use logger.exception() for complete debug info (incl. traceback)
  -- available only server-side, never to clients.
- **Consistency**: Standard JSON shape for all errors; enables OpenAPI docs + tests.
- **DRY**: Replaces duplicated HTTPException/str(exc)/type() patterns in handlers,
  dependencies, routes (e.g., dumps.py, dependencies.py).
- **Extensibility**: Specific safe_detail/error_type per case; integrates with
  existing ImpactError hierarchy and registration order.

Usage:
    from impact.api.errors import log_and_sanitize_error
    ...
    except SomeError as e:
        # In handlers: return log_and_sanitize_error(e, request, safe_detail=..., status_code=...)
    # Or raise HTTPException -- wrap in handlers.

This is a plain utility (not dep) per Package Organization best practice; keeps
business logic separate from FastAPI request extraction.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def log_and_sanitize_error(
    exc: Exception,
    request: Request | None = None,
    *,
    error_type: str | None = None,
    safe_detail: str | None = None,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    """Log full exception details internally and return sanitized JSON response.

    - Logs: Complete traceback + context (request path/method, exc type) via
      logger.exception() -- secure, for ops/debug only.
    - Response: Consistent, safe shape; no internals (prevents recon/enumeration).
    - Truncates long messages to avoid log bombing; friendly fallback for users.
    - Used by handlers to enforce policy (e.g., update base_impact_error_handler).

    Args:
        exc: The raw exception (logged fully).
        request: Optional for logging context (path/method).
        error_type: Short code (e.g., "validation_error"); defaults to exc class.
        safe_detail: User-facing message; defaults to generic safe string.
        status_code: HTTP status.
        extra: Additional log-only fields (never in response).

    Returns:
        JSONResponse with sanitized body.
    """
    # Sanitize for response (security: no PII/paths/stacks)
    if error_type is None:
        error_type = "internal_error"  # Generic; avoid leaking class names like "ParseError"
    if safe_detail is None:
        # Friendly generic; override per error type for actionability
        safe_detail = (
            "An internal error occurred. Please try again or contact support."
        )

    # Logging context: full details for devs/ops (structured, truncated)
    log_extra: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "status_code": status_code,
    }
    if request:
        # Safe: path/method only; no query params/body that might contain secrets
        log_extra["path"] = request.url.path
        log_extra["method"] = request.method
    if extra:
        log_extra.update(extra)

    # logger.exception auto-includes traceback + exc info
    logger.exception(
        f"API error [{error_type}]: {str(exc)[:500]}...",  # Truncate to prevent bloat
        extra=log_extra,
    )

    # Consistent response (typed, no internals; supports OpenAPI via handlers)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": error_type,
            "detail": safe_detail,
        },
    )
