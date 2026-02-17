"""Exception handlers for the FastAPI application.

Registration order matters: Starlette matches exception handlers by exact type
first. Register specific subclasses BEFORE their base classes so that e.g.
DataValidationError is caught by its own handler (400) rather than falling
through to the ImpactError catch-all (500). The base ImpactError handler must
be registered LAST.

Revised to leverage ImpactError class pattern (enhanced with to_response()/
safe_detail) + thin root helper (errors.py):
- Classes handle sanitization (DRY, no leaks per AGENTS.md/Security).
- Thin logging only (no repetition/complexity).
- Responses consistent/safe; full details logged.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

# Thin root helper for logging (reusable; classes handle response)
from errors import log_error
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


async def base_impact_error_handler(request: Request, exc: ImpactError) -> JSONResponse:
    """Single thin handler for all ImpactError subclasses (sufficient per request).

    - Leverages class to_response()/safe_detail for sanitization (DRY, class-driven).
    - Uses exc.status_code if present (e.g., ResponseError/ProviderError for 502/503)
      else defaults 500; logs full via root helper.
    - Eliminates 7 subclass handlers (lean; no repetition/complexity).
    """
    # Full internals logged (helper); sanitized class response
    log_error(exc, request)

    # Leverage status_code from exc (Provider/Response/etc) or default; consistent
    # with HTTPException handling
    status_code = getattr(exc, "status_code", None) or status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(
        status_code=status_code,
        content=exc.to_response(),
    )


async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """Wraps built-in ValueError as ImpactError for class pattern (kept as specified)."""
    # Convert to ImpactError for consistent sanitization (thin wrapper)
    impact_exc = ImpactError(str(exc))
    log_error(exc, request)  # Log original
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=impact_exc.to_response(),
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """For HTTPException (routes/deps): wrap as ImpactError for class pattern
    (kept as specified). Ensures consistent sanitized response; logs original.
    """
    # Use ImpactError for to_response(); safe_detail from HTTP
    impact_exc = ImpactError(exc.detail)
    log_error(exc, request)  # Log full HTTP exc
    return JSONResponse(
        status_code=exc.status_code,
        content=impact_exc.to_response(),
    )


def register_exception_handlers(application: FastAPI) -> None:
    """Register all exception handlers.

    Order: HTTPException/ValueError before base ImpactError (MUST last).
    Starlette resolves by exact type. Subclasses of ImpactError now consolidated
    to one handler (lean; status_code from exc for Provider/Response 502/503).
    """
    # Built-in types (kept)
    application.add_exception_handler(HTTPException, http_exception_handler)
    application.add_exception_handler(ValueError, value_error_handler)
    # Base catch-all for ImpactError hierarchy (sufficient; MUST be last)
    application.add_exception_handler(ImpactError, base_impact_error_handler)
