"""Exception handlers for the FastAPI application (app-level).

Catches ``AppError`` (base for all app + domain errors) with class-driven
sanitization. Thin logging via root ``errors.py`` helper.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from api.exceptions import AppError
from errors import log_error


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    """Catch-all for AppError hierarchy (auth errors, impact errors, etc.).

    Leverages class to_response()/safe_detail for sanitization.
    """
    log_error(exc, request)
    status_code = getattr(exc, "status_code", None) or status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(
        status_code=status_code,
        content=exc.to_response(),
    )


async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """Wraps built-in ValueError as AppError for consistent responses."""
    app_exc = AppError(str(exc))
    log_error(exc, request)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=app_exc.to_response(),
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTPException with a neutral error type."""
    log_error(exc, request)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "request_error", "detail": exc.detail},
    )


def register_exception_handlers(application: FastAPI) -> None:
    """Register all exception handlers.

    Order: HTTPException/ValueError before base AppError (MUST be last).
    """
    application.add_exception_handler(HTTPException, http_exception_handler)
    application.add_exception_handler(ValueError, value_error_handler)
    application.add_exception_handler(AppError, app_error_handler)
