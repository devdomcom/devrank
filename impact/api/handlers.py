"""Exception handlers for the FastAPI application.

Registration order matters: Starlette matches exception handlers by exact type
first. Register specific subclasses BEFORE their base classes so that e.g.
DataValidationError is caught by its own handler (400) rather than falling
through to the ImpactError catch-all (500). The base ImpactError handler must
be registered LAST.
"""
from __future__ import annotations

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

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
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "impact_error", "detail": str(exc), "type": type(exc).__name__},
    )


async def validation_error_handler(request: Request, exc: DataValidationError) -> JSONResponse:
    body: dict[str, object] = {
        "error": "data_validation_error",
        "detail": str(exc),
    }
    if exc.field is not None:
        body["field"] = exc.field
    if exc.value is not None:
        body["value"] = str(exc.value)
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=body)


async def manifest_not_found_handler(request: Request, exc: ManifestNotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "manifest_not_found", "detail": str(exc), "path": exc.path},
    )


async def manifest_invalid_handler(request: Request, exc: ManifestInvalidError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"error": "manifest_invalid", "detail": str(exc), "path": exc.path},
    )


async def parse_error_handler(request: Request, exc: ParseError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "error": "parse_error",
            "detail": str(exc),
            "source": exc.source,
            "line": exc.line_number,
        },
    )


async def provider_error_handler(request: Request, exc: ProviderError) -> JSONResponse:
    code = (
        status.HTTP_502_BAD_GATEWAY
        if exc.status_code and exc.status_code >= 500
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return JSONResponse(
        status_code=code,
        content={"error": "provider_error", "detail": str(exc), "provider": exc.provider},
    )


async def adapter_error_handler(request: Request, exc: AdapterError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "adapter_error", "detail": str(exc), "adapter": exc.adapter},
    )


async def response_error_handler(request: Request, exc: ResponseError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "response_error", "detail": str(exc), "details": exc.details},
    )


async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"error": "value_error", "detail": str(exc)},
    )


def register_exception_handlers(application: FastAPI) -> None:
    """Register all exception handlers.

    Order: most specific subclasses first, base ImpactError last.
    Starlette resolves handlers by exact type match, so subclass handlers
    must be registered before their parent to take precedence.
    """
    # Specific subclasses (registered first to take priority)
    application.add_exception_handler(DataValidationError, validation_error_handler)
    application.add_exception_handler(ManifestNotFoundError, manifest_not_found_handler)
    application.add_exception_handler(ManifestInvalidError, manifest_invalid_handler)
    application.add_exception_handler(ParseError, parse_error_handler)
    application.add_exception_handler(ProviderError, provider_error_handler)
    application.add_exception_handler(AdapterError, adapter_error_handler)
    application.add_exception_handler(ResponseError, response_error_handler)
    # Built-in types
    application.add_exception_handler(ValueError, value_error_handler)
    # Base catch-all (MUST be last)
    application.add_exception_handler(ImpactError, base_impact_error_handler)
