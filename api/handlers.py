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
# Common errors from metrics/ledger (ValueError etc.)


async def base_impact_error_handler(request: Request, exc: ImpactError) -> JSONResponse:
    """Base handler for impact errors (hierarchy catch-all)."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "impact_error", "detail": str(exc), "type": type(exc).__name__},
    )


async def validation_error_handler(request: Request, exc: DataValidationError) -> JSONResponse:
    """Specific for DataValidationError (400)."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "data_validation_error",
            "detail": str(exc),
            "field": exc.field,
            "value": exc.value,
        },
    )


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
    """For parsing errors (422)."""
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
    status_code = status.HTTP_502_BAD_GATEWAY if exc.status_code and exc.status_code >= 500 else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=status_code,
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


# Handler for common metrics/ledger errors (e.g. ValueError from bad context)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"error": "value_error", "detail": str(exc)},
    )


def register_exception_handlers(application: FastAPI) -> None:
    application.add_exception_handler(DataValidationError, validation_error_handler)
    application.add_exception_handler(ManifestNotFoundError, manifest_not_found_handler)
    application.add_exception_handler(ManifestInvalidError, manifest_invalid_handler)
    application.add_exception_handler(ParseError, parse_error_handler)
    application.add_exception_handler(ProviderError, provider_error_handler)
    application.add_exception_handler(AdapterError, adapter_error_handler)
    application.add_exception_handler(ResponseError, response_error_handler)
    application.add_exception_handler(ValueError, value_error_handler)
    application.add_exception_handler(ImpactError, base_impact_error_handler)
