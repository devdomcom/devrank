"""Impact pipeline exceptions.

All impact-specific errors inherit from ``ImpactError``, which itself inherits
from ``api.exceptions.AppError`` (the app-level base). This gives the app-level
error handler a single catch-all (``AppError``) for both auth and pipeline errors.

Auth exceptions (``AuthenticationError``, ``AuthorizationError``) live in
``api/exceptions.py`` — they are app-level concerns, not impact-specific.
"""
from __future__ import annotations

from api.exceptions import AppError


class ImpactError(AppError):
    """Base exception for all impact-related errors."""
    pass


class DataValidationError(ImpactError):
    """Raised when data fails validation checks."""

    def __init__(self, message: str, field: str = None, value=None):
        self.field = field
        self.value = value
        super().__init__(message, status_code=400)

    @property
    def safe_detail(self) -> str:
        detail = self._message
        if self.field:
            detail = f"{detail} (field: {self.field})"
        return detail

    @property
    def error_type(self) -> str:
        return "data_validation_error"


class ParseError(ImpactError):
    """Raised when parsing data from external sources fails."""

    def __init__(self, message: str, source: str = None, line_number: int = None):
        self.source = source
        self.line_number = line_number
        super().__init__(message, status_code=422)

    @property
    def safe_detail(self) -> str:
        return f"Parse error: {self._message}"

    @property
    def error_type(self) -> str:
        return "parse_error"


class ManifestError(ImpactError):
    """Base for manifest errors."""

    def __init__(self, message: str, path: str = None, status_code: int | None = None):
        self.path = path
        super().__init__(message, status_code=status_code)

    @property
    def safe_detail(self) -> str:
        return f"Manifest error: {self._message}"


class ManifestNotFoundError(ManifestError):
    """Raised when manifest file is missing (404)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path=path, status_code=404)

    @property
    def safe_detail(self) -> str:
        return "Manifest file not found at the provided dump path."

    @property
    def error_type(self) -> str:
        return "manifest_not_found"


class ManifestInvalidError(ManifestError):
    """Raised when manifest is invalid (JSON/fields; 422)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path=path, status_code=422)

    @property
    def safe_detail(self) -> str:
        return "Manifest file is invalid (check JSON format/fields)."

    @property
    def error_type(self) -> str:
        return "manifest_invalid"


class ProviderError(ImpactError):
    """Raised when a provider (e.g., GitHub API) operation fails."""

    def __init__(self, message: str, provider: str = None, provider_status_code: int = None):
        self.provider = provider
        self.provider_status_code = provider_status_code
        super().__init__(message, status_code=503)

    @property
    def safe_detail(self) -> str:
        return "External provider error (e.g., rate limit or downtime)."


class AdapterError(ImpactError):
    """Raised when adapter processing fails."""

    def __init__(self, message: str, adapter: str = None):
        self.adapter = adapter
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        return "Data processing error."

    @property
    def error_type(self) -> str:
        return "adapter_error"


class ResponseError(ImpactError):
    """Raised for response formatting, parsing, or API output errors."""

    def __init__(self, message: str, status_code: int = 500, details: dict | None = None):
        self.details = details or {}
        super().__init__(message, status_code=status_code)

    @property
    def safe_detail(self) -> str:
        return "Error formatting response."
