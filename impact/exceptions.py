"""
Custom exceptions for the impact metrics system.

Enhanced with class-based sanitization (`safe_detail`, `to_response()`) to complement
the thin root logging helper (errors.py). This leverages the existing hierarchy
for DRY, type-safe error responses (per AGENTS.md/Security: friendly, no leaks)
without handler complexity or repetition. Full logging via root helper.

Subclasses override `safe_detail` for user-friendly messages (internals like paths
logged only).
"""


class ImpactError(Exception):
    """Base exception for all impact-related errors.

    Provides `safe_detail` (for responses) and `to_response()` (class-driven dict)
    to keep error logic in exceptions (not scattered handlers). Use with:
        from errors import log_error
        log_error(exc, request)
        return JSONResponse(status=..., content=exc.to_response())
    """

    def __init__(self, message: str, status_code: int | None = None):
        """Extended init for optional status_code (e.g., 400/422/502; used by
        thin handler for correct HTTP response; consistent with ResponseError/
        ProviderError).
        """
        super().__init__(message)
        self._message = message  # Store for safe_detail
        self.status_code = status_code  # For handler getattr; logged/sanitized

    @property
    def safe_detail(self) -> str:
        """User-facing detail (override in subs; never exposes raw internals)."""
        return self._message  # Default; subs customize (e.g., hide paths)

    @property
    def error_type(self) -> str:
        """Sanitized error code from class (e.g., "data_validation_error").

        Avoids leaking raw __name__; overrides in subs for specifics.
        """
        # e.g., DataValidationError -> "data_validation_error"
        name = self.__class__.__name__.lower()
        return name.replace("error", "_error") if name.endswith("error") else name

    def to_response(self) -> dict[str, str]:
        """Return consistent sanitized dict for JSONResponse.

        Enables class-based pattern: no type names/stacks leaked. error_type
        derived from class for DRY consistency.
        """
        return {
            "error": self.error_type,  # Class-derived (e.g., "impact_error")
            "detail": self.safe_detail,
        }


class DataValidationError(ImpactError):
    """Raised when data fails validation checks.

    Overrides safe_detail to avoid leaking `value` (logged only).
    """

    def __init__(self, message: str, field: str = None, value=None):
        self.field = field
        self.value = value  # Logged only via helper; not in response
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Friendly msg; field included if present (no value)."""
        detail = self._message
        if self.field:
            detail = f"{detail} (field: {self.field})"
        return detail

    @property
    def error_type(self) -> str:
        """Specific type."""
        return "data_validation_error"


class ParseError(ImpactError):
    """Raised when parsing data from external sources fails.

    Overrides to hide source/line (logged only; prevents path leak).
    """

    def __init__(self, message: str, source: str = None, line_number: int = None):
        self.source = source
        self.line_number = line_number
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Generic parse msg; details logged only."""
        return f"Parse error: {self._message}"

    @property
    def error_type(self) -> str:
        """Specific type."""
        return "parse_error"


class ManifestError(ImpactError):
    """Base for manifest errors (hierarchy).

    Hides path in safe_detail (logged only; anti-enumeration).
    """

    def __init__(self, message: str, path: str = None):
        self.path = path
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Friendly manifest msg; path logged only."""
        return f"Manifest error: {self._message}"


# Subs inherit ManifestError.safe_detail (or override further)
class ManifestNotFoundError(ManifestError):
    """Raised specifically when manifest file is missing (404)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path)

    @property
    def safe_detail(self) -> str:
        """Specific 404 msg."""
        return "Manifest file not found at the provided dump path."

    @property
    def error_type(self) -> str:
        """Specific type for API/test consistency."""
        return "manifest_not_found"

    def __init__(self, message: str, path: str = None):
        # Override init to set status_code=404 (for thin handler)
        super().__init__(message, status_code=404)
        # path for logging only (set in super ManifestError if needed)


class ManifestInvalidError(ManifestError):
    """Raised when manifest is invalid (JSON/fields; 422)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path)

    @property
    def safe_detail(self) -> str:
        """Specific invalid msg."""
        return "Manifest file is invalid (check JSON format/fields)."

    @property
    def error_type(self) -> str:
        """Specific type."""
        return "manifest_invalid"


class ProviderError(ImpactError):
    """Raised when a provider (e.g., GitHub API) operation fails.

    Hides provider/status in safe_detail (logged only).
    """

    def __init__(self, message: str, provider: str = None, status_code: int = None):
        self.provider = provider
        self.status_code = status_code
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Generic provider msg."""
        return "External provider error (e.g., rate limit or downtime)."

    # error_type from base suffices ("provider_error"); no override needed
    # status_code handled in thin base handler (getattr; consistent with ResponseError 502/503)


class AdapterError(ImpactError):
    """Raised when adapter processing fails."""

    def __init__(self, message: str, adapter: str = None):
        self.adapter = adapter
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Generic adapter msg."""
        return "Data processing error."

    @property
    def error_type(self) -> str:
        """Specific type."""
        return "adapter_error"


class ResponseError(ImpactError):
    """Raised for response formatting, parsing, or API output errors.

    Keeps status_code; hides details.
    """

    def __init__(self, message: str, status_code: int = 500, details: dict | None = None):
        self.status_code = status_code
        self.details = details or {}  # Logged only
        super().__init__(message)

    @property
    def safe_detail(self) -> str:
        """Generic response msg."""
        return "Error formatting response."

    # error_type from base suffices ("response_error"); no override needed
