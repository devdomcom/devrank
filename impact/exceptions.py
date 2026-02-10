"""
Custom exceptions for the impact metrics system.

These exceptions provide more specific error information than generic
exceptions, enabling better debugging and error handling throughout
the codebase.
"""


class ImpactError(Exception):
    """Base exception for all impact-related errors."""

    pass


class DataValidationError(ImpactError):
    """Raised when data fails validation checks."""

    def __init__(self, message: str, field: str = None, value=None):
        self.field = field
        self.value = value
        super().__init__(message)


class ParseError(ImpactError):
    """Raised when parsing data from external sources fails."""

    def __init__(self, message: str, source: str = None, line_number: int = None):
        self.source = source
        self.line_number = line_number
        super().__init__(message)


class ManifestError(ImpactError):
    """Base for manifest errors (hierarchy)."""

    def __init__(self, message: str, path: str = None):
        self.path = path
        super().__init__(message)


class ManifestNotFoundError(ManifestError):
    """Raised specifically when manifest file is missing (404)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path)


class ManifestInvalidError(ManifestError):
    """Raised when manifest is invalid (JSON/fields; 422)."""

    def __init__(self, message: str, path: str = None):
        super().__init__(message, path)


class ProviderError(ImpactError):
    """Raised when a provider (e.g., GitHub API) operation fails."""

    def __init__(self, message: str, provider: str = None, status_code: int = None):
        self.provider = provider
        self.status_code = status_code
        super().__init__(message)


class AdapterError(ImpactError):
    """Raised when adapter processing fails."""

    def __init__(self, message: str, adapter: str = None):
        self.adapter = adapter
        super().__init__(message)


class ResponseError(ImpactError):
    """Raised for response formatting, parsing, or API output errors."""

    def __init__(self, message: str, status_code: int = 500, details: dict | None = None):
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)
