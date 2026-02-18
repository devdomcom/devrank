"""App-level exception hierarchy with sanitized responses.

Base ``AppError`` provides ``safe_detail``, ``error_type``, and ``to_response()``
for consistent, leak-free JSON error responses. All domain-specific error trees
(auth, impact pipeline, etc.) inherit from this base.

Auth exceptions live here (app-level concern).
Impact pipeline exceptions live in ``impact/exceptions.py``.
"""
from __future__ import annotations


class AppError(Exception):
    """Base application error with class-driven sanitization.

    Provides ``safe_detail`` (user-facing), ``error_type`` (class-derived code),
    and ``to_response()`` (consistent dict) so handlers stay thin and DRY.
    """

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self._message = message
        self.status_code = status_code

    @property
    def safe_detail(self) -> str:
        """User-facing detail (override in subclasses to hide internals)."""
        return self._message

    @property
    def error_type(self) -> str:
        """Sanitized error code derived from class name."""
        name = self.__class__.__name__.lower()
        return name.replace("error", "_error") if name.endswith("error") else name

    def to_response(self) -> dict[str, str]:
        """Consistent sanitized dict for JSONResponse."""
        return {
            "error": self.error_type,
            "detail": self.safe_detail,
        }


# ── Auth exceptions (app-level) ─────────────────────────────────────────


class AuthenticationError(AppError):
    """Login / token validation failures (401)."""

    def __init__(self, message: str = "Invalid credentials"):
        super().__init__(message, status_code=401)

    @property
    def error_type(self) -> str:
        return "authentication_error"


class AuthorizationError(AppError):
    """Authenticated user lacks required permissions (403)."""

    def __init__(self, message: str = "Insufficient permissions"):
        super().__init__(message, status_code=403)

    @property
    def error_type(self) -> str:
        return "authorization_error"
