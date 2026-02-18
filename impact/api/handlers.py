"""Shim — exception handlers have been moved to api.handlers (app-level)."""
from api.handlers import register_exception_handlers

__all__ = ["register_exception_handlers"]
