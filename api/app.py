"""Shim — API has been consolidated into impact.api. Re-exports for backwards compat."""
from impact.api.app import API_V1_PREFIX, app, create_app

__all__ = ["app", "create_app", "API_V1_PREFIX"]
