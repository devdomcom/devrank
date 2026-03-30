"""Adapter registry — provider dispatch for dump parsing.

Dict-based registry with ``register_adapter()`` so new providers can be
added without modifying this module.  Built-in adapters are registered
eagerly at import time.
"""
from __future__ import annotations

from impact.adapters.base import ProviderAdapter

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
_ADAPTER_REGISTRY: dict[str, type[ProviderAdapter]] = {}


def register_adapter(provider: str, adapter_class: type[ProviderAdapter]) -> None:
    """Register a concrete ``ProviderAdapter`` class for *provider*."""
    _ADAPTER_REGISTRY[provider] = adapter_class


def get_adapter(provider: str) -> ProviderAdapter:
    """Instantiate and return an adapter for *provider*.

    Raises ``ValueError`` with a helpful message listing available
    providers when *provider* is not registered.
    """
    cls = _ADAPTER_REGISTRY.get(provider)
    if cls is None:
        available = sorted(_ADAPTER_REGISTRY.keys()) or ["(none)"]
        raise ValueError(
            f"Unsupported provider: {provider!r}. "
            f"Available: {', '.join(available)}"
        )
    return cls()


def available_adapters() -> list[str]:
    """Return sorted list of registered provider names."""
    return sorted(_ADAPTER_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Built-in adapter registrations
# ---------------------------------------------------------------------------
def _register_builtins() -> None:
    from impact.adapters.github import GitHubAdapter

    register_adapter("github", GitHubAdapter)


_register_builtins()
