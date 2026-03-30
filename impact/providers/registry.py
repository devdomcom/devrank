"""Fetcher registry — provider dispatch for the fetch pipeline.

Works identically to ``impact.adapters.registry``: a dict-based registry
with a ``register_fetcher`` helper so new providers can be added without
touching this module.

Default registrations are loaded eagerly for built-in providers (GitHub).
Third-party providers call ``register_fetcher()`` at import time.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from impact.providers.base import FetchConfig, ProviderFetcher

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
_FETCHER_REGISTRY: dict[str, type[ProviderFetcher]] = {}


def register_fetcher(provider: str, fetcher_class: type[ProviderFetcher]) -> None:
    """Register a concrete ``ProviderFetcher`` class for *provider*."""
    _FETCHER_REGISTRY[provider] = fetcher_class


def get_fetcher(provider: str, config: FetchConfig) -> ProviderFetcher:
    """Instantiate and return a fetcher for *provider*.

    Raises ``ValueError`` with a helpful message listing available
    providers when *provider* is not registered.
    """
    cls = _FETCHER_REGISTRY.get(provider)
    if cls is None:
        available = sorted(_FETCHER_REGISTRY.keys()) or ["(none)"]
        raise ValueError(
            f"Unsupported provider: {provider!r}. "
            f"Available: {', '.join(available)}"
        )
    return cls(config)


def available_providers() -> list[str]:
    """Return sorted list of registered provider names."""
    return sorted(_FETCHER_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Built-in provider registrations (lazy import to avoid circular deps)
# ---------------------------------------------------------------------------
def _register_builtins() -> None:
    from impact.providers.github_live import GitHubLiveFetcher

    register_fetcher("github", GitHubLiveFetcher)


_register_builtins()
