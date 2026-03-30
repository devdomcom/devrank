"""Abstract base classes for provider-specific data fetching.

``ProviderFetcher`` is the fetch-layer counterpart to
``impact.adapters.base.ProviderAdapter``.  Adapters *parse* already-downloaded
data; fetchers *download* data from a remote API and persist it as a canonical
dump.

Concrete implementations live alongside their API clients
(e.g. ``impact.providers.github_live.GitHubLiveFetcher``).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from impact.domain.models import CanonicalBundle


@dataclass
class FetchConfig:
    """Provider-neutral fetch configuration.

    Every concrete fetcher accepts at least these fields.  Provider-specific
    subclasses may extend with additional options (e.g. ``fetch_contents``
    for the GitHub blob API).
    """

    user_login: str
    repos: list[str]
    start: datetime
    end: datetime
    token: str
    out_dir: Path
    user_timezone: str | None = None
    notes: str | None = None


class ProviderFetcher(ABC):
    """Abstract base for provider data fetchers.

    Mirrors the adapter pattern: one concrete class per supported provider.
    The fetcher downloads raw data, writes a canonical dump to disk, and
    returns a ``CanonicalBundle`` ready for the metrics pipeline.
    """

    @abstractmethod
    def run(self) -> CanonicalBundle:
        """Fetch data from the provider and return a canonical bundle."""

    @abstractmethod
    def check_health(self) -> bool:
        """Verify connectivity and authentication with the provider API.

        Returns ``True`` if the API is reachable and the token is valid.
        """
