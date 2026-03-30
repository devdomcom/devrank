from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from celery import shared_task

from impact.providers.base import FetchConfig
from impact.providers.registry import get_fetcher


def _parse_iso(dt: str | None, default: datetime) -> datetime:
    if not dt:
        return default
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))


def _build_config(
    provider: str,
    user_login: str,
    repos: list[str],
    token: str,
    out_dir: str,
    start: datetime,
    end: datetime,
    user_timezone: str | None = None,
    notes: str | None = None,
    **extra: object,
) -> FetchConfig:
    """Build a ``FetchConfig`` (or provider-specific subclass) from kwargs.

    For the GitHub provider, we import ``LiveFetchConfig`` to preserve
    GitHub-specific options like ``fetch_contents``.  All other providers
    receive the base ``FetchConfig``.
    """
    base_kwargs = dict(
        user_login=user_login,
        repos=repos,
        start=start,
        end=end,
        token=token,
        out_dir=Path(out_dir),
        user_timezone=user_timezone,
        notes=notes,
    )

    if provider == "github":
        from impact.providers.github_live import LiveFetchConfig

        return LiveFetchConfig(
            **base_kwargs,
            fetch_contents=bool(extra.get("fetch_contents", False)),
        )

    return FetchConfig(**base_kwargs)


@shared_task(bind=True)
def run_fetch(
    self,
    user_login: str,
    repos: list[str],
    token: str,
    out_dir: str,
    start_iso: str | None = None,
    end_iso: str | None = None,
    user_timezone: str | None = None,
    notes: str | None = None,
    provider: str = "github",
    **extra: object,
):
    """Fetch data from a provider and return summary stats.

    Dispatches to the appropriate ``ProviderFetcher`` via the fetcher
    registry, so adding a new provider requires zero changes here.
    """
    now = datetime.now(UTC)
    start = _parse_iso(start_iso, now - timedelta(days=365))
    end = _parse_iso(end_iso, now)

    cfg = _build_config(
        provider=provider,
        user_login=user_login,
        repos=repos,
        token=token,
        out_dir=out_dir,
        start=start,
        end=end,
        user_timezone=user_timezone,
        notes=notes,
        **extra,
    )

    fetcher = get_fetcher(provider, cfg)
    bundle = fetcher.run()
    return {
        "provider": provider,
        "users": len(bundle.users),
        "pull_requests": len(bundle.pull_requests),
        "out_dir": out_dir,
    }
