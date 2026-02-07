from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from celery import shared_task

from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig


def _parse_iso(dt: str | None, default: datetime) -> datetime:
    if not dt:
        return default
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))


@shared_task(bind=True)
def run_fetch(
    self,
    user_login: str,
    repos: list[str],
    token: str,
    out_dir: str,
    start_iso: str | None = None,
    end_iso: str | None = None,
):
    now = datetime.now(UTC)
    start = _parse_iso(start_iso, now - timedelta(days=365))
    end = _parse_iso(end_iso, now)
    cfg = LiveFetchConfig(
        user_login=user_login,
        repos=repos,
        start=start,
        end=end,
        token=token,
        out_dir=Path(out_dir),
    )
    fetcher = GitHubLiveFetcher(cfg)
    bundle = fetcher.run()
    return {
        "users": len(bundle.users),
        "pull_requests": len(bundle.pull_requests),
        "out_dir": out_dir,
    }
