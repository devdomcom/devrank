#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from impact.ingestion.github_live import GitHubLiveIngestion, LiveFetchConfig


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch live GitHub data and emit canonical dump.")
    parser.add_argument("--token", required=False, help="GitHub token (or set GITHUB_TOKEN)")
    parser.add_argument("--user", required=True, help="Assessed user login")
    parser.add_argument("--repos", required=True, help="Comma-separated list of repos (owner/repo). Use @all for all provided.")
    parser.add_argument("--from", dest="since", help="ISO start date (default: 365 days ago)")
    parser.add_argument("--to", dest="until", help="ISO end date (default: now)")
    parser.add_argument("--out", required=True, help="Output folder for dump")
    return parser.parse_args()


def parse_date(val: str, default: datetime) -> datetime:
    if not val:
        return default
    return datetime.fromisoformat(val.replace("Z", "+00:00"))


def main():
    args = parse_args()
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GitHub token required via --token or GITHUB_TOKEN")

    now = datetime.now(timezone.utc)
    start_default = now - timedelta(days=365)
    start = parse_date(args.since, start_default)
    end = parse_date(args.until, now)

    repos = [r.strip() for r in args.repos.split(",") if r.strip()]

    out_dir = Path(args.out)
    cfg = LiveFetchConfig(
        user_login=args.user,
        repos=repos,
        start=start,
        end=end,
        token=token,
        out_dir=out_dir,
    )
    ingestion = GitHubLiveIngestion(cfg)
    bundle = ingestion.ingest()
    print(f"Wrote dump to {out_dir}. Users: {len(bundle.users)}, PRs: {len(bundle.pull_requests)}")


if __name__ == "__main__":
    main()
