#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Ensure repo root on sys.path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402

# Settings instance provides github_token (DEVRANK_GITHUB_TOKEN preferred)
from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch live GitHub data and emit canonical dump.")
    parser.add_argument(
        "--token",
        required=False,
        help="GitHub token (or set DEVRANK_GITHUB_TOKEN; falls back for legacy GITHUB_TOKEN)",
    )
    parser.add_argument("--user", required=True, help="Assessed user login")
    parser.add_argument(
        "--repos",
        required=True,
        help="Comma-separated list of repos (owner/repo). Use @all for all provided.",
    )
    parser.add_argument("--from", dest="since", help="ISO start date (default: 365 days ago)")
    parser.add_argument("--to", dest="until", help="ISO end date (default: now)")
    parser.add_argument("--out", required=True, help="Output folder for dump")
    parser.add_argument(
        "--tz", required=True,
        help="User timezone (IANA, e.g. America/Bogota); required for off-hours metric",
    )
    parser.add_argument("--notes", help="Optional notes to include in the dump manifest")
    parser.add_argument(
        "--fetch-contents", action="store_true",
        help="Fetch full file contents via Git Blobs API (for tree-sitter AST analysis)",
    )
    return parser.parse_args()


def parse_date(val: str, default: datetime) -> datetime:
    if not val:
        return default
    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def main():
    args = parse_args()
    # config.settings.github_token pulls from DEVRANK_GITHUB_TOKEN or legacy GITHUB_TOKEN
    # (see AliasChoices in config.py). This centralizes token logic (DRY).
    token = args.token or config.settings.github_token
    if not token:
        raise SystemExit(
            "GitHub token required via --token or GITHUB_TOKEN/DEVRANK_GITHUB_TOKEN env"
        )

    now = datetime.now(UTC)
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
        user_timezone=args.tz,
        notes=args.notes,
        fetch_contents=args.fetch_contents,
    )
    fetcher = GitHubLiveFetcher(cfg)
    bundle = fetcher.run()
    print(f"Wrote dump to {out_dir}. Users: {len(bundle.users)}, PRs: {len(bundle.pull_requests)}")


if __name__ == "__main__":
    main()
