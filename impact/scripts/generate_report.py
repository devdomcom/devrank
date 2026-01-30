#!/usr/bin/env python3
import argparse
import json
import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from impact.ingestion.dump import DumpIngestion
from impact.ledger.ledger import Ledger
from impact.metrics import get_metrics
from impact.domain.models import MetricContext
from impact.celery_app import app as celery_app
from celery.exceptions import TimeoutError as CeleryTimeout
from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig


# Thresholds for rating metrics (using best judgment for defaults)
METRIC_THRESHOLDS = {
    'pr_throughput': {
        'key': 'merge_ratio',
        'excellent': lambda x: x >= 0.9,
        'good': lambda x: 0.7 <= x < 0.9,
        'neutral': lambda x: 0.5 <= x < 0.7,
        'bad': lambda x: x < 0.5,
    },
    'cycle_time': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 3,
        'neutral': lambda x: 3 < x <= 7,
        'bad': lambda x: x > 7,
    },
    'pr_merge_effectiveness': {
        'key': 'average_back_and_forth',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 2,
        'neutral': lambda x: 2 < x <= 4,
        'bad': lambda x: x > 4,
    },
    'review_leverage': {
        'key': 'effectiveness_percentage',
        'excellent': lambda x: x >= 80,
        'good': lambda x: 60 <= x < 80,
        'neutral': lambda x: 30 <= x < 60,
        'bad': lambda x: x < 30,
    },
    'review_iterations': {
        'key': 'average_iterations',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 2,
        'neutral': lambda x: 2 < x <= 4,
        'bad': lambda x: x > 4,
    },
    'time_to_first_review': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 6,
        'neutral': lambda x: 6 < x <= 24,
        'bad': lambda x: x > 24,
    },
    'slow_review_response': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 2,
        'good': lambda x: 2 < x <= 12,
        'neutral': lambda x: 12 < x <= 48,
        'bad': lambda x: x > 48,
    },
}


def get_metric_rating(metric_slug, details):
    if metric_slug not in METRIC_THRESHOLDS:
        return 'unknown'
    thresh = METRIC_THRESHOLDS[metric_slug]
    key = thresh['key']
    if key not in details:
        return 'unknown'
    val = details[key]
    for level in ['excellent', 'good', 'neutral', 'bad']:
        if thresh[level](val):
            return level
    return 'unknown'


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description='Generate DevRank impact report.')
    parser.add_argument('--dump-path', help='Target path for new fetch dumps (optional if --existing-dump is provided)')
    parser.add_argument('--existing-dump', help='Use an existing dump directory; skips live fetch even if fetch flags are provided')
    parser.add_argument('--metrics', nargs='*', help='Metric slugs to run (e.g., pr_merge_effectiveness review_leverage)')
    parser.add_argument('--out', help='Output path for the report (not implemented yet)')
    # Optional: trigger live fetch via Celery before running report
    parser.add_argument('--fetch-user', help='User login to fetch (assessed user)')
    parser.add_argument('--fetch-repos', help='Comma-separated repos to fetch (owner/repo)')
    parser.add_argument('--fetch-token', help='GitHub token; defaults to GITHUB_TOKEN env')
    parser.add_argument('--fetch-from', dest='fetch_from', help='ISO start date (default: 365 days ago)')
    parser.add_argument('--fetch-to', dest='fetch_to', help='ISO end date (default: now)')
    parser.add_argument('--broker', help='Celery broker URL override (default env CELERY_BROKER_URL)')
    parser.add_argument('--fetch-timeout', type=int, default=900, help='Timeout in seconds to wait for fetch task (default 900s)')

    args = parser.parse_args()

    if not args.dump_path and not args.existing_dump:
        raise SystemExit("Provide --existing-dump to reuse a dump, or --dump-path plus fetch flags to create one.")

    # Decide dump directory (existing vs new fetch target)
    dump_dir = Path(args.existing_dump or args.dump_path)

    # Optional: live fetch via Celery (required if fetch flags provided and not reusing)
    if args.fetch_repos and args.fetch_user and not args.existing_dump:
        token = args.fetch_token or os.environ.get("GITHUB_TOKEN")
        if not token:
            raise SystemExit("fetch requested but no GitHub token provided (--fetch-token or GITHUB_TOKEN)")
        repos = [r.strip() for r in args.fetch_repos.split(",") if r.strip()]
        now = datetime.now(timezone.utc)
        start_iso = args.fetch_from or (now - timedelta(days=365)).isoformat()
        end_iso = args.fetch_to or now.isoformat()

        if args.broker:
            celery_app.conf.broker_url = args.broker
            celery_app.conf.result_backend = os.environ.get("CELERY_BACKEND_URL", "redis://localhost:6379/1")

        insp = celery_app.control.inspect(timeout=5)
        ping = insp.ping() if insp else None
        if not ping:
            raise SystemExit("Celery worker not reachable. Start it with: docker compose up -d worker")

        task = celery_app.send_task(
            "impact.tasks.fetch.run_fetch",
            kwargs={
                "user_login": args.fetch_user,
                "repos": repos,
                "token": token,
                "out_dir": str(dump_dir),
                "start_iso": start_iso,
                "end_iso": end_iso,
            },
        )
        print(f"Queued fetch task {task.id}, waiting for completion...")
        try:
            result = task.get(timeout=args.fetch_timeout)
        except CeleryTimeout:
            raise SystemExit(f"Fetch task {task.id} did not finish within {args.fetch_timeout}s. Check worker logs.")
        print(f"Fetch completed: {result}")
    elif args.existing_dump and (args.fetch_repos or args.fetch_user):
        print("Existing dump specified; ignoring fetch flags.")

    ingestion = DumpIngestion(str(dump_dir))
    bundle = ingestion.ingest()

    # Read manifest for user and dates if metrics are requested
    user_login = None
    start_date = None
    end_date = None
    if args.metrics:
        manifest_path = os.path.join(dump_dir, 'dump_manifest.json')
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        user_login = manifest['user']
        start_date = datetime.fromisoformat(manifest['from'].replace('Z', '+00:00')) if 'from' in manifest else None
        end_date = datetime.fromisoformat(manifest['to'].replace('Z', '+00:00')) if 'to' in manifest else None

    # Header
    print("🚀 DevRank Impact Report")
    print("=" * 80)
    if user_login:
        print(f"👤 User: {user_login}")
    if start_date and end_date:
        print(f"📅 Period: {start_date} to {end_date}")
    print()
    print("📊 Data Summary:")
    print(f"  👥 Users: {len(bundle.users)}")
    print(f"  📁 Repositories: {len(bundle.repositories)}")
    print(f"  🔄 Pull Requests: {len(bundle.pull_requests)}")
    print(f"  💾 Commits: {len(bundle.commits)}")
    print(f"  👀 Reviews: {len(bundle.reviews)}")
    print(f"  💬 Comments: {len(bundle.comments)}")
    print()

    if args.metrics:

        # Create ledger
        ledger = Ledger(bundle)

        # Create context
        context = MetricContext(
            ledger=ledger,
            user_login=user_login,
            start_date=start_date,
            end_date=end_date
        )

        # Get available metrics
        available_metrics = get_metrics()

        for metric_slug in args.metrics:
            if metric_slug not in available_metrics:
                print(f"Metric '{metric_slug}' not found. Available: {list(available_metrics.keys())}")
                continue
            metric_class = available_metrics[metric_slug]
            metric = metric_class()
            result = metric.run(context)

            # Compute rating
            rating = get_metric_rating(metric.slug, result.details)

            # Print result with modern formatting
            print("=" * 80)
            print(f"📊 {metric.name} ({metric.slug})")
            print("=" * 80)
            print(f"🏆 Rating: {rating.upper()}")
            print(f"💡 Summary: {result.summary}")
            print()
            print("📈 Details:")
            for key, value in result.details.items():
                if isinstance(value, list):
                    print(f"  • {key}:")
                    for item in value:
                        print(f"    - {item}")
                else:
                    print(f"  • {key}: {value}")
            print()

    if args.out:
        print(f"Output to {args.out} is not implemented yet.")


if __name__ == '__main__':
    main()
