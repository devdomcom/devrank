#!/usr/bin/env python3
import argparse
import json
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from impact.ingestion.dump import DumpIngestion
from impact.ledger.ledger import Ledger
from impact.metrics import get_metrics
from impact.domain.models import MetricContext


def main():
    parser = argparse.ArgumentParser(description='Generate DevRank impact report.')
    parser.add_argument('--dump-path', required=True, help='Path to the dump data directory')
    parser.add_argument('--metrics', nargs='*', help='Metric slugs to run (e.g., pr_merge_effectiveness review_leverage)')
    parser.add_argument('--out', help='Output path for the report (not implemented yet)')

    args = parser.parse_args()

    ingestion = DumpIngestion(args.dump_path)
    bundle = ingestion.ingest()

    # For now, just print a summary
    print(f"Successfully ingested data:")
    print(f"  Users: {len(bundle.users)}")
    print(f"  Repositories: {len(bundle.repositories)}")
    print(f"  Pull Requests: {len(bundle.pull_requests)}")
    print(f"  Commits: {len(bundle.commits)}")
    print(f"  Reviews: {len(bundle.reviews)}")
    print(f"  Comments: {len(bundle.comments)}")
    print()

    if args.metrics:
        # Read manifest for user and dates
        manifest_path = os.path.join(args.dump_path, 'dump_manifest.json')
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        user_login = manifest['user']
        start_date = datetime.fromisoformat(manifest['from'].replace('Z', '+00:00')) if 'from' in manifest else None
        end_date = datetime.fromisoformat(manifest['to'].replace('Z', '+00:00')) if 'to' in manifest else None

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

            # Print result
            print(f"Metric: {metric.name} ({metric.slug})")
            print(f"Summary: {result.summary}")
            print(f"Details: {result.details}")
            print()

    if args.out:
        print(f"Output to {args.out} is not implemented yet.")


if __name__ == '__main__':
    main()