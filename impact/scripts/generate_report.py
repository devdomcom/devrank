#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from celery.exceptions import TimeoutError as CeleryTimeout

from impact.celery_app import app as celery_app
from impact.config.role_metrics import get_role_config
from impact.domain.models import MetricContext
from impact.ingestion.dump import DumpIngestion
from impact.ledger.ledger import Ledger
from impact.metrics import get_metrics
from impact.templates.pdf_report import generate_candidate_pdf
from impact.thresholds import METRIC_THRESHOLDS, get_continuous_score, rate_from_yaml, resolve_role_metric_cfg


def _apply_allowed_ratings(rating: str, metric_slug: str, role_config: dict) -> str:
    """Restrict rating to allowed set from role YAML (force neutral if not allowed)."""
    if not role_config or "metrics" not in role_config:
        return rating
    mcfg = role_config["metrics"].get(metric_slug)
    if not mcfg:
        return rating
    allowed = mcfg.get("allowed_ratings")
    if allowed and rating not in allowed:
        return "neutral"
    return rating


def get_metric_rating(metric_slug, details, role_config):
    # Special case: no activity (e.g. unblock_time with 0 CRs) rates neutral (not excellent)
    if details.get("no_cr_activity"):
        return "neutral"
    # Guard: metrics with no_data flag should not be rated
    if details.get("no_data"):
        return "INSUFFICIENT_DATA"

    # Check no_rating (descriptive only; e.g. net_code_contribution)
    mcfg = resolve_role_metric_cfg(metric_slug, role_config)
    if mcfg and mcfg.get("no_rating"):
        return "descriptive"
    thresh = METRIC_THRESHOLDS.get(metric_slug, {})
    if thresh.get("no_rating"):
        return "descriptive"

    # Resolve key + value
    key = (mcfg or {}).get("key") or thresh.get("key")
    if not key or key not in details:
        return "unknown"
    val = details[key]

    # Try YAML thresholds first (direction-aware)
    if mcfg:
        rating = rate_from_yaml(val, mcfg)
        return _apply_allowed_ratings(rating, metric_slug, role_config)

    # Fallback to code thresholds
    if metric_slug not in METRIC_THRESHOLDS:
        return "unknown"
    for level in ["excellent", "good", "neutral", "bad"]:
        if level in thresh and thresh[level](val):
            return _apply_allowed_ratings(level, metric_slug, role_config)
    return "unknown"


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description="Generate DevRank impact report.")
    parser.add_argument(
        "--dump-path",
        help="Target path for new fetch dumps (optional if --existing-dump is provided)",
    )
    parser.add_argument(
        "--existing-dump",
        help="Use an existing dump directory; skips live fetch even if fetch flags are provided",
    )
    parser.add_argument(
        "--metrics",
        nargs="*",
        help="Metric slugs to run (e.g., pr_merge_effectiveness review_leverage)",
    )
    parser.add_argument("--out", help="Output path for the report (not implemented yet)")
    # Optional: trigger live fetch via Celery before running report
    parser.add_argument("--fetch-user", help="User login to fetch (assessed user)")
    parser.add_argument("--fetch-repos", help="Comma-separated repos to fetch (owner/repo)")
    parser.add_argument("--fetch-token", help="GitHub token; defaults to GITHUB_TOKEN env")
    parser.add_argument(
        "--fetch-from", dest="fetch_from", help="ISO start date (default: 365 days ago)"
    )
    parser.add_argument("--fetch-to", dest="fetch_to", help="ISO end date (default: now)")
    parser.add_argument(
        "--broker", help="Celery broker URL override (default env CELERY_BROKER_URL)"
    )
    parser.add_argument(
        "--fetch-timeout",
        type=int,
        default=900,
        help="Timeout in seconds to wait for fetch task (default 900s)",
    )
    parser.add_argument(
        "--role",
        required=True,
        help="Role name for YAML config (e.g. senior_dev from roles/; copy/edit YAMLs).",
    )
    parser.add_argument(
        "--role-config",
        help="On-fly custom role YAML path (overrides --role; copy/paste/edit from roles/ dir for seamless use).",
    )
    parser.add_argument(
        "--export",
        help="Export mode (e.g. candidate.pdf to generate PDF report; format determines template).",
    )

    args = parser.parse_args()

    if not args.dump_path and not args.existing_dump:
        raise SystemExit(
            "Provide --existing-dump to reuse a dump, or --dump-path plus fetch flags to create one."
        )

    # Decide dump directory (existing vs new fetch target)
    dump_dir = Path(args.existing_dump or args.dump_path)

    # Optional: live fetch via Celery (required if fetch flags provided and not reusing)
    if args.fetch_repos and args.fetch_user and not args.existing_dump:
        token = args.fetch_token or os.environ.get("GITHUB_TOKEN")
        if not token:
            raise SystemExit(
                "fetch requested but no GitHub token provided (--fetch-token or GITHUB_TOKEN)"
            )
        repos = [r.strip() for r in args.fetch_repos.split(",") if r.strip()]
        now = datetime.now(UTC)
        start_iso = args.fetch_from or (now - timedelta(days=365)).isoformat()
        end_iso = args.fetch_to or now.isoformat()

        if args.broker:
            celery_app.conf.broker_url = args.broker
            celery_app.conf.result_backend = os.environ.get(
                "CELERY_BACKEND_URL", "redis://localhost:6379/1"
            )

        insp = celery_app.control.inspect(timeout=5)
        ping = insp.ping() if insp else None
        if not ping:
            raise SystemExit(
                "Celery worker not reachable. Start it with: docker compose up -d worker"
            )

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
            raise SystemExit(
                f"Fetch task {task.id} did not finish within {args.fetch_timeout}s. Check worker logs."
            )
        print(f"Fetch completed: {result}")
    elif args.existing_dump and (args.fetch_repos or args.fetch_user):
        print("Existing dump specified; ignoring fetch flags.")

    ingestion = DumpIngestion(str(dump_dir))
    bundle = ingestion.ingest()

    # Read manifest for user, dates, and repositories
    manifest_path = os.path.join(dump_dir, "dump_manifest.json")
    manifest: dict = {}
    user_login = None
    start_date = None
    end_date = None
    manifest_role = None
    repositories: list[str] = []
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        user_login = manifest.get("user")
        start_date = (
            datetime.fromisoformat(manifest["from"].replace("Z", "+00:00"))
            if "from" in manifest
            else None
        )
        end_date = (
            datetime.fromisoformat(manifest["to"].replace("Z", "+00:00"))
            if "to" in manifest
            else None
        )
        manifest_role = manifest.get("role")
        repositories = manifest.get("repositories", [])

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

    # Role config for ratings (YAML from roles/ dir; on-fly via --role-config or --role;
    # copy/paste/edit YAMLs; seamless with report/thresholds)
    role = args.role
    role_config = get_role_config(role, getattr(args, "role_config", None))

    metrics_results = []  # Collect for export (always init)
    if args.metrics:
        # Create ledger
        ledger = Ledger(bundle)

        # Create context
        context = MetricContext(
            ledger=ledger, user_login=user_login, start_date=start_date, end_date=end_date
        )

        # Get available metrics
        available_metrics = get_metrics()

        for metric_slug in args.metrics:
            if metric_slug not in available_metrics:
                print(
                    f"Metric '{metric_slug}' not found. Available: {list(available_metrics.keys())}"
                )
                continue
            metric_class = available_metrics[metric_slug]
            metric = metric_class()
            result = metric.run(context)

            # Compute rating + continuous score (YAML thresholds first, code fallback)
            rating = get_metric_rating(metric.slug, result.details, role_config)
            continuous_score = get_continuous_score(metric.slug, result.details, role_config)

            # Print result with modern formatting
            print("=" * 80)
            # Short desc after name for clarity (unique purpose per metric analysis)
            print(f"📊 {metric.name} ({metric.slug}): {metric.description}")
            print("=" * 80)
            score_str = f" (score: {continuous_score:.1f}/100)" if continuous_score is not None else ""
            print(f"🏆 Rating: {rating.upper()}{score_str}")
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

            # Collect for export
            metrics_results.append({
                "name": metric.name,
                "slug": metric.slug,
                "description": metric.description,
                "category": metric.category,
                "rating": rating,
                "score": continuous_score,
                "summary": result.summary,
                "details": result.details,
            })

    # Export mode (e.g. --export candidate.pdf for PDF template; modern sections/rating)
    if args.export and args.export.endswith(".pdf") and metrics_results:
        period_str = f"{start_date.date()} to {end_date.date()}" if start_date and end_date else "N/A"
        generate_candidate_pdf(
            metrics_results, user_login or "Candidate", period_str, args.export, repositories,
        )

    if args.out:
        print(f"Output to {args.out} is not implemented yet.")


if __name__ == "__main__":
    main()
