"""
Time to Restore (DORA MTTR Proxy)

Approximates Mean Time to Restore using revert-to-fix cycle detection.
The sample data contains 14 revert commits; this metric measures the time
between a revert and the subsequent fix commit on the same file set.

When GitHub Releases/Deployments API data is available (see data pipeline),
this metric can be enhanced with deployment-aware incident detection.
"""

import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class TimeToRestore(Metric):
    """
    Time to Restore (DORA MTTR proxy): time from revert to next fix.

    Algorithm:
      1. Identify revert commits via "Revert" prefix in message.
      2. Find the files touched by the revert commit's PR.
      3. Find the next non-revert commit on the same file set (the "fix").
      4. MTTR = fix_commit.date - revert_commit.date.
    """

    @property
    def slug(self) -> str:
        return "time_to_restore"

    @property
    def name(self) -> str:
        return "Time to Restore"

    @property
    def description(self) -> str:
        return "Median hours from revert to fix commit (DORA MTTR proxy via revert-to-fix cycle)."

    @property
    def category(self) -> str:
        return "reliability"

    @property
    def frameworks(self) -> list[str]:
        return ["DORA"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        # Get all commits in the period (from any user -- reverts are repo-wide)
        all_commits = context.ledger.bundle.commits
        if context.start_date:
            all_commits = [c for c in all_commits if c.date >= context.start_date]
        if context.end_date:
            all_commits = [c for c in all_commits if c.date <= context.end_date]

        all_commits.sort(key=lambda c: c.date)

        # Step 1: Identify revert commits
        revert_pattern = re.compile(r'^Revert\s', re.IGNORECASE)
        revert_commits = [c for c in all_commits if revert_pattern.match(c.message or "")]

        restore_times: list[float] = []
        incidents: list[dict] = []

        for revert in revert_commits:
            # Step 2: Get files touched by the revert's PR
            if not revert.pull_request_number:
                continue

            revert_files = {
                f.filename
                for f in context.ledger.get_files_for_pr(revert.pull_request_number)
            }
            if not revert_files:
                continue

            # Step 3: Find next non-revert commit touching the same files
            fix_commit = None
            for candidate in all_commits:
                if candidate.date <= revert.date:
                    continue
                if revert_pattern.match(candidate.message or ""):
                    continue  # skip other reverts
                if not candidate.pull_request_number:
                    continue

                candidate_files = {
                    f.filename
                    for f in context.ledger.get_files_for_pr(candidate.pull_request_number)
                }
                if revert_files & candidate_files:
                    fix_commit = candidate
                    break

            if fix_commit:
                restore_hours = (fix_commit.date - revert.date).total_seconds() / 3600
                restore_times.append(restore_hours)
                incidents.append({
                    "revert_sha": revert.sha[:8],
                    "revert_date": revert.date.isoformat(),
                    "fix_sha": fix_commit.sha[:8],
                    "fix_date": fix_commit.date.isoformat(),
                    "restore_hours": round(restore_hours, 2),
                    "affected_files": len(revert_files),
                })

        median_hours = percentile(restore_times, 0.5) if restore_times else 0.0
        p75_hours = percentile(restore_times, 0.75) if restore_times else 0.0

        summary = (
            f"MTTR proxy: {len(incidents)} revert-to-fix cycles detected. "
            f"Median restore: {median_hours:.1f}h."
        )

        details: dict[str, object] = {
            "revert_count": len(revert_commits),
            "incidents_with_fix": len(incidents),
            "median_restore_hours": round(median_hours, 1),
            "p75_restore_hours": round(p75_hours, 1),
            "incidents": incidents,
            "period_days": round(period_days, 1),
        }

        if not revert_commits or not incidents:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
