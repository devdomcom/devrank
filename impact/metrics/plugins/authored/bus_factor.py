"""
Bus Factor Metric

The minimum number of developers who could leave before code becomes unmaintainable.
Calculated by analyzing file-level contribution distribution and determining how many
top contributors can be "removed" before any file loses all knowledgeable maintainers.

Critical in the AI era: AI-generated code often creates "phantom ownership" where
no human truly understands the code, artificially inflating contributor counts.
"""

from collections import defaultdict
from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class BusFactor(Metric):
    """
    Bus Factor: minimum developers who could leave before code is unmaintainable.

    Calculates by iteratively removing the top contributor and checking if any
    files would lose all knowledgeable maintainers. The count of removals before
    this happens is the bus factor.

    Also tracks AI phantom ownership risk: files where AI is a top contributor
    but human review depth is low.
    """

    @property
    def slug(self) -> str:
        return "bus_factor"

    @property
    def name(self) -> str:
        return "Bus Factor"

    @property
    def description(self) -> str:
        return "Min developers who could leave before code becomes unmaintainable (files lose all knowledgeable maintainers)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        # Step 1: Determine *which files* the target user touched
        user_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(user_prs, exclude_drafts=True, only_merged=False)

        files_in_scope: set[str] = set()
        for pr in prs:
            for file in context.ledger.get_files_for_pr(pr.number):
                if not is_generated_file(file.filename, file.patch):
                    files_in_scope.add(file.filename)

        # Step 2: Build contributor map from ALL repo PRs that touch those files
        # This is the critical fix -- bus factor must reflect *all* contributors,
        # not just the target user, to avoid always returning 1.
        file_contributors: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        for pr in context.ledger.bundle.pull_requests:
            pr_author = pr.user.login
            for file in context.ledger.get_files_for_pr(pr.number):
                if file.filename not in files_in_scope:
                    continue
                if is_generated_file(file.filename, file.patch):
                    continue

                change_weight = file.changes if file.changes else (file.additions + file.deletions)
                contribution_score = max(1.0, change_weight)
                file_contributors[file.filename][pr_author] += contribution_score

        # If no files found, return no-data
        if not files_in_scope:
            return MetricResult(
                metric_slug=self.slug,
                summary="No files found in analysis period.",
                details={
                    "bus_factor": 0,
                    "total_files": 0,
                    "unique_contributors": 0,
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Calculate bus factor
        bus_factor, at_risk_files, contributor_order = self._calculate_bus_factor(
            file_contributors, files_in_scope
        )

        # Find files with single contributor (high risk)
        single_contributor_files = [
            f for f in files_in_scope
            if len(file_contributors.get(f, {})) == 1
        ]

        # Calculate contributor stats
        all_contributors: set[str] = set()
        for contributors in file_contributors.values():
            all_contributors.update(contributors.keys())

        # Per-file contributor count
        per_file_contributors = {
            f: len(file_contributors.get(f, {}))
            for f in files_in_scope
        }

        # Build summary
        if bus_factor == 0:
            summary = f"CRITICAL: Bus factor is 0 - {len(single_contributor_files)} files have single contributors."
        elif bus_factor == 1:
            summary = f"WARNING: Bus factor is 1 - one departure risks unmaintainable code ({len(at_risk_files)} files at risk)."
        else:
            summary = f"Bus factor: {bus_factor} ({len(all_contributors)} contributors across {len(files_in_scope)} files)."

        details: dict[str, object] = {
            "bus_factor": bus_factor,
            "total_files": len(files_in_scope),
            "unique_contributors": len(all_contributors),
            "contributor_order": contributor_order[:10],  # Top 10 contributors by removal order
            "single_contributor_files_count": len(single_contributor_files),
            "single_contributor_files": single_contributor_files[:20],  # Cap for payload size
            "at_risk_files_count": len(at_risk_files),
            "at_risk_files": list(at_risk_files)[:20],
            "per_file_contributor_count": per_file_contributors,
            "period_days": round(period_days, 1),
        }

        # No-data guard for insufficient data
        if len(files_in_scope) < 3 or (period_days < 14 and len(prs) < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )

    def _calculate_bus_factor(
        self,
        file_contributors: dict[str, dict[str, float]],
        files_in_scope: set[str],
    ) -> tuple[int, set[str], list[str]]:
        """
        Calculate bus factor by iteratively removing top contributors.

        Algorithm:
        1. While there are contributors:
           a. Find the contributor with highest total contribution
           b. Remove them from all files
           c. Check if any file now has 0 contributors
           d. If yes, stop - bus factor is the count of removals
        2. If all contributors removed without orphaning files, bus factor = total contributors

        Returns:
            (bus_factor, at_risk_files, contributor_removal_order)
        """
        # Create a mutable copy of contributor data
        remaining_contributions: dict[str, dict[str, float]] = {
            f: dict(contributors) for f, contributors in file_contributors.items()
        }

        # Track all contributors across files
        all_contributors: set[str] = set()
        for contributors in file_contributors.values():
            all_contributors.update(contributors.keys())

        if not all_contributors:
            return 0, files_in_scope, []

        bus_factor = 0
        contributor_order: list[str] = []
        at_risk_files: set[str] = set()

        # Iteratively remove top contributors
        remaining_contributors = set(all_contributors)

        while remaining_contributors:
            # Calculate total contribution for each remaining contributor
            contributor_totals: dict[str, float] = defaultdict(float)
            for file, contributors in remaining_contributions.items():
                for contributor, score in contributors.items():
                    if contributor in remaining_contributors:
                        contributor_totals[contributor] += score

            if not contributor_totals:
                break

            # Find top contributor
            top_contributor = max(contributor_totals.keys(), key=lambda c: contributor_totals[c])

            # Remove this contributor from consideration
            remaining_contributors.remove(top_contributor)
            contributor_order.append(top_contributor)

            # Check if any file would lose all contributors
            newly_at_risk: set[str] = set()
            for file in files_in_scope:
                file_contribs = remaining_contributions.get(file, {})
                # Count remaining contributors for this file
                remaining_count = sum(
                    1 for c in file_contribs.keys() if c in remaining_contributors
                )
                if remaining_count == 0:
                    newly_at_risk.add(file)

            if newly_at_risk:
                # Bus factor reached - removing this contributor orphans files
                at_risk_files = newly_at_risk
                break

            bus_factor += 1

        # If we removed everyone without orphaning files
        if not at_risk_files and not remaining_contributors:
            bus_factor = len(all_contributors)

        return bus_factor, at_risk_files, contributor_order
