"""
Contributor Experience Metric

Measures what fraction of the codebase's line activity was contributed by the
target developer. This is a developer-centric relative measure — not absolute
lines added, but the share of all activity in the period that this person
accounted for.

CodeScene defines experience as depth of familiarity with the codebase. A
developer who has touched a large share of the code (measured by lines) has
high experience. This metric captures that as:

    experience_pct = user_lines / total_lines * 100

DRY: Reuses build_file_contributors(by="lines") from main_developer.py to
attribute line contributions to PR authors across all bundle files.
"""

from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import build_file_contributors
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class ContributorExperience(Metric):
    """
    Contributor Experience: relative share of codebase activity by the target developer.

    Computes experience_pct = user_lines / total_lines * 100 by aggregating
    line contributions across all files in the bundle. This measures what
    fraction of the work in the analysis period was done by this developer,
    providing a relative signal of experience/familiarity with the codebase.
    """

    @property
    def slug(self) -> str:
        return "contributor_experience"

    @property
    def name(self) -> str:
        return "Contributor Experience"

    @property
    def description(self) -> str:
        return (
            "Relative share of codebase activity contributed by the target developer "
            "(user_lines / total_lines × 100). Higher values indicate deeper "
            "familiarity with the codebase."
        )

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
            else 30.0  # period fallback per AGENTS.md
        )

        bundle = context.ledger.bundle

        # Step 1: Build per-file contributor attribution across ALL bundle files
        # (not just the user's files) so we can compute a relative percentage.
        all_files = {
            f.filename
            for f in getattr(bundle, "files", [])
            if f.filename and not is_generated_file(f.filename, getattr(f, "patch", None))
        }

        if not all_files:
            return MetricResult(
                metric_slug=self.slug,
                summary="No files found in analysis period.",
                details={
                    "experience_pct": 0.0,
                    "user_lines": 0.0,
                    "total_lines": 0.0,
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        file_contributors = build_file_contributors(
            all_files, bundle, context.ledger, by="lines"
        )

        # Step 2: Aggregate per-author total line contributions
        author_totals: dict[str, float] = defaultdict(float)
        for filename, authors in file_contributors.items():
            for author, score in authors.items():
                author_totals[author] += score

        total_lines = sum(author_totals.values())
        user_lines = author_totals.get(context.user_login, 0.0)

        # Step 3: Compute relative experience percentage
        experience_pct = (user_lines / total_lines * 100) if total_lines > 0 else 0.0

        # Build summary
        if total_lines == 0:
            summary = "No contribution data available."
        else:
            summary = (
                f"Contributor experience: {experience_pct:.1f}% "
                f"({user_lines:.0f} of {total_lines:.0f} lines in {round(period_days, 1)} days)."
            )

        details: dict = {
            "experience_pct": round(experience_pct, 1),
            "user_lines": round(user_lines, 1),
            "total_lines": round(total_lines, 1),
            "period_days": round(period_days, 1),
        }

        if total_lines == 0 or experience_pct == 0:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
