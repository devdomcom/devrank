from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_coding_days_ratio, filter_prs_for_contribution


class CodingDays(Metric):
    """Distinct commit days / working days ratio in period (engagement consistency)."""

    @property
    def slug(self) -> str:
        return "coding_days"

    @property
    def name(self) -> str:
        return "Coding Days"

    @property
    def description(self) -> str:
        return "% working days (Mon-Fri) with >=1 commit (daily engagement)."

    @property
    def category(self) -> str:
        return "productivity_throughput"

    def run(self, context: MetricContext) -> MetricResult:
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # Dates from commits only (per spec; filter meaningful PRs but use all commits)
        commit_dates = [c.date for c in commits]

        # Use helper for ratio/details (reuses week-style logic)
        day_details = compute_coding_days_ratio(
            commit_dates, context.start_date, context.end_date
        )

        ratio_pct = day_details["ratio"] * 100
        summary = f"{day_details['active_days']}/{day_details['total_working_days']} working days active ({ratio_pct:.1f}%)."
        details: dict[str, object] = {
            **day_details,
            "ratio_pct": ratio_pct,
            "commit_count": len(commits),
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if not commits or day_details["total_working_days"] == 0 or (period_days < 14 and len(commits) < 3):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
