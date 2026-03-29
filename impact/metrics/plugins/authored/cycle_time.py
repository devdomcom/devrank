from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import calculate_merge_time_hours, percentile


class CycleTime(Metric):
    """
    Measures the time from PR creation to merge for a user's pull requests.

    This metric calculates the median and 75th percentile of merge times (in hours)
    for all PRs authored by the user that were merged within the specified time window.
    Lower cycle times generally indicate faster iteration and delivery.

    Details returned:
        - merged_count: Number of merged PRs
        - median_hours: Median time to merge
        - p75_hours: 75th percentile time to merge
        - per_pr_hours: Breakdown per PR
    """

    @property
    def slug(self) -> str:
        return "cycle_time"

    @property
    def name(self) -> str:
        return "Cycle Time"

    @property
    def description(self) -> str:
        return "Measures median time from PR open to merge (speed of delivery)."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["DORA", "SPACE", "Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        merged_prs = context.ledger.get_merged_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        durations_hours: list[float] = []
        per_pr = []
        for pr in merged_prs:
            hours = calculate_merge_time_hours(pr)
            if hours is not None:
                durations_hours.append(hours)
                per_pr.append({"number": pr.number, "hours": hours})

        median = percentile(durations_hours, 0.5) if durations_hours else 0.0
        p75 = percentile(durations_hours, 0.75) if durations_hours else 0.0

        summary = f"{len(merged_prs)} merged PRs. Median: {median:.2f}h, p75: {p75:.2f}h."
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )
        details: dict[str, object] = {
            "merged_count": len(merged_prs),
            "median_hours": median,
            "p75_hours": p75,
            "per_pr_hours": per_pr,
            "period_days": round(period_days, 1),
        }
        # Combined period+count guard (AGENTS.md mandate)
        if not durations_hours or (period_days < 14 and len(durations_hours) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
