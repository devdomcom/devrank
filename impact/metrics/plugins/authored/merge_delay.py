from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import calculate_merge_delay_hours, percentile


class MergeDelay(Metric):
    """Median time from last approval to merge (post-approval process bottleneck)."""

    @property
    def slug(self) -> str:
        return "merge_delay"

    @property
    def name(self) -> str:
        return "Merge Delay"

    @property
    def description(self) -> str:
        return "Median hours from latest approval to merge (isolates post-approval gaps)."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        merged_prs = context.ledger.get_merged_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        delays_hours: list[float] = []
        per_pr = []
        for pr in merged_prs:
            hours = calculate_merge_delay_hours(context.ledger, pr)
            if hours is not None:
                delays_hours.append(hours)
                per_pr.append({"number": pr.number, "hours": hours})

        median = percentile(delays_hours, 0.5) if delays_hours else 0.0
        p75 = percentile(delays_hours, 0.75) if delays_hours else 0.0

        summary = f"{len(merged_prs)} merged PRs. Median post-approval delay: {median:.2f}h, p75: {p75:.2f}h."
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
        if not delays_hours or (period_days < 14 and len(delays_hours) < 3):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
