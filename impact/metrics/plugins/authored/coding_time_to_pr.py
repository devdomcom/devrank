from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import calculate_coding_time_to_pr_hours, filter_prs_for_contribution, percentile


class CodingTimeToPR(Metric):
    """Time from first commit to PR open (pre-PR coding effort)."""

    @property
    def slug(self) -> str:
        return "coding_time_to_pr"

    @property
    def name(self) -> str:
        return "Coding Time To PR"

    @property
    def description(self) -> str:
        return "Median hours from first commit to PR creation (pre-PR coding time)."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # All non-draft PRs (coding applies pre-open; exclude incomplete drafts)
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=False)

        durations_hours: list[float] = []
        per_pr = []
        for pr in filtered_prs:
            hours = calculate_coding_time_to_pr_hours(context.ledger, pr)
            if hours is not None:
                durations_hours.append(hours)
                per_pr.append({"number": pr.number, "hours": hours})

        median = percentile(durations_hours, 0.5) if durations_hours else 0.0
        p75 = percentile(durations_hours, 0.75) if durations_hours else 0.0

        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )
        summary = f"{len(filtered_prs)} PRs. Median pre-PR coding: {median:.2f}h, p75: {p75:.2f}h."
        details: dict[str, object] = {
            "pr_count": len(filtered_prs),
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
