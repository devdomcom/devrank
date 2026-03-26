from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_absolute_churn_trend, filter_prs_for_contribution


class AbsoluteChurnTrend(Metric):
    """Absolute churn trend by date (adds+deletes) to detect integration bottlenecks."""

    @property
    def slug(self) -> str:
        return "absolute_churn_trend"

    @property
    def name(self) -> str:
        return "Absolute Churn Trend"

    @property
    def description(self) -> str:
        return "Lines added/deleted per date to spot churn spikes and integration bottlenecks."

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=False)

        details = compute_absolute_churn_trend(
            context.ledger,
            filtered_prs,
            start_date=context.start_date,
            end_date=context.end_date,
        )

        max_daily = details.get("max_daily_churn", 0)
        total_churn = details.get("total_churn", 0)
        pr_count = details.get("pr_count", 0)

        summary = (
            f"Absolute churn: {total_churn} lines across {pr_count} PRs "
            f"(max daily churn: {max_daily})."
        )

        if details.get("no_data"):
            summary = "Insufficient data to compute absolute churn trend."
        elif details.get("per_day"):
            top_day = max(details["per_day"], key=lambda d: d["churn"], default=None)
            if top_day:
                summary += f" Peak day: {top_day['date']} ({top_day['churn']} lines)."

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
