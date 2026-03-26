from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_code_churn, filter_prs_for_contribution


class CodeChurnRate(Metric):
    @property
    def slug(self) -> str:
        return "code_churn_rate"

    @property
    def name(self) -> str:
        return "Code Churn Rate"

    @property
    def description(self) -> str:
        return "% lines in PRs modifying own recent code (<=30d; flags output instability)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["SPACE", "CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(
            user, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        details = compute_code_churn(
            context.ledger, user, prs,
            start_date=context.start_date, end_date=context.end_date,
        )
        # Period/freq aware: overall + max_weekly (spikes) + per_week
        rate = details["churn_rate"]
        summary = f"Code churn: {rate:.1f}% overall (max weekly {details.get('max_weekly_churn', 0):.1f}%; {details['churn_per_week']:.1f} lines/wk; {len(prs)} PRs)."
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
