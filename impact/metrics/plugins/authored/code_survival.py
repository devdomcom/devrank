from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_code_survival, filter_prs_for_contribution


class CodeSurvival(Metric):
    """Code Survival / Cohort Analysis: % contributed code still present over time."""

    @property
    def slug(self) -> str:
        return "code_survival"

    @property
    def name(self) -> str:
        return "Code Survival"

    @property
    def description(self) -> str:
        return "% of contributed lines that remain untouched by subsequent changes (durability signal)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=True)

        details = compute_code_survival(
            context.ledger,
            context.user_login,
            filtered_prs,
            start_date=context.start_date,
            end_date=context.end_date,
        )

        rate = details.get("survival_rate", 0.0)
        total = details.get("total_contributed", 0)
        survived = details.get("total_survived", 0)

        summary = (
            f"Code survival rate: {rate:.1f}% "
            f"({survived}/{total} lines survived from {len(filtered_prs)} PRs)."
        )

        if details.get("no_data"):
            summary = "Insufficient data to compute code survival trend."

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
