from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_sum_of_coupling, filter_prs_for_contribution


class SumOfCoupling(Metric):
    """Sum of Coupling (SoC) per entity across all revisions.

    SoC per entity = sum of times it changed together with any other entity
    across all PRs in the period. Higher SoC indicates more coupling risk.
    """

    @property
    def slug(self) -> str:
        return "sum_of_coupling"

    @property
    def name(self) -> str:
        return "Sum of Coupling"

    @property
    def description(self) -> str:
        return "Per-entity total coupling score across revisions (higher = more coupling risk)."

    @property
    def category(self) -> str:
        return "code_quality"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=False)

        details = compute_sum_of_coupling(
            context.ledger,
            filtered_prs,
            start_date=context.start_date,
            end_date=context.end_date,
        )

        max_score = details.get("max_coupling_score", 0)
        total_entities = details.get("total_entities", 0)
        summary = f"Sum of coupling computed for {total_entities} entities (max SoC: {max_score})."

        if details.get("no_data"):
            summary = "Insufficient data to compute sum of coupling."
        elif details.get("per_entity"):
            top_entity = details["per_entity"][0]
            summary += f" Top coupled entity: {top_entity['entity']} (SoC: {top_entity['coupling_score']})."

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
