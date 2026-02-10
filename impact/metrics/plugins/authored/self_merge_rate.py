from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_self_merge_rate, filter_prs_for_contribution


class SelfMergeRate(Metric):
    @property
    def slug(self) -> str:
        return "self_merge_rate"

    @property
    def name(self) -> str:
        return "Self-Merge Rate"

    @property
    def description(self) -> str:
        return "% PRs (own+others) merged by author w/o approval; bad if repo culture uses reviews."

    def run(self, context: MetricContext) -> MetricResult:
        details = compute_self_merge_rate(context.ledger, context.user_login)
        rate = details["self_merge_rate"]
        summary = f"Self-merge rate: {rate:.1f}% ({details['no_approval_count']}/{details['engineer_merged_count']} merges; repo {details['repo_self_merge_rate']:.1f}%)."
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
