from typing import Dict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


class PRThroughput(Metric):
    @property
    def slug(self) -> str:
        return "pr_throughput"

    @property
    def name(self) -> str:
        return "PR Throughput"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)
        merged_prs = context.ledger.get_merged_prs_for_user(context.user_login, context.start_date, context.end_date)

        opened_count = len(prs)
        merged_count = len(merged_prs)
        merge_ratio = merged_count / opened_count if opened_count else 0.0

        summary = f"{opened_count} PRs opened, {merged_count} merged in window. Merge ratio: {merge_ratio:.2f}"
        details: Dict[str, object] = {
            "opened_count": opened_count,
            "merged_count": merged_count,
            "merge_ratio": merge_ratio,
            "opened_pr_numbers": [pr.number for pr in prs],
            "merged_pr_numbers": [pr.number for pr in merged_prs],
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
