from typing import Dict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


class PRThroughput(Metric):
    """
    Measures the volume of pull requests opened and merged by a user.

    This metric tracks how many PRs were opened and how many were merged within
    the specified time window, along with the merge ratio. Higher throughput
    with a good merge ratio indicates productive contribution.

    Details returned:
        - opened_count: Number of PRs opened in the window
        - merged_count: Number of PRs merged in the window
        - merge_ratio: Ratio of merged to opened PRs
        - opened_pr_numbers: List of opened PR numbers
        - merged_pr_numbers: List of merged PR numbers
    """

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
