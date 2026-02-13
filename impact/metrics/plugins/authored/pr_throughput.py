from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class PRThroughput(Metric):
    """
    Measures the volume of pull requests opened and merged by a user.

    This metric tracks how many PRs were opened and how many were merged within
    the specified time window, along with the merge ratio. Both opened and merged
    counts use PRs created in the window, ensuring the ratio is always <= 1.0.

    Details returned:
        - opened_count: Number of PRs opened in the window
        - merged_count: Number of PRs opened in the window that were eventually merged
        - merge_ratio: Ratio of merged to opened PRs (0.0 to 1.0)
        - opened_pr_numbers: List of opened PR numbers
        - merged_pr_numbers: List of merged PR numbers
    """

    @property
    def slug(self) -> str:
        return "pr_throughput"

    @property
    def name(self) -> str:
        return "PR Throughput"

    @property
    def description(self) -> str:
        return "Quantifies PR volume (opened/merged counts) and success ratio for productivity."

    @property
    def category(self) -> str:
        return "productivity_throughput"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        opened_count = len(prs)
        merged = [pr for pr in prs if pr.merged]
        merged_count = len(merged)
        merge_ratio = merged_count / opened_count if opened_count else 0.0

        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        summary = f"{opened_count} PRs opened, {merged_count} merged in window. Merge ratio: {merge_ratio:.2f}"
        details: dict[str, object] = {
            "opened_count": opened_count,
            "merged_count": merged_count,
            "merge_ratio": merge_ratio,
            "period_days": round(period_days, 1),
            "opened_pr_numbers": [pr.number for pr in prs],
            "merged_pr_numbers": [pr.number for pr in merged],
        }

        if opened_count == 0 or (period_days < 14 and opened_count < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
