from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_change_request


class ReviewIterations(Metric):
    """
    Count how many change-request cycles a PR authored by the user went through before merge.
    """

    @property
    def slug(self) -> str:
        return "review_iterations"

    @property
    def name(self) -> str:
        return "Review Iterations"

    @property
    def description(self) -> str:
        return "Avg change-request cycles per merged PR (review churn/rounds)."

    @property
    def category(self) -> str:
        return "responsiveness"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean", "DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        merged = [pr for pr in prs if pr.merged]

        per_pr = []
        counts: list[int] = []
        for pr in merged:
            reviews = context.ledger.get_reviews_for_pr(pr.number)
            changes_requested = [r for r in reviews if is_change_request(r, context.ledger)]
            cr_times = sorted([r.submitted_at for r in changes_requested])
            cycles = 0
            last_cycle_time = None
            for t in cr_times:
                if last_cycle_time is None or (t - last_cycle_time).total_seconds() > 4 * 3600:
                    cycles += 1
                    last_cycle_time = t
            per_pr.append({"number": pr.number, "iterations": cycles})
            counts.append(cycles)

        avg = sum(counts) / len(counts) if counts else 0.0
        summary = f"{len(merged)} merged PRs; avg iterations: {avg:.2f}"
        details: dict[str, object] = {
            "merged_prs": len(merged),
            "average_iterations": avg,
            "per_pr": per_pr,
            "period_days": period_days,
        }
        if not counts or (period_days < 14 and len(counts) < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
