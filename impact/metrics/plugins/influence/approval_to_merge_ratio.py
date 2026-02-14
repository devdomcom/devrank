from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import approval_was_final


class ApprovalToMergeRatio(Metric):
    """
    Ratio of approvals that were final (no rework, direct to merge).
    """

    @property
    def slug(self) -> str:
        return "approval_to_merge_ratio"

    @property
    def name(self) -> str:
        return "Approval To Merge Ratio"

    @property
    def description(self) -> str:
        return "% approvals that were last activity leading to merge (no reworks)."

    @property
    def category(self) -> str:
        return "influence_review"

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Filter out self-reviews (reviews on user's own PRs)
        filtered_reviews = []
        for rev in reviews:
            pr = context.ledger.get_pr(rev.pull_request_number)
            if pr and pr.user.login != context.user_login:
                filtered_reviews.append(rev)
        reviews = filtered_reviews

        approvals = [r for r in reviews if r.state.value == "approved"]
        total_approvals = len(approvals)
        final_approvals = 0
        per_review: list[dict] = []
        for rev in approvals:
            was_final = approval_was_final(context.ledger, rev)
            if was_final:
                final_approvals += 1
            per_review.append(
                {
                    "review_id": rev.id,
                    "pr_number": rev.pull_request_number,
                    "was_final": was_final,
                }
            )
        ratio = final_approvals / total_approvals if total_approvals else 0.0

        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        summary = f"{final_approvals}/{total_approvals} approvals were final ({ratio:.2f} ratio)."
        details: dict[str, object] = {
            "total_approvals": total_approvals,
            "final_approvals": final_approvals,
            "ratio": ratio,
            "period_days": round(period_days, 1),
            "per_review": per_review,
        }

        if total_approvals == 0 or (period_days < 14 and total_approvals < 2):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
