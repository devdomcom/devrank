from typing import Dict, List

from impact.metrics.base import Metric
from impact.metrics.utils import approval_was_final
from impact.domain.models import MetricContext, MetricResult


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

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        final_approvals = 0
        per_review: List[dict] = []
        for rev in reviews:
            was_final = approval_was_final(context.ledger, rev)
            if was_final:
                final_approvals += 1
            per_review.append({
                "review_id": rev.id,
                "pr_number": rev.pull_request_number,
                "was_final": was_final,
            })

        total_approvals = len([r for r in reviews if r.state.value == "approved"])
        ratio = final_approvals / total_approvals if total_approvals else 0.0

        summary = f"{final_approvals}/{total_approvals} approvals were final ({ratio:.2f} ratio)."
        details: Dict[str, object] = {
            "total_approvals": total_approvals,
            "final_approvals": final_approvals,
            "ratio": ratio,
            "per_review": per_review,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
