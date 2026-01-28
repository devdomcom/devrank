from typing import Dict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


class ReviewLeverage(Metric):
    @property
    def slug(self) -> str:
        return "review_leverage"

    @property
    def name(self) -> str:
        return "Review Leverage"

    def _is_effective_change_request(self, review, context):
        pr = next((p for p in context.ledger.bundle.pull_requests if p.number == review.pull_request_number), None)
        if pr and pr.merged:
            commits = context.ledger.get_commits_for_pr(review.pull_request_number)
            return any(c.date > review.submitted_at for c in commits)
        return False

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(context.user_login, context.start_date, context.end_date)
        change_requests = [r for r in reviews if r.state.value == "changes_requested"]

        if not change_requests:
            summary = "No change requests made."
            details = {}
        else:
            effective_changes = sum(1 for r in change_requests if self._is_effective_change_request(r, context))
            total_change_requests = len(change_requests)
            percentage = (effective_changes / total_change_requests) * 100 if total_change_requests > 0 else 0

            summary = f"{len(reviews)} PRs reviewed, {effective_changes} effective change requests out of {total_change_requests} ({percentage:.1f}%)"

            details = {
                "total_reviews": len(reviews),
                "change_requests": total_change_requests,
                "effective_changes": effective_changes,
                "effectiveness_percentage": percentage,
                "change_request_details": [
                    {
                        "pr_number": r.pull_request_number,
                        "effective": self._is_effective_change_request(r, context)
                    } for r in change_requests
                ]
            }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details
        )