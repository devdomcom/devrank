from typing import Dict, List

from impact.metrics.base import Metric
from impact.metrics.utils import percentile
from impact.domain.models import MetricContext, MetricResult, ReviewState


class UnblockTime(Metric):
    """
    Median time for engineer to re-review after blocking CR (unblock speed).
    """

    @property
    def slug(self) -> str:
        return "unblock_time"

    @property
    def name(self) -> str:
        return "Unblock Time"

    @property
    def description(self) -> str:
        return "Median hours to re-review after blocking CR + commits (unblock speed)."

    def run(self, context: MetricContext) -> MetricResult:
        # User's CR reviews
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )
        cr_reviews = [r for r in reviews if r.state == ReviewState.CHANGES_REQUESTED]

        response_times: List[float] = []
        per_cr = []
        for cr in cr_reviews:
            pr_num = cr.pull_request_number
            # Commits after CR by author
            commits = [c for c in context.ledger.get_commits_for_pr(pr_num) if c.date > cr.submitted_at and c.author.login != context.user_login]
            if not commits:
                per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": None})
                continue
            first_commit = min(commits, key=lambda c: c.date)
            # User's next review after first commit
            later_reviews = [r for r in context.ledger.get_reviews_for_pr(pr_num) if r.submitted_at > first_commit.date and r.user.login == context.user_login]
            if not later_reviews:
                per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": None})
                continue
            next_review = min(later_reviews, key=lambda r: r.submitted_at)
            delta = next_review.submitted_at - cr.submitted_at  # or from commit?
            hours = delta.total_seconds() / 3600
            response_times.append(hours)
            per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": hours})

        median = percentile(response_times, 0.5) if response_times else 0.0
        p75 = percentile(response_times, 0.75) if response_times else 0.0

        summary = f"{len(per_cr)} CRs; median unblock: {median:.1f}h."
        details: Dict[str, object] = {
            "cr_count": len(per_cr),
            "median_hours": median,
            "p75_hours": p75,
            "per_cr": per_cr,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
