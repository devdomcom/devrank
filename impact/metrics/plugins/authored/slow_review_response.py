from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_change_request, percentile


class SlowReviewResponse(Metric):
    """
    Measures how long it takes the PR author to push a new commit after a changes-requested review.
    """

    @property
    def slug(self) -> str:
        return "slow_review_response"

    @property
    def name(self) -> str:
        return "Slow Review Response"

    @property
    def description(self) -> str:
        return "Median author response time to changes-requested reviews."

    @property
    def category(self) -> str:
        return "responsiveness"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = [pr for pr in prs if pr.merged]  # only closed/merged PRs for responsiveness

        response_times: list[float] = []
        per_review = []

        for pr in prs:
            commits = [
                c
                for c in context.ledger.get_commits_for_pr(pr.number)
                if c.author.login == pr.user.login
            ]
            commits.sort(key=lambda c: c.date)
            reviews = context.ledger.get_reviews_for_pr(pr.number)
            for review in reviews:
                if not is_change_request(review, context.ledger):
                    continue
                next_commit = next((c for c in commits if c.date > review.submitted_at), None)
                if not next_commit:
                    per_review.append({"pr": pr.number, "review_id": review.id, "hours": None})
                    continue
                delta = next_commit.date - review.submitted_at
                hours = delta.total_seconds() / 3600
                response_times.append(hours)
                per_review.append({"pr": pr.number, "review_id": review.id, "hours": hours})

        median = percentile(response_times, 0.5) if response_times else 0.0
        p75 = percentile(response_times, 0.75) if response_times else 0.0
        summary = (
            f"{len(response_times)} responses measured; median: {median:.2f}h, p75: {p75:.2f}h"
        )
        details: dict[str, object] = {
            "samples": len(response_times),
            "median_hours": median,
            "p75_hours": p75,
            "per_review": per_review,
        }
        if not response_times:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
