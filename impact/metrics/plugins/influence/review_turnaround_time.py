from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class ReviewTurnaroundTime(Metric):
    """
    Median time for engineer to act on opened PRs/review requests.
    """

    @property
    def slug(self) -> str:
        return "review_turnaround_time"

    @property
    def name(self) -> str:
        return "Review Turnaround Time"

    @property
    def description(self) -> str:
        return "Median hours to first review/action on opened PRs (balanced by period)."

    def run(self, context: MetricContext) -> MetricResult:
        # PRs reviewed by user (influence)
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # Unique PRs
        reviewed_prs = {}
        for r in reviews:
            if r.pull_request_number not in reviewed_prs:
                pr = context.ledger.get_pr(r.pull_request_number)
                if pr:
                    reviewed_prs[r.pull_request_number] = pr

        durations: list[float] = []
        per_pr = []
        for pr_num, pr in reviewed_prs.items():
            # User's first review on this PR
            user_reviews = [
                r
                for r in context.ledger.get_reviews_for_pr(pr_num)
                if r.user.login == context.user_login
            ]
            if not user_reviews:
                continue
            first_user_review = min(user_reviews, key=lambda r: r.submitted_at)
            delta = first_user_review.submitted_at - pr.created_at
            hours = delta.total_seconds() / 3600
            durations.append(hours)
            per_pr.append({"pr_number": pr_num, "hours": hours})

        median = percentile(durations, 0.5) if durations else 0.0
        p75 = percentile(durations, 0.75) if durations else 0.0

        # Balance by period length (industry avg assumption: adjust threshold implicitly via rate)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 10.0
        # e.g., response rate per day, but keep median time primary

        summary = f"{len(per_pr)} PRs reviewed; median: {median:.1f}h (period {period_days:.0f}d)."
        details: dict[str, object] = {
            "reviewed_prs": len(per_pr),
            "median_hours": median,
            "p75_hours": p75,
            "period_days": round(period_days, 1),
            "per_pr": per_pr,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
