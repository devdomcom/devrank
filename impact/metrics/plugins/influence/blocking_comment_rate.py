from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_change_request


class BlockingCommentRate(Metric):
    """
    % blocking change requests (vs cosmetic) to show ownership.
    """

    @property
    def slug(self) -> str:
        return "blocking_comment_rate"

    @property
    def name(self) -> str:
        return "Blocking Comment Rate"

    @property
    def description(self) -> str:
        return "% blocking CRs/comments (ownership vs cosmetic feedback)."

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

        blocking_count = 0
        per_review: list[dict] = []
        for rev in reviews:
            is_blocking = is_change_request(rev, context.ledger)  # DRY reuse
            if is_blocking:
                blocking_count += 1
            per_review.append(
                {
                    "review_id": rev.id,
                    "pr_number": rev.pull_request_number,
                    "is_blocking": is_blocking,
                }
            )

        total_reviews = len(reviews)
        blocking_rate = blocking_count / total_reviews if total_reviews else 0.0

        # Balance by period (DRY with trivial/review turnaround)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0
        blocking_per_day = blocking_count / period_days if period_days > 0 else 0.0

        summary = f"{blocking_count}/{total_reviews} blocking ({blocking_rate:.2f} rate, {blocking_per_day:.2f}/day)."
        details: dict[str, object] = {
            "total_reviews": total_reviews,
            "blocking_count": blocking_count,
            "blocking_rate": blocking_rate,
            "period_days": round(period_days, 1),
            "blocking_per_day": round(blocking_per_day, 2),
            "per_review": per_review,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
