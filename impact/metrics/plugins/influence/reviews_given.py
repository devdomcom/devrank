from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class ReviewsGiven(Metric):
    """
    Quantifies reviews provided to measure collaboration quantity.
    """

    @property
    def slug(self) -> str:
        return "reviews_given"

    @property
    def name(self) -> str:
        return "Reviews Given"

    @property
    def description(self) -> str:
        return "Count of reviews given (collaboration volume, normalized by period)."

    @property
    def category(self) -> str:
        return "review_impact"

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

        review_count = len(reviews)

        # Normalize by period (DRY with other time-based like trivial rate)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0
        reviews_per_day = review_count / period_days if period_days > 0 else 0.0
        reviews_per_week = reviews_per_day * 7

        summary = f"{review_count} reviews given ({reviews_per_week:.1f}/week)."
        details: dict[str, object] = {
            "review_count": review_count,
            "period_days": round(period_days, 1),
            "reviews_per_day": round(reviews_per_day, 2),
            "reviews_per_week": round(reviews_per_week, 1),
            "review_pr_numbers": list({r.pull_request_number for r in reviews}),
        }

        if period_days < 14 and review_count < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
