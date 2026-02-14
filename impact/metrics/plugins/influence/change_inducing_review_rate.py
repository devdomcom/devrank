from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import review_led_to_commit


class ChangeInducingReviewRate(Metric):
    """
    Rate of reviews inducing immediate author commits (clear correlation).
    """

    @property
    def slug(self) -> str:
        return "change_inducing_review_rate"

    @property
    def name(self) -> str:
        return "Change-Inducing Review Rate"

    @property
    def description(self) -> str:
        return "% reviews followed by immediate commit (proximity/no-intervening)."

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

        inducing_count = 0
        per_review: list[dict] = []
        for rev in reviews:
            induced_change = review_led_to_commit(context.ledger, rev)
            if induced_change:
                inducing_count += 1
            per_review.append(
                {
                    "review_id": rev.id,
                    "pr_number": rev.pull_request_number,
                    "induced_change": induced_change,
                }
            )

        total_reviews = len(reviews)
        inducing_rate = inducing_count / total_reviews if total_reviews else 0.0

        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        summary = (
            f"{inducing_count}/{total_reviews} reviews induced changes ({inducing_rate:.2f} rate)."
        )
        details: dict[str, object] = {
            "total_reviews": total_reviews,
            "inducing_count": inducing_count,
            "inducing_rate": inducing_rate,
            "period_days": round(period_days, 1),
            "per_review": per_review,
        }

        if total_reviews == 0 or (period_days < 14 and total_reviews < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
