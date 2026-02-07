from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import review_led_to_merge


class PRMergeRate(Metric):
    """
    Rate at which engineer's reviews lead to PR merges (proximity, no intervening).
    """

    @property
    def slug(self) -> str:
        return "pr_merge_rate"

    @property
    def name(self) -> str:
        return "PR Merge Rate"

    @property
    def description(self) -> str:
        return "Proportion of reviews leading to merge (close sequence, no other interveners)."

    def run(self, context: MetricContext) -> MetricResult:
        # User's reviews (influence on others' PRs)
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        effective_count = 0
        per_review: list[dict] = []
        for rev in reviews:
            led_to_merge = review_led_to_merge(context.ledger, rev)
            if led_to_merge:
                effective_count += 1
            per_review.append(
                {
                    "review_id": rev.id,
                    "pr_number": rev.pull_request_number,
                    "led_to_merge": led_to_merge,
                }
            )

        total_reviews = len(reviews)
        merge_rate = effective_count / total_reviews if total_reviews else 0.0

        summary = f"{effective_count}/{total_reviews} reviews led to merge ({merge_rate:.2f} rate)."
        details: dict[str, object] = {
            "total_reviews": total_reviews,
            "effective_merges": effective_count,
            "merge_rate": merge_rate,
            "per_review": per_review,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
