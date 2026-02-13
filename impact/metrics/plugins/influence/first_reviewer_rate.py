from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class FirstReviewerRate(Metric):
    """% of reviews where user was first (driving review position)."""

    @property
    def slug(self) -> str:
        return "first_reviewer_rate"

    @property
    def name(self) -> str:
        return "First Reviewer Rate"

    @property
    def description(self) -> str:
        return "% reviews where user was the first reviewer on PR (position driver)."

    @property
    def category(self) -> str:
        return "influence_review"

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Filter out self-reviews
        filtered_reviews = []
        for rev in reviews:
            pr = context.ledger.get_pr(rev.pull_request_number)
            if pr and pr.user.login != context.user_login:
                filtered_reviews.append(rev)
        reviews = filtered_reviews

        first_count = 0
        per_review = []
        for rev in reviews:
            pr = context.ledger.get_pr(rev.pull_request_number)
            pr_reviews = context.ledger.get_reviews_for_pr(rev.pull_request_number)
            # Exclude PR author's self-reviews: only external reviewers count
            external_reviews = [
                r for r in pr_reviews
                if not pr or r.user.login != pr.user.login
            ]
            all_sorted = sorted(external_reviews, key=lambda r: r.submitted_at)
            is_first = all_sorted and all_sorted[0].id == rev.id
            if is_first:
                first_count += 1
            per_review.append(
                {
                    "review_id": rev.id,
                    "pr_number": rev.pull_request_number,
                    "is_first": is_first,
                }
            )

        total = len(reviews)
        rate = first_count / total if total else 0.0

        # Period normalize (DRY)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0
        first_per_day = first_count / period_days if period_days > 0 else 0.0

        summary = f"{first_count}/{total} first reviews ({rate:.2f} rate, {first_per_day:.2f}/day)."
        details: dict[str, object] = {
            "total_reviews": total,
            "first_count": first_count,
            "rate": rate,
            "period_days": round(period_days, 1),
            "first_per_day": round(first_per_day, 2),
            "per_review": per_review,
        }

        if total == 0 or (period_days < 14 and total < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
