from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class ReviewBreadth(Metric):
    @property
    def slug(self) -> str:
        return "review_breadth"

    @property
    def name(self) -> str:
        return "Review Breadth"

    @property
    def description(self) -> str:
        return "Number of distinct PR authors reviewed (cross-team reach and mentorship surface)."

    @property
    def category(self) -> str:
        return "review_impact"

    @property
    def frameworks(self) -> list[str]:
        return ["SPACE", "Network"]

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Compute period length for no_data guard
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        # Collect unique PR authors (excluding self-reviews)
        author_pr_counts: dict[str, set[int]] = {}
        for review in reviews:
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            if pr.user.login == context.user_login:
                continue
            author = pr.user.login
            if author not in author_pr_counts:
                author_pr_counts[author] = set()
            author_pr_counts[author].add(pr.number)

        unique_authors = len(author_pr_counts)
        per_author = [
            {"author": author, "prs_reviewed": len(pr_nums)}
            for author, pr_nums in sorted(
                author_pr_counts.items(), key=lambda x: -len(x[1])
            )
        ]

        summary = (
            f"Reviewed PRs from {unique_authors} distinct authors "
            f"({sum(len(v) for v in author_pr_counts.values())} PRs total)."
        )
        details: dict[str, object] = {
            "unique_authors": unique_authors,
            "per_author": per_author,
            "total_prs_reviewed": sum(len(v) for v in author_pr_counts.values()),
            "period_days": round(period_days, 1),
        }
        if period_days < 21 and len(reviews) < 3:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
