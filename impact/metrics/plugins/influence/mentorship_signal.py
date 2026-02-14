from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class MentorshipSignal(Metric):
    """Rate of reviewing PRs from low-activity contributors (fewer than N PRs in the period)."""

    # Threshold: authors with fewer than this many PRs are considered "junior/new"
    JUNIOR_PR_THRESHOLD = 5

    @property
    def slug(self) -> str:
        return "mentorship_signal"

    @property
    def name(self) -> str:
        return "Mentorship Signal"

    @property
    def description(self) -> str:
        return "% reviews targeting low-activity contributors (<5 PRs in period); mentorship investment."

    @property
    def category(self) -> str:
        return "influence_review"

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Scale the junior threshold by period length (~5 PRs/month baseline, min 2)
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 30
        junior_threshold = max(2, int(self.JUNIOR_PR_THRESHOLD * (period_days / 30.0)))

        # Build activity map: how many PRs each author has in the period
        author_pr_counts: dict[str, int] = {}

        # Collect unique reviewed PRs (exclude self-reviews)
        reviewed_authors: list[dict] = []
        seen_prs: set[int] = set()

        for review in reviews:
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            if pr.user.login == context.user_login:
                continue
            if pr.number in seen_prs:
                continue
            seen_prs.add(pr.number)

            author = pr.user.login
            # Lazily compute author's PR count in the period
            if author not in author_pr_counts:
                author_prs = context.ledger.get_prs_for_user(
                    author, context.start_date, context.end_date
                )
                author_pr_counts[author] = len(author_prs)

            is_junior = author_pr_counts[author] < junior_threshold
            reviewed_authors.append({
                "pr_number": pr.number,
                "author": author,
                "author_pr_count": author_pr_counts[author],
                "is_junior": is_junior,
            })

        total_reviewed = len(reviewed_authors)
        junior_reviews = sum(1 for r in reviewed_authors if r["is_junior"])
        mentorship_rate = (junior_reviews / total_reviewed * 100) if total_reviewed else 0.0

        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        summary = (
            f"Mentorship signal: {mentorship_rate:.1f}% of reviews target low-activity authors "
            f"({junior_reviews}/{total_reviewed} PRs)."
        )
        details: dict[str, object] = {
            "mentorship_rate": mentorship_rate,
            "junior_review_count": junior_reviews,
            "total_reviewed_prs": total_reviewed,
            "junior_threshold": junior_threshold,
            "period_days": round(period_days, 1),
            "per_review": reviewed_authors,
        }
        if total_reviewed == 0 or (period_days < 21 and total_reviewed < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
