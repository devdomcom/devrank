from impact.domain.models import MetricContext, MetricResult, ReviewState
from impact.metrics.base import Metric


class ContestedReviewRate(Metric):
    """% of change-request reviews that were ignored (no follow-up commit, or merged without re-review)."""

    @property
    def slug(self) -> str:
        return "contested_review_rate"

    @property
    def name(self) -> str:
        return "Contested Review Rate"

    @property
    def description(self) -> str:
        return "% change-request reviews ignored (no follow-up commit or merged without re-review)."

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Only consider change-request reviews on others' PRs
        cr_reviews: list[dict] = []
        contested_count = 0

        for review in reviews:
            if review.state != ReviewState.CHANGES_REQUESTED:
                continue
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            # Skip self-reviews
            if pr.user.login == context.user_login:
                continue

            rev_time = review.submitted_at

            # Check if PR author pushed a commit after this CR
            commits_after = [
                c for c in context.ledger.get_commits_for_pr(pr.number)
                if c.date > rev_time and c.author.login == pr.user.login
            ]

            # Check if this reviewer re-reviewed after any follow-up commit
            later_reviews_by_user = [
                r for r in context.ledger.get_reviews_for_pr(pr.number)
                if r.user.login == context.user_login
                and r.submitted_at > rev_time
            ]

            if not commits_after:
                # No follow-up commits at all — CR was ignored
                # But only count as contested if PR was merged anyway
                if pr.merged:
                    contested = True
                else:
                    # PR still open or closed without merge — not necessarily contested
                    contested = False
            elif pr.merged and not later_reviews_by_user:
                # Author pushed commits but merged without this reviewer re-reviewing
                contested = True
            else:
                contested = False

            cr_reviews.append({
                "pr_number": pr.number,
                "review_id": review.id,
                "commits_after": len(commits_after),
                "re_reviewed": len(later_reviews_by_user) > 0,
                "pr_merged": pr.merged,
                "contested": contested,
            })

            if contested:
                contested_count += 1

        total_crs = len(cr_reviews)
        rate = (contested_count / total_crs * 100) if total_crs else 0.0

        summary = (
            f"Contested review rate: {rate:.1f}% "
            f"({contested_count}/{total_crs} change requests were ignored/overridden)."
        )
        details: dict[str, object] = {
            "contested_rate": rate,
            "contested_count": contested_count,
            "total_change_requests": total_crs,
            "per_review": cr_reviews,
        }
        if total_crs == 0:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
