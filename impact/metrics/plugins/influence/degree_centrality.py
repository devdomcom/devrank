from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class DegreeCentrality(Metric):
    @property
    def slug(self) -> str:
        return "degree_centrality"

    @property
    def name(self) -> str:
        return "Degree Centrality"

    @property
    def description(self) -> str:
        return "Number of unique collaborators via review interactions (in + out degree in review network)."

    @property
    def category(self) -> str:
        return "review_impact"

    @property
    def frameworks(self) -> list[str]:
        return ["Network"]

    def run(self, context: MetricContext) -> MetricResult:
        # Get user's reviews (outgoing edges)
        reviews_given = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Get user's PRs to find incoming reviews
        user_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Collect collaborators (union of in and out)
        collaborators: set[str] = set()
        reviewers_on_my_prs: set[str] = set()
        authors_i_reviewed: set[str] = set()

        for review in reviews_given:
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            author = pr.user.login
            if author == context.user_login:
                continue
            authors_i_reviewed.add(author)
            collaborators.add(author)

        for pr in user_prs:
            pr_reviews = context.ledger.get_reviews_for_pr(pr.number)
            for review in pr_reviews:
                reviewer = review.user.login
                if reviewer == context.user_login:
                    continue
                reviewers_on_my_prs.add(reviewer)
                collaborators.add(reviewer)

        unique_count = len(collaborators)
        in_degree = len(reviewers_on_my_prs)
        out_degree = len(authors_i_reviewed)

        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        summary = (
            f"Degree centrality: {unique_count} unique collaborators "
            f"({out_degree} authors reviewed, {in_degree} reviewers on PRs)."
        )
        details: dict[str, object] = {
            "unique_collaborators": unique_count,
            "in_degree": in_degree,
            "out_degree": out_degree,
            "collaborators": sorted(collaborators),
            "period_days": round(period_days, 1),
            "reviewers_on_prs": sorted(reviewers_on_my_prs),
            "authors_reviewed": sorted(authors_i_reviewed),
        }
        if period_days < 21 and unique_count < 3:
            details["no_data"] = True

        # Explicit zero-connection guard (long period with no review partners)
        if unique_count == 0:
            details["no_data"] = True
            details["no_data_reason"] = "No review collaborations in period"
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
