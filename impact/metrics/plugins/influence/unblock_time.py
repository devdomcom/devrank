from impact.domain.models import MetricContext, MetricResult, ReviewState
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class UnblockTime(Metric):
    """
    Median time for engineer to re-review after blocking CR (unblock speed).
    """

    @property
    def slug(self) -> str:
        return "unblock_time"

    @property
    def name(self) -> str:
        return "Unblock Time"

    @property
    def description(self) -> str:
        return "Median hours to re-review after blocking CR + commits (unblock speed)."

    @property
    def category(self) -> str:
        return "responsiveness"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        # User's CR reviews
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

        cr_reviews = [r for r in reviews if r.state == ReviewState.CHANGES_REQUESTED]

        response_times: list[float] = []
        per_cr = []
        for cr in cr_reviews:
            pr_num = cr.pull_request_number
            pr = context.ledger.get_pr(pr_num)
            if not pr:
                per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": None})
                continue
            # Commits after CR by the PR author (not "not the reviewer")
            commits = [
                c
                for c in context.ledger.get_commits_for_pr(pr_num)
                if c.date > cr.submitted_at and c.author.login == pr.user.login
            ]
            if not commits:
                per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": None})
                continue
            first_commit = min(commits, key=lambda c: c.date)
            # User's next review after first commit
            later_reviews = [
                r
                for r in context.ledger.get_reviews_for_pr(pr_num)
                if r.submitted_at > first_commit.date and r.user.login == context.user_login
            ]
            if not later_reviews:
                per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": None})
                continue
            next_review = min(later_reviews, key=lambda r: r.submitted_at)
            delta = next_review.submitted_at - first_commit.date
            hours = delta.total_seconds() / 3600
            response_times.append(hours)
            per_cr.append({"cr_id": cr.id, "pr_number": pr_num, "hours": hours})

        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        if response_times:
            median = percentile(response_times, 0.5)
            p75 = percentile(response_times, 0.75)
            summary = f"{len(per_cr)} CRs; median unblock: {median:.1f}h."
        else:
            median = 0.0
            p75 = 0.0
            summary = "No blocking CRs in period."
        details: dict[str, object] = {
            "cr_count": len(per_cr),
            "median_hours": median,
            "p75_hours": p75,
            "period_days": round(period_days, 1),
            "per_cr": per_cr,
            "no_cr_activity": len(cr_reviews) == 0,
            "no_data": len(response_times) == 0 or (period_days < 21 and len(response_times) < 2),
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
