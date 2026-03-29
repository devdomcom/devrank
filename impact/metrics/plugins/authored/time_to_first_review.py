from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class TimeToFirstReview(Metric):
    """
    Time from PR creation to first review by someone other than the author.
    """

    @property
    def slug(self) -> str:
        return "time_to_first_review"

    @property
    def name(self) -> str:
        return "Time to First Review"

    @property
    def description(self) -> str:
        return "Median time from PR creation to initial reviewer feedback."

    @property
    def category(self) -> str:
        return "responsiveness"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        durations: list[float] = []
        per_pr = []
        for pr in prs:
            reviews = [
                r
                for r in context.ledger.get_reviews_for_pr(pr.number)
                if r.user.login != pr.user.login
            ]
            if not reviews:
                per_pr.append({"number": pr.number, "hours": None})
                continue
            first = min(reviews, key=lambda r: r.submitted_at)
            delta = first.submitted_at - pr.created_at
            hours = delta.total_seconds() / 3600
            durations.append(hours)
            per_pr.append({"number": pr.number, "hours": hours})

        median = percentile(durations, 0.5) if durations else 0.0
        p75 = percentile(durations, 0.75) if durations else 0.0
        summary = f"{len([p for p in per_pr if p['hours'] is not None])} PRs reviewed; median: {median:.2f}h, p75: {p75:.2f}h"
        details: dict[str, object] = {
            "reviewed_prs": len([p for p in per_pr if p["hours"] is not None]),
            "median_hours": median,
            "p75_hours": p75,
            "per_pr": per_pr,
        }
        if not durations:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
