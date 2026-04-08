from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class PickupTime(Metric):
    """
    Time from PR creation to the first non-author activity (review, comment,
    or review_requested timeline event).  Measures how long a PR sits idle
    before someone picks it up — a key Lean flow metric.
    """

    @property
    def slug(self) -> str:
        return "pickup_time"

    @property
    def name(self) -> str:
        return "Pickup Time"

    @property
    def description(self) -> str:
        return "Median time from PR opened to first non-author review activity."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date,
        )

        durations: list[float] = []
        per_pr: list[dict] = []

        for pr in prs:
            first_activity = self._first_non_author_activity(pr, context)
            if first_activity is None:
                per_pr.append({"number": pr.number, "hours": None})
                continue
            hours = (first_activity - pr.created_at).total_seconds() / 3600
            durations.append(hours)
            per_pr.append({"number": pr.number, "hours": round(hours, 2)})

        median = percentile(durations, 0.5) if durations else 0.0
        p75 = percentile(durations, 0.75) if durations else 0.0
        picked_up = len(durations)

        summary = f"{picked_up} PRs picked up; median: {median:.2f}h, p75: {p75:.2f}h"
        details: dict[str, object] = {
            "picked_up_prs": picked_up,
            "median_hours": round(median, 2),
            "p75_hours": round(p75, 2),
            "per_pr": per_pr,
            "period_days": period_days,
        }
        if not durations or (period_days < 14 and len(durations) < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)

    @staticmethod
    def _first_non_author_activity(pr, context) -> "datetime | None":
        """Earliest non-author signal: review, review comment, or review_requested event."""
        from datetime import datetime

        author = pr.user.login
        candidates: list[datetime] = []

        # Reviews by non-author
        for r in context.ledger.get_reviews_for_pr(pr.number):
            if r.user.login != author:
                candidates.append(r.submitted_at)

        # Review comments by non-author
        for c in context.ledger.get_comments_for_pr(pr.number):
            if c.user.login != author:
                candidates.append(c.created_at)

        # Timeline: review_requested events (someone was asked to review)
        for evt in context.ledger.get_timeline_for_pr(pr.number):
            if evt.event == "review_requested" and evt.actor.login != author:
                candidates.append(evt.created_at)

        return min(candidates) if candidates else None
