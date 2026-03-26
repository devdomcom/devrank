from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import percentile


class ReviewTurnaroundTime(Metric):
    """
    Median time for engineer to act on opened PRs/review requests.
    """

    @property
    def slug(self) -> str:
        return "review_turnaround_time"

    @property
    def name(self) -> str:
        return "Review Turnaround Time"

    @property
    def description(self) -> str:
        return "Median hours to first review/action on opened PRs (balanced by period)."

    @property
    def category(self) -> str:
        return "responsiveness"

    @property
    def frameworks(self) -> list[str]:
        return ["SPACE", "Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        # PRs reviewed by user (influence)
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

        # Unique PRs
        reviewed_prs = {}
        for r in reviews:
            if r.pull_request_number not in reviewed_prs:
                pr = context.ledger.get_pr(r.pull_request_number)
                if pr:
                    reviewed_prs[r.pull_request_number] = pr

        durations: list[float] = []
        per_pr = []
        for pr_num, pr in reviewed_prs.items():
            # User's first review on this PR
            user_reviews = [
                r
                for r in context.ledger.get_reviews_for_pr(pr_num)
                if r.user.login == context.user_login
            ]
            if not user_reviews:
                continue
            first_user_review = min(user_reviews, key=lambda r: r.submitted_at)

            # Try to use review_requested timeline event if available
            # to measure from when the reviewer was actually assigned
            reference_time = pr.created_at
            timeline_events = context.ledger.get_timeline_for_pr(pr_num)
            review_requested_events = [
                evt for evt in timeline_events
                if evt.event == "review_requested"
                and evt.created_at <= first_user_review.submitted_at
            ]
            if review_requested_events:
                # Use the latest review_requested event before the first review
                latest_request = max(review_requested_events, key=lambda e: e.created_at)
                reference_time = latest_request.created_at

            delta = first_user_review.submitted_at - reference_time
            hours = delta.total_seconds() / 3600
            note = None
            if not review_requested_events:
                note = "Measured from PR creation; may overcount if reviewer was assigned late"
            entry: dict = {"pr_number": pr_num, "hours": hours}
            if note:
                entry["note"] = note
            durations.append(hours)
            per_pr.append(entry)

        if durations:
            median = percentile(durations, 0.5)
            p75 = percentile(durations, 0.75)
        else:
            median = 0.0
            p75 = 0.0

        # Balance by period length
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        summary = f"{len(per_pr)} PRs reviewed; median: {median:.1f}h (period {period_days:.0f}d)."
        details: dict[str, object] = {
            "reviewed_prs": len(per_pr),
            "median_hours": median,
            "p75_hours": p75,
            "period_days": round(period_days, 1),
            "per_pr": per_pr,
        }

        if not durations and period_days >= 14:
            # Long period with zero reviews: use entire period as turnaround → rates "bad"
            median = period_days * 24
            p75 = median
            details["median_hours"] = median
            details["p75_hours"] = p75
        elif period_days < 14 and len(per_pr) < 3:
            details["no_data"] = True
            details["note"] = "Measured from PR creation; may overcount if reviewer was assigned late"

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
