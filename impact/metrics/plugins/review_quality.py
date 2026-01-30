from datetime import timedelta
from typing import Dict, List

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult, ReviewState


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] * (c - k) + values[c] * (k - f)


class ReviewIterations(Metric):
    """
    Count how many change-request cycles a PR authored by the user went through before merge.
    """

    @property
    def slug(self) -> str:
        return "review_iterations"

    @property
    def name(self) -> str:
        return "Review Iterations"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)
        merged = [pr for pr in prs if pr.merged]

        per_pr = []
        counts: List[int] = []
        for pr in merged:
            reviews = context.ledger.get_reviews_for_pr(pr.number)
            changes_requested = [r for r in reviews if r.state == ReviewState.CHANGES_REQUESTED]
            per_pr.append({"number": pr.number, "iterations": len(changes_requested)})
            counts.append(len(changes_requested))

        avg = sum(counts) / len(counts) if counts else 0.0
        summary = f"{len(merged)} merged PRs; avg iterations: {avg:.2f}"
        details: Dict[str, object] = {
            "merged_prs": len(merged),
            "average_iterations": avg,
            "per_pr": per_pr,
        }
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)


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

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)

        durations: List[float] = []
        per_pr = []
        for pr in prs:
            reviews = [r for r in context.ledger.get_reviews_for_pr(pr.number) if r.user.login != pr.user.login]
            if not reviews:
                per_pr.append({"number": pr.number, "hours": None})
                continue
            first = min(reviews, key=lambda r: r.submitted_at)
            delta = first.submitted_at - pr.created_at
            hours = delta.total_seconds() / 3600
            durations.append(hours)
            per_pr.append({"number": pr.number, "hours": hours})

        median = _percentile(durations, 0.5) if durations else 0.0
        p75 = _percentile(durations, 0.75) if durations else 0.0
        summary = f"{len([p for p in per_pr if p['hours'] is not None])} PRs reviewed; median: {median:.2f}h, p75: {p75:.2f}h"
        details: Dict[str, object] = {
            "reviewed_prs": len([p for p in per_pr if p["hours"] is not None]),
            "median_hours": median,
            "p75_hours": p75,
            "per_pr": per_pr,
        }
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)


class SlowReviewResponse(Metric):
    """
    Measures how long it takes the PR author to push a new commit after a changes-requested review.
    """

    @property
    def slug(self) -> str:
        return "slow_review_response"

    @property
    def name(self) -> str:
        return "Slow Review Response"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)
        prs = [pr for pr in prs if pr.merged]  # only closed/merged PRs for responsiveness

        response_times: List[float] = []
        per_review = []

        for pr in prs:
            commits = [c for c in context.ledger.get_commits_for_pr(pr.number) if c.author.login == pr.user.login]
            commits.sort(key=lambda c: c.date)
            reviews = context.ledger.get_reviews_for_pr(pr.number)
            for review in reviews:
                if review.state != ReviewState.CHANGES_REQUESTED:
                    continue
                # find first author commit after review
                next_commit = next((c for c in commits if c.date > review.submitted_at), None)
                if not next_commit:
                    per_review.append({"pr": pr.number, "review_id": review.id, "hours": None})
                    continue
                delta = next_commit.date - review.submitted_at
                hours = delta.total_seconds() / 3600
                response_times.append(hours)
                per_review.append({"pr": pr.number, "review_id": review.id, "hours": hours})

        median = _percentile(response_times, 0.5) if response_times else 0.0
        p75 = _percentile(response_times, 0.75) if response_times else 0.0
        summary = f"{len(response_times)} responses measured; median: {median:.2f}h, p75: {p75:.2f}h"
        details: Dict[str, object] = {
            "samples": len(response_times),
            "median_hours": median,
            "p75_hours": p75,
            "per_review": per_review,
        }
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
