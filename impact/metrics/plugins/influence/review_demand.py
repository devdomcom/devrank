from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class ReviewDemand(Metric):
    """Quantifies demand for user's reviews via review_requested timeline events (folks seeking your input)."""

    @property
    def slug(self) -> str:
        return "review_demand"

    @property
    def name(self) -> str:
        return "Review Demand"

    @property
    def description(self) -> str:
        return "Count of review_requested events (demand for your reviews; normalized by period)."

    def run(self, context: MetricContext) -> MetricResult:
        # Aggregate review_requested targeting the user (accurate demand via requested_reviewer)
        # Accumulate per-PR (re-requests after push count separately; no de-dup)
        demand_count = 0
        pr_requests = defaultdict(int)
        for pr in context.ledger.bundle.pull_requests:
            for evt in context.ledger.get_timeline_for_pr(pr.number):
                if (
                    evt.event == "review_requested"
                    and getattr(evt, "requested_reviewer", None)
                    and evt.requested_reviewer.login == context.user_login
                ):
                    demand_count += 1
                    pr_requests[pr.number] += 1
        per_pr = [{"pr_number": num, "requests": count} for num, count in sorted(pr_requests.items())]

        # Normalize (DRY with reviews_given)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0
        demand_per_day = demand_count / period_days if period_days > 0 else 0.0
        demand_per_week = demand_per_day * 7

        summary = f"{demand_count} review requests for you ({demand_per_week:.1f}/week)."
        details: dict[str, object] = {
            "demand_count": demand_count,
            "period_days": round(period_days, 1),
            "demand_per_day": round(demand_per_day, 2),
            "demand_per_week": round(demand_per_week, 1),
            "affected_prs": len(pr_requests),
            "per_pr": per_pr,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
