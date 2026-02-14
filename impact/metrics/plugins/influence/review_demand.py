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

    @property
    def category(self) -> str:
        return "influence_review"

    def run(self, context: MetricContext) -> MetricResult:
        # Aggregate review_requested targeting the user (accurate demand via requested_reviewer)
        # Deduplicate by PR — count at most 1 request per PR (re-requests after
        # force-pushes on the same PR should not inflate demand)
        demand_count = 0
        requested_prs: set[int] = set()
        total_timeline_events = 0
        for pr in context.ledger.bundle.pull_requests:
            for evt in context.ledger.get_timeline_for_pr(pr.number):
                total_timeline_events += 1
                if (
                    evt.event == "review_requested"
                    and getattr(evt, "requested_reviewer", None)
                    and evt.requested_reviewer.login == context.user_login
                ):
                    if pr.number not in requested_prs:
                        requested_prs.add(pr.number)
                        demand_count += 1
                    break  # only count first request per PR
        per_pr = [{"pr_number": num, "requests": 1} for num in sorted(requested_prs)]

        # Normalize (DRY with reviews_given)
        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0
        demand_per_day = demand_count / period_days if period_days > 0 else 0.0
        demand_per_week = demand_per_day * 7

        # no_data: no timeline events at all means data may be incomplete,
        # OR zero demand requests, OR small sample in short period
        no_data = (
            total_timeline_events == 0
            or demand_count == 0
            or (period_days < 14 and demand_count < 2)
        )

        summary = f"{demand_count} review requests for you ({demand_per_week:.1f}/week)."
        if no_data:
            summary += " (no timeline data available)" if total_timeline_events == 0 else ""
        details: dict[str, object] = {
            "demand_count": demand_count,
            "period_days": round(period_days, 1),
            "demand_per_day": round(demand_per_day, 2),
            "demand_per_week": round(demand_per_week, 1),
            "affected_prs": len(requested_prs),
            "per_pr": per_pr,
            "no_data": no_data,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
