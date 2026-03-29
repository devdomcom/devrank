"""
WIP Load (Work In Progress)

Lean metric measuring concurrent open PRs over the analysis period.
High WIP signals context-switching overhead and flow bottlenecks.
Low WIP with steady throughput indicates healthy Lean flow.

Uses existing PR data (created_at, closed_at) -- no new data sources needed.
"""

from datetime import timedelta

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class WIPLoad(Metric):
    """
    Work In Progress Load: concurrent open PRs over the analysis period.

    For each day in the period, counts PRs that are open (created_at <= day
    and (closed_at > day or still open)). Reports max, average, and daily
    WIP time series.

    Lean WIP limits recommendation: WIP > 3 concurrent PRs per developer
    typically indicates overcommitment.
    """

    @property
    def slug(self) -> str:
        return "wip_load"

    @property
    def name(self) -> str:
        return "WIP Load"

    @property
    def description(self) -> str:
        return "Concurrent open PRs per day (Lean WIP indicator; high WIP = context-switching overhead)."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean", "Kanban"]

    def run(self, context: MetricContext) -> MetricResult:
        if not context.start_date or not context.end_date:
            return MetricResult(
                metric_slug=self.slug,
                summary="Cannot compute WIP without date range.",
                details={"no_data": True},
            )

        period_days = (context.end_date - context.start_date).total_seconds() / 86400

        # Get ALL user PRs (not just merged) -- WIP counts open PRs
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        # Also include PRs created before the window that were still open during it
        pre_window_prs = [
            pr for pr in context.ledger.get_prs_for_user(context.user_login)
            if pr.created_at and pr.created_at < context.start_date
            and (not pr.closed_at or pr.closed_at > context.start_date)
        ]
        all_relevant_prs = list(prs) + pre_window_prs

        # Calculate daily WIP
        daily_wip: list[dict] = []
        wip_values: list[int] = []
        current_day = context.start_date.date()
        end_day = context.end_date.date()

        while current_day <= end_day:
            # Count PRs open on this day
            count = 0
            for pr in all_relevant_prs:
                if not pr.created_at:
                    continue
                pr_created = pr.created_at.date()
                pr_closed = pr.closed_at.date() if pr.closed_at else None

                if pr_created <= current_day and (pr_closed is None or pr_closed > current_day):
                    count += 1

            wip_values.append(count)
            daily_wip.append({
                "date": current_day.isoformat(),
                "wip": count,
            })
            current_day += timedelta(days=1)

        max_wip = max(wip_values) if wip_values else 0
        avg_wip = sum(wip_values) / len(wip_values) if wip_values else 0.0

        summary = (
            f"WIP Load: max {max_wip}, avg {avg_wip:.1f} concurrent PRs "
            f"over {len(wip_values)} days."
        )

        details: dict[str, object] = {
            "max_concurrent_prs": max_wip,
            "avg_concurrent_prs": round(avg_wip, 2),
            "total_prs_in_period": len(all_relevant_prs),
            "daily_wip": daily_wip,
            "period_days": round(period_days, 1),
        }

        if not all_relevant_prs or (period_days < 7 and len(all_relevant_prs) < 2):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
