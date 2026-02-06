from typing import Dict

from impact.metrics.base import Metric
from impact.metrics.utils import get_weekly_activity_counts
from impact.domain.models import MetricContext, MetricResult


class Burstiness(Metric):
    """
    Measures work distribution across active weeks to detect big bursts vs steady contributions.
    """

    @property
    def slug(self) -> str:
        return "burstiness"

    @property
    def name(self) -> str:
        return "Burstiness"

    @property
    def description(self) -> str:
        return "Burst ratio (max/avg activity per active week) for pacing/sustainability."

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        activity_dates = []
        for pr in prs:
            activity_dates.append(pr.created_at)
            if pr.merged_at:
                activity_dates.append(pr.merged_at)
        for c in commits:
            activity_dates.append(c.date)
        for r in reviews:
            activity_dates.append(r.submitted_at)

        # Reuse shared helper (DRY with Active Weeks)
        weekly_counts = get_weekly_activity_counts(activity_dates)
        counts = list(weekly_counts.values())
        active_weeks = len(counts)
        total_activities = sum(counts)
        avg_weekly = total_activities / active_weeks if active_weeks else 0.0
        max_weekly = max(counts) if counts else 0
        burst_ratio = max_weekly / avg_weekly if avg_weekly > 0 else 0.0

        summary = f"Burst ratio: {burst_ratio:.2f} (max/avg weekly activity). Lower = steadier."
        details: Dict[str, object] = {
            "burst_ratio": burst_ratio,
            "active_weeks": active_weeks,
            "total_activities": total_activities,
            "max_weekly": max_weekly,
            "avg_weekly": avg_weekly,
            "per_week": weekly_counts,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
