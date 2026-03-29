from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import get_weekly_activity_counts


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

    @property
    def category(self) -> str:
        return "contextual"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

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
        for c in commits:
            activity_dates.append(c.date)
        for r in reviews:
            activity_dates.append(r.submitted_at)

        # Reuse shared helper (DRY with Active Weeks)
        weekly_counts = get_weekly_activity_counts(activity_dates)
        counts = list(weekly_counts.values())
        active_weeks = len(counts)
        total_activities = sum(counts)

        # Calculate total weeks in analysis period
        if context.start_date and context.end_date:
            total_period_days = (context.end_date - context.start_date).total_seconds() / 86400
            total_weeks = max(1, int(total_period_days / 7))
        else:
            total_weeks = active_weeks  # fallback

        # Use total_weeks (not active_weeks) so inactive weeks inflate the
        # burst ratio as expected -- a developer who works one week and takes
        # three off should NOT appear perfectly steady.
        avg_weekly = total_activities / total_weeks if total_weeks > 0 else 0.0
        max_weekly = max(counts) if counts else 0
        burst_ratio = max_weekly / avg_weekly if avg_weekly > 0 else 0.0

        summary = f"Burst ratio: {burst_ratio:.2f} (max/avg weekly activity). Lower = steadier."
        details: dict[str, object] = {
            "burst_ratio": burst_ratio,
            "active_weeks": active_weeks,
            "total_weeks": total_weeks,
            "total_activities": total_activities,
            "max_weekly": max_weekly,
            "avg_weekly": avg_weekly,
            "per_week": weekly_counts,
        }
        if total_activities == 0:
            details["no_data"] = True

        if total_weeks < 4:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
