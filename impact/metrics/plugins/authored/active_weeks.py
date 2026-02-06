from typing import Dict

from impact.metrics.base import Metric
from impact.metrics.utils import get_week_activity_details
from impact.domain.models import MetricContext, MetricResult


class ActiveWeeks(Metric):
    """
    Surfaces granular active/inactive weeks and gaps to detect disengagement/absences.
    """

    @property
    def slug(self) -> str:
        return "active_weeks"

    @property
    def name(self) -> str:
        return "Active Weeks"

    @property
    def description(self) -> str:
        return "Granular active weeks, gaps, and ratio to detect disengagement."

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

        # Granular details (repurposed for gaps/absences)
        week_details = get_week_activity_details(
            activity_dates, context.start_date, context.end_date
        )

        summary = f"{week_details['active_count']} active weeks / {week_details['total_weeks']} total. Max gap: {week_details['max_gap_weeks']} weeks (disengagement marker)."
        details: Dict[str, object] = {
            **week_details,
            "activity_sources": {
                "prs": len(prs),
                "commits": len(commits),
                "reviews": len(reviews),
            },
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
