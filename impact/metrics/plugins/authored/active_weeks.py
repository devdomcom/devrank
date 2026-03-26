from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import get_week_activity_details


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

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["SPACE"]

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
            if context.start_date and context.end_date:
                if context.start_date <= pr.created_at <= context.end_date:
                    activity_dates.append(pr.created_at)
            else:
                activity_dates.append(pr.created_at)
        for c in commits:
            if context.start_date and context.end_date:
                if context.start_date <= c.date <= context.end_date:
                    activity_dates.append(c.date)
            else:
                activity_dates.append(c.date)
        for r in reviews:
            if context.start_date and context.end_date:
                if context.start_date <= r.submitted_at <= context.end_date:
                    activity_dates.append(r.submitted_at)
            else:
                activity_dates.append(r.submitted_at)

        # Granular details (repurposed for gaps/absences)
        week_details = get_week_activity_details(
            activity_dates, context.start_date, context.end_date
        )

        summary = f"{week_details['active_count']} active weeks / {week_details['total_weeks']} total. Max gap: {week_details['max_gap_weeks']} weeks (disengagement marker)."
        details: dict[str, object] = {
            **week_details,
            "activity_sources": {
                "prs": len(prs),
                "commits": len(commits),
                "reviews": len(reviews),
            },
        }

        if not activity_dates:
            details["no_data"] = True

        if week_details["total_weeks"] < 4:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
