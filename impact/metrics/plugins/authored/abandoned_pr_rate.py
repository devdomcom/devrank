from datetime import timedelta

from impact.domain.models import MetricContext, MetricResult, PullRequestState
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class AbandonedPRRate(Metric):
    @property
    def slug(self) -> str:
        return "abandoned_pr_rate"

    @property
    def name(self) -> str:
        return "Abandoned PR Rate"

    @property
    def description(self) -> str:
        return "% open PRs stale (>30d; measures abandoned/blocked; rating worsens w/ longer window)."

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(
            user, context.start_date, context.end_date
        )
        # Open only (exclude drafts for quality)
        open_prs = [
            pr for pr in filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
            if pr.state == PullRequestState.OPEN
        ]
        # Stale: open >30d at context end (deterministic fallback: max created +30d or start+30d)
        end_date = context.end_date
        if not end_date:
            dates = [pr.created_at for pr in open_prs]
            if dates:
                end_date = max(dates) + timedelta(days=30)
            elif context.start_date:
                end_date = context.start_date + timedelta(days=30)
            else:
                end_date = None
        threshold = timedelta(days=30)
        stale_count = sum(
            1 for pr in open_prs
            if end_date and (end_date - pr.created_at) > threshold
        )
        rate = (stale_count / len(open_prs) * 100) if open_prs else 0.0
        # Rating worsens w/ window: scale by days/30
        period_days = (end_date - context.start_date).days if context.start_date and end_date else 30
        weighted = rate * (period_days / 30.0)
        summary = f"Abandoned PR rate: {rate:.1f}% stale ({stale_count}/{len(open_prs)} open; weighted {weighted:.1f} for window)."
        details = {
            "abandoned_rate": rate,
            "stale_count": stale_count,
            "open_pr_count": len(open_prs),
            "period_days": period_days,
            "weighted_score": weighted,
            "per_pr": [{"number": pr.number, "age_days": (end_date - pr.created_at).days if end_date else 0} for pr in open_prs],
        }
        if not open_prs:
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
