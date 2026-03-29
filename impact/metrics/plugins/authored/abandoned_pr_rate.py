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
        return "% open PRs stale (>=30d; measures abandoned/blocked; rating worsens w/ longer window)."

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["Lean"]

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
        stale_prs = [
            pr for pr in open_prs
            if end_date and (end_date - pr.created_at) >= threshold
        ]
        rate = (len(stale_prs) / len(open_prs) * 100) if open_prs else 0.0
        period_days = (end_date - context.start_date).days if (context.start_date and end_date) else 30

        # Age-weighted score: each stale PR contributes severity proportional
        # to how long it has been open (capped at 5x for very old PRs).
        # This rewards closing old PRs more than fresh ones.
        if stale_prs and end_date:
            severity_sum = sum(
                min((end_date - pr.created_at).days / 30.0, 5.0)
                for pr in stale_prs
            )
            weighted_score = (severity_sum / len(open_prs)) * 20  # scale to 0-100
        else:
            weighted_score = 0.0

        per_pr_details = [
            {"number": pr.number, "age_days": (end_date - pr.created_at).days if end_date else 0}
            for pr in open_prs
        ]
        summary = f"Abandoned PR rate: {rate:.1f}% stale ({len(stale_prs)}/{len(open_prs)} open; weighted score {weighted_score:.1f})."
        details = {
            "abandoned_rate": rate,
            "stale_count": len(stale_prs),
            "open_pr_count": len(open_prs),
            "period_days": period_days,
            "weighted_score": round(weighted_score, 1),
            "per_pr": per_pr_details,
        }
        # Combined period+count guard
        if not open_prs or (period_days < 14 and len(open_prs) < 3):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
