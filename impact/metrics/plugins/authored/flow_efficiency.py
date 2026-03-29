"""
Flow Efficiency (Kanban)

Measures the ratio of active work time to total lead time for merged PRs.
Active time = commit time spans (time between consecutive commits on the PR).
Wait time  = total lead time - active time (review wait, CI wait, merge delay).

Without CI data, this is an approximation: active_time = commit_span,
wait_time = review_wait + merge_delay. GitHub Actions API would improve
accuracy but is not required.
"""

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, percentile


class FlowEfficiency(Metric):
    """
    Flow Efficiency: active time / total lead time for merged PRs.

    For each merged PR:
      - total_lead_time = merged_at - created_at
      - active_time     = sum of commit inter-arrival gaps (capped at 4h each
                          to exclude overnight/weekend breaks)
      - flow_efficiency = active_time / total_lead_time

    High efficiency (>40%) indicates smooth flow with minimal wait.
    Low efficiency (<15%) suggests bottlenecks (review wait, CI queues).
    """

    @property
    def slug(self) -> str:
        return "flow_efficiency"

    @property
    def name(self) -> str:
        return "Flow Efficiency"

    @property
    def description(self) -> str:
        return "Active coding time / total lead time for merged PRs (Kanban flow health)."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    @property
    def frameworks(self) -> list[str]:
        return ["Kanban", "Lean"]

    def run(self, context: MetricContext) -> MetricResult:
        merged_prs = context.ledger.get_merged_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        efficiencies: list[float] = []
        per_pr: list[dict] = []

        for pr in merged_prs:
            if not pr.created_at or not pr.merged_at:
                continue

            total_hours = (pr.merged_at - pr.created_at).total_seconds() / 3600
            if total_hours <= 0:
                continue

            # Calculate active coding time from commit timestamps
            commits = context.ledger.get_commits_for_pr(pr.number)
            author_commits = [
                c for c in commits
                if c.author.login == pr.user.login
            ]
            author_commits.sort(key=lambda c: c.date)

            if not author_commits:
                per_pr.append({
                    "number": pr.number,
                    "total_hours": round(total_hours, 2),
                    "active_hours": 0,
                    "efficiency": 0.0,
                })
                efficiencies.append(0.0)
                continue

            # Active time = sum of inter-commit gaps, capped at 4h each
            # (gaps > 4h are likely breaks, not active work)
            MAX_GAP_HOURS = 4.0
            active_hours = 0.0
            for i in range(1, len(author_commits)):
                gap = (author_commits[i].date - author_commits[i - 1].date).total_seconds() / 3600
                active_hours += min(gap, MAX_GAP_HOURS)

            # Add minimum 1h for the first commit (writing time)
            active_hours += min(1.0, total_hours)

            # Cap active time to total lead time
            active_hours = min(active_hours, total_hours)
            efficiency = (active_hours / total_hours) * 100

            efficiencies.append(efficiency)
            per_pr.append({
                "number": pr.number,
                "total_hours": round(total_hours, 2),
                "active_hours": round(active_hours, 2),
                "efficiency": round(efficiency, 1),
                "commit_count": len(author_commits),
            })

        median_eff = percentile(efficiencies, 0.5) if efficiencies else 0.0
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        summary = (
            f"Flow efficiency: {median_eff:.1f}% median across {len(efficiencies)} merged PRs. "
            f"{'Healthy flow.' if median_eff > 30 else 'Significant wait time detected.'}"
        )

        details: dict[str, object] = {
            "median_efficiency": round(median_eff, 1),
            "pr_count": len(efficiencies),
            "per_pr": per_pr,
            "period_days": round(period_days, 1),
        }

        if not efficiencies or (period_days < 14 and len(efficiencies) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
