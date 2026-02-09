from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class CoAuthorContributionRate(Metric):
    @property
    def slug(self) -> str:
        return "co_author_contribution_rate"

    @property
    def name(self) -> str:
        return "Co-Author Contribution Rate"

    @property
    def description(self) -> str:
        return "Inbound/outbound co-author commit % (collab on own/other PRs)."

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        commits = context.ledger.get_commits_for_user(user, context.start_date, context.end_date)
        inbound_co = 0
        inbound_total = 0
        per_pr_co = []
        for pr in prs:
            pr_commits = context.ledger.get_commits_for_pr(pr.number)
            total = len(pr_commits)
            co = sum(1 for c in pr_commits if c.author.login != pr.user.login)
            inbound_co += co
            inbound_total += total
            per_pr_co.append({"number": pr.number, "co_authors": co, "total_commits": total})
        inbound_rate = (inbound_co / inbound_total * 100) if inbound_total else 0.0
        outbound = sum(1 for c in commits if c.pull_request_number and (pr := context.ledger.get_pr(c.pull_request_number)) and pr.user.login != user)
        outbound_rate = (outbound / len(commits) * 100) if commits else 0.0
        # Period-balanced: collab events/week (short period tolerates low raw rates; long expects more)
        start = context.start_date or context.end_date or None
        end = context.end_date
        period_days = (end - start).days if start and end else 30
        weeks = max(1, period_days / 7.0)
        total_co_events = inbound_co + outbound
        collab_per_week = total_co_events / weeks
        # Balance: 0 events in <=30d not BAD (neutral floor); long periods penalize isolation
        if total_co_events == 0 and period_days <= 30:
            collab_per_week = 0.5
        summary = f"Combined collab rate: {inbound_rate:.1f}% in/{outbound_rate:.1f}% out; {collab_per_week:.1f} events/week (period: {period_days}d)."
        details: dict[str, object] = {
            "collab_per_week": collab_per_week,
            "inbound_rate": inbound_rate,
            "outbound_rate": outbound_rate,
            "inbound_co_commits": inbound_co,
            "outbound_commits": outbound,
            "total_co_events": total_co_events,
            "period_days": period_days,
            "per_pr_co": per_pr_co,
            "analyzed_pr_numbers": [p.number for p in all_prs],
        }
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
