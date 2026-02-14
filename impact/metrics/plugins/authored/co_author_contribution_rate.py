import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution

CO_AUTHOR_PATTERN = re.compile(r'Co-authored-by:\s*(.+)', re.IGNORECASE)


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

    @property
    def category(self) -> str:
        return "scope_collaboration"

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        commits = context.ledger.get_commits_for_user(user, context.start_date, context.end_date)
        # Inbound collaboration: others helped the user on the user's own PRs.
        # A Co-authored-by trailer on a commit in the user's PR means someone
        # else contributed to that commit (e.g., pair programming, suggestions).
        inbound_co = 0
        inbound_total = 0
        per_pr_co = []
        for pr in prs:
            pr_commits = context.ledger.get_commits_for_pr(pr.number)
            total = len(pr_commits)
            # Count commits that have Co-authored-by trailers (others helped on user's PR)
            co = 0
            for c in pr_commits:
                co_authors = CO_AUTHOR_PATTERN.findall(c.message)
                if co_authors:
                    co += 1
            inbound_co += co
            inbound_total += total
            per_pr_co.append({"number": pr.number, "co_authors": co, "total_commits": total})
        inbound_rate = (inbound_co / inbound_total * 100) if inbound_total else 0.0
        # Outbound collaboration: user helped others on their PRs.
        # The user authored a commit on someone else's PR with a Co-authored-by
        # trailer, indicating collaborative work where the user contributed to
        # another engineer's PR.
        outbound = 0
        for c in commits:
            if c.pull_request_number:
                pr = context.ledger.get_pr(c.pull_request_number)
                if pr and pr.user.login != user:
                    # User authored commit on someone else's PR with co-author trailer
                    co_authors = CO_AUTHOR_PATTERN.findall(c.message)
                    if co_authors:
                        outbound += 1
        outbound_rate = (outbound / len(commits) * 100) if commits else 0.0
        # Period-balanced: collab events/week
        start = context.start_date or context.end_date or None
        end = context.end_date
        period_days = (end - start).days if start and end else 30
        weeks = max(1, period_days / 7.0)
        total_co_events = inbound_co + outbound
        collab_per_week = total_co_events / weeks
        # When there are zero events, report 0.0 (no artificial floor)
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
        if total_co_events == 0 or (period_days < 14 and total_co_events < 2):
            details["no_data"] = True
        summary = f"Combined collab rate: {inbound_rate:.1f}% in/{outbound_rate:.1f}% out; {collab_per_week:.1f} events/week (period: {period_days}d)."
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
