from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_bug_fix_indicator


class BugFixFocusRate(Metric):
    """Percentage of PRs/commits focused on bug fixes (moderate % ideal per industry balance)."""

    @property
    def slug(self) -> str:
        return "bug_fix_focus_rate"

    @property
    def name(self) -> str:
        return "Bug Fix Focus Rate"

    @property
    def description(self) -> str:
        return "% PRs/commits with bug-fix indicators (titles/bodies/messages), deduplicated."

    @property
    def category(self) -> str:
        return "contextual"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        # Filter: exclude drafts for focus/quality (incomplete).
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )

        bug_prs = [pr for pr in prs if is_bug_fix_indicator(pr.title) or is_bug_fix_indicator(pr.body or "")]
        bug_commits = [c for c in commits if is_bug_fix_indicator(c.message)]

        # Deduplicate: collect SHAs of commits belonging to bug-fix PRs
        bug_pr_numbers = {pr.number for pr in bug_prs}
        pr_commit_shas: set[str] = set()
        for pr in prs:
            for c in context.ledger.get_commits_for_pr(pr.number):
                pr_commit_shas.add(c.sha)

        # Only count bug commits that are NOT part of any PR (standalone)
        standalone_bug_commits = [
            c for c in bug_commits if c.sha not in pr_commit_shas
        ]

        # Also count standalone (non-PR) commits for totals
        standalone_commits = [c for c in commits if c.sha not in pr_commit_shas]

        total_prs = len(prs)
        total_standalone = len(standalone_commits)
        total_items = total_prs + total_standalone
        total_bug = len(bug_prs) + len(standalone_bug_commits)
        pr_rate = (len(bug_prs) / total_prs * 100) if total_prs else 0.0
        standalone_rate = (len(standalone_bug_commits) / total_standalone * 100) if total_standalone else 0.0
        overall_rate = (total_bug / total_items * 100) if total_items else 0.0

        summary = f"{overall_rate:.1f}% overall bug-fix focus (PRs: {pr_rate:.1f}% [{len(bug_prs)}/{total_prs}], standalone commits: {standalone_rate:.1f}% [{len(standalone_bug_commits)}/{total_standalone}])."
        analyzed_pr_numbers = [pr.number for pr in all_prs]  # full for verify/consistency
        non_bug_pr_numbers = [n for n in [p.number for p in prs] if n not in [p.number for p in bug_prs]]
        details: dict[str, object] = {
            "total_prs": total_prs,
            "bug_pr_count": len(bug_prs),
            "pr_rate": pr_rate,
            "total_standalone_commits": total_standalone,
            "bug_standalone_commit_count": len(standalone_bug_commits),
            "standalone_commit_rate": standalone_rate,
            "total_items": total_items,
            "total_bug": total_bug,
            "overall_rate": overall_rate,
            "bug_pr_numbers": [pr.number for pr in bug_prs],
            "non_bug_pr_numbers": non_bug_pr_numbers,
            "analyzed_pr_numbers": analyzed_pr_numbers,
            "period_days": period_days,
        }
        if total_items == 0 or (period_days < 14 and total_items < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
