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
        return "% PRs/commits with bug-fix indicators (titles/bodies/messages)."

    def run(self, context: MetricContext) -> MetricResult:
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

        total_prs = len(prs)
        total_commits = len(commits)
        total_items = total_prs + total_commits
        total_bug = len(bug_prs) + len(bug_commits)
        pr_rate = (len(bug_prs) / total_prs * 100) if total_prs else 0.0
        commit_rate = (len(bug_commits) / total_commits * 100) if total_commits else 0.0
        overall_rate = (total_bug / total_items * 100) if total_items else 0.0

        summary = f"{overall_rate:.1f}% overall bug-fix focus (PRs: {pr_rate:.1f}% [{len(bug_prs)}/{total_prs}], commits: {commit_rate:.1f}% [{len(bug_commits)}/{total_commits}])."
        analyzed_pr_numbers = [pr.number for pr in all_prs]  # full for verify/consistency
        non_bug_pr_numbers = [n for n in [p.number for p in prs] if n not in [p.number for p in bug_prs]]
        details: dict[str, object] = {
            "total_prs": total_prs,
            "bug_pr_count": len(bug_prs),
            "pr_rate": pr_rate,
            "total_commits": total_commits,
            "bug_commit_count": len(bug_commits),
            "commit_rate": commit_rate,
            "total_items": total_items,
            "total_bug": total_bug,
            "overall_rate": overall_rate,
            "bug_pr_numbers": [pr.number for pr in bug_prs],
            "non_bug_pr_numbers": non_bug_pr_numbers,
            "analyzed_pr_numbers": analyzed_pr_numbers,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
