from impact.domain.models import MetricContext, MetricResult, PullRequest
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, get_pr_size_category, percentile


class PRSizeDistribution(Metric):
    """
    Analyzes the size distribution of pull requests authored by a user.

    This metric examines additions, deletions, and total changes across PRs opened by the user,
    providing percentiles and categorization to highlight trivial work (very small PRs)
    or risky oversized PRs (very large changes). Only considers PRs where the user is the author.

    Categories:
        - Small: < 100 lines changed
        - Medium: 100-999 lines changed
        - Large: >= 1000 lines changed
        - Trivial: < 10 lines changed (subset of small)

    Details returned:
        - pr_count: Total number of PRs analyzed
        - additions_median: Median additions across PRs
        - deletions_median: Median deletions across PRs
        - changes_median: Median total changes
        - additions_p75: 75th percentile additions
        - deletions_p75: 75th percentile deletions
        - changes_p75: 75th percentile changes
        - small_pr_count: Number of small PRs
        - medium_pr_count: Number of medium PRs
        - large_pr_count: Number of large PRs
        - small_pr_percent: Percentage of small PRs
        - medium_pr_percent: Percentage of medium PRs
        - large_pr_percent: Percentage of large PRs
        - small_pr_numbers: List of PR numbers for small PRs
        - medium_pr_numbers: List of PR numbers for medium PRs
        - large_pr_numbers: List of PR numbers for large PRs (potential risk)
        - trivial_pr_numbers: List of PR numbers with < 10 changes (trivial)
    """

    @property
    def slug(self) -> str:
        return "pr_size_distribution"

    @property
    def name(self) -> str:
        return "PR Size Distribution"

    @property
    def description(self) -> str:
        return "Analyzes PR size distribution and % large changes (flags risk/monoliths)."

    @property
    def category(self) -> str:
        return "code_quality_size"

    def run(self, context: MetricContext) -> MetricResult:
        # Filter: exclude drafts for quality/size (incomplete); include closed (attempted).
        # Reviewed all: only quality metrics filter; throughput/activity include drafts.
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        additions: list[float] = []
        deletions: list[float] = []
        changes: list[float] = []
        small_prs: list[PullRequest] = []
        medium_prs: list[PullRequest] = []
        large_prs: list[PullRequest] = []
        trivial_prs: list[PullRequest] = []

        for pr in prs:
            add = pr.additions
            del_ = pr.deletions
            ch = add + del_ * 0.5  # Deletions carry half the risk weight of additions
            additions.append(add)
            deletions.append(del_)
            changes.append(ch)

            category = get_pr_size_category(ch)
            if category == "trivial":
                trivial_prs.append(pr)
            elif category == "small":
                small_prs.append(pr)
            elif category == "medium":
                medium_prs.append(pr)
            else:  # large
                large_prs.append(pr)

        pr_count = len(prs)
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        if pr_count == 0:
            summary = "No PRs found in the window."
            details: dict[str, object] = {
                "pr_count": 0,
                "period_days": period_days,
                "additions_median": 0.0,
                "deletions_median": 0.0,
                "changes_median": 0.0,
                "additions_p75": 0.0,
                "deletions_p75": 0.0,
                "changes_p75": 0.0,
                "small_pr_count": 0,
                "medium_pr_count": 0,
                "large_pr_count": 0,
                "small_pr_percent": 0.0,
                "medium_pr_percent": 0.0,
                "large_pr_percent": 0.0,
                "small_pr_numbers": [],
                "medium_pr_numbers": [],
                "large_pr_numbers": [],
                "trivial_pr_numbers": [],
                "no_data": True,
            }
        else:
            additions_median = percentile(additions, 0.5)
            deletions_median = percentile(deletions, 0.5)
            changes_median = percentile(changes, 0.5)
            additions_p75 = percentile(additions, 0.75)
            deletions_p75 = percentile(deletions, 0.75)
            changes_p75 = percentile(changes, 0.75)

            small_count = len(small_prs)
            medium_count = len(medium_prs)
            large_count = len(large_prs)
            trivial_count = len(trivial_prs)

            small_percent = (small_count / pr_count) * 100
            medium_percent = (medium_count / pr_count) * 100
            large_percent = (large_count / pr_count) * 100

            summary = f"{changes_median:.2f} median changes. Small: {small_percent:.1f}%, Medium: {medium_percent:.1f}%, Large: {large_percent:.1f}%"
            details: dict[str, object] = {
                "pr_count": pr_count,
                "period_days": period_days,
                "additions_median": additions_median,
                "deletions_median": deletions_median,
                "changes_median": changes_median,
                "additions_p75": additions_p75,
                "deletions_p75": deletions_p75,
                "changes_p75": changes_p75,
                "small_pr_count": small_count,
                "medium_pr_count": medium_count,
                "large_pr_count": large_count,
                "small_pr_percent": small_percent,
                "medium_pr_percent": medium_percent,
                "large_pr_percent": large_percent,
                "small_pr_numbers": [pr.number for pr in small_prs],
                "medium_pr_numbers": [pr.number for pr in medium_prs],
                "large_pr_numbers": [pr.number for pr in large_prs],
                "trivial_pr_numbers": [pr.number for pr in trivial_prs],
            }
            if period_days < 14 and pr_count < 3:
                details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
