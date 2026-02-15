from impact.domain.models import MetricContext, MetricResult, PullRequest
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, get_pr_size_category


class TrivialContributionRate(Metric):
    """
    Measures the daily rate of trivial contributions (pull requests with < 10 lines changed).

    This metric identifies the frequency of low-impact PRs per day, accounting for the time period,
    to detect potential gaming behavior. A high daily rate suggests issues with contribution quality.
    The rating normalizes by period duration for fair comparison across different time windows.

    Details returned:
        - total_pr_count: Total number of PRs analyzed
        - trivial_pr_count: Number of trivial PRs (< 10 changes)
        - trivial_rate: Percentage of PRs that are trivial
        - period_days: Duration of the analysis period in days
        - trivial_prs_per_day: Average trivial PRs per day
        - trivial_pr_numbers: List of PR numbers for trivial PRs
    """

    @property
    def slug(self) -> str:
        return "trivial_contribution_rate"

    @property
    def name(self) -> str:
        return "Trivial Contribution Rate"

    @property
    def description(self) -> str:
        return "Daily rate of tiny PRs (<10 lines) to detect low-impact/gaming behavior."

    @property
    def category(self) -> str:
        return "process_discipline"

    def run(self, context: MetricContext) -> MetricResult:
        # Filter: exclude drafts for quality/trivial rate (incomplete); include closed.
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        trivial_prs: list[PullRequest] = []
        for pr in prs:
            changes = pr.additions + pr.deletions
            if get_pr_size_category(changes) == "trivial":
                trivial_prs.append(pr)

        total_pr_count = len(prs)
        trivial_pr_count = len(trivial_prs)
        trivial_rate = (trivial_pr_count / total_pr_count * 100) if total_pr_count > 0 else 0.0

        # Compute period duration in days
        if context.start_date and context.end_date:
            period_duration = context.end_date - context.start_date
            days = period_duration.total_seconds() / (24 * 3600)
            days = max(days, 1)  # Avoid division by zero, minimum 1 day
        else:
            days = 30  # Default to 30 days if no period specified

        trivial_prs_per_day = trivial_pr_count / days

        summary = f"{trivial_prs_per_day:.2f} trivial PRs per day"
        details: dict[str, object] = {
            "total_pr_count": total_pr_count,
            "trivial_pr_count": trivial_pr_count,
            "trivial_rate": trivial_rate,
            "period_days": days,
            "trivial_prs_per_day": trivial_prs_per_day,
            "trivial_pr_numbers": [pr.number for pr in trivial_prs],
        }
        # Mark as no_data when there are no PRs to evaluate or the sample is
        # too small (short period with few PRs), so the threshold system does
        # not reward 0.0 as "excellent".
        if total_pr_count == 0 or (days < 14 and total_pr_count < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
