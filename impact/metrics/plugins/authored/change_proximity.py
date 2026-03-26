from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_change_proximity, filter_prs_for_contribution


class ChangeProximity(Metric):
    """Measures how concentrated or scattered changes are within files.

    High proximity score = scattered changes across files = risky.
    Low proximity score = concentrated changes in specific areas = safer.

    The metric sums the distances between consecutive changed lines in each file.
    Changes that are close together (low proximity) suggest focused, cohesive work.
    Changes that are scattered (high proximity) suggest risky, unfocused changes.

    Example:
    - Lines 10, 11, 12 changed → proximity = 1 + 1 = 2 (concentrated)
    - Lines 10, 50, 100 changed → proximity = 40 + 50 = 90 (scattered)
    """

    @property
    def slug(self) -> str:
        return "change_proximity"

    @property
    def name(self) -> str:
        return "Change Proximity"

    @property
    def description(self) -> str:
        return "Measures concentration of changes within files (low=cohesive, high=scattered/risky)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # Exclude drafts; include both merged and closed for quality assessment
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=False)

        details = compute_change_proximity(
            context.ledger, filtered_prs,
            start_date=context.start_date, end_date=context.end_date,
        )

        avg_proximity = details["avg_proximity_per_change"]
        total_proximity = details["total_proximity"]
        total_changes = details["total_changes"]
        pr_count = details["pr_count"]

        summary = (
            f"Average change proximity: {avg_proximity:.1f} lines "
            f"(total: {total_proximity} across {total_changes} changes in {pr_count} PRs)."
        )

        if details.get("no_data"):
            summary = "Insufficient data to compute change proximity."
        elif details["per_file"]:
            top_scattered = details["per_file"][0]
            summary += f" Most scattered: {top_scattered['filename']} (proximity: {top_scattered['proximity']})."

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
