from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import detect_module_boundary, filter_prs_for_contribution


class ModuleAreaBreadth(Metric):
    """
    Measures the breadth of codebase understanding by counting distinct areas touched per PR.

    This metric analyzes the average number of unique codebase areas (directories) modified
    per PR in the period. Areas are determined by truncating file paths to N directory levels
    for stability. Higher average indicates wider understanding across contributions.

    Areas are normalized from file paths (e.g., "frontend/components/Button.js" -> "frontend").
    The rating normalizes by PR count for fair comparison across different contribution volumes.

    Details returned:
        - total_pr_count: Total number of PRs analyzed
        - total_files_touched: Total unique files modified
        - distinct_areas_count: Number of unique areas touched
        - distinct_areas: List of unique area names
        - areas_per_pr: Average areas touched per PR (used for rating)
        - truncation_levels: Number of directory levels used for normalization
    """

    @property
    def slug(self) -> str:
        return "module_area_breadth"

    @property
    def name(self) -> str:
        return "Module / Area Breadth"

    @property
    def description(self) -> str:
        return "Avg distinct codebase areas touched per PR (measures understanding breadth)."

    @property
    def category(self) -> str:
        return "process_discipline"

    def run(self, context: MetricContext) -> MetricResult:
        # Filter: exclude drafts for breadth/quality (incomplete contributions).
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        all_areas: set[str] = set()
        total_files = 0
        sum_pr_areas = 0

        for pr in prs:
            pr_areas: set[str] = set()
            files = context.ledger.get_files_for_pr(pr.number)
            for file in files:
                area = detect_module_boundary(file.filename)
                pr_areas.add(area)
            all_areas.update(pr_areas)
            sum_pr_areas += len(pr_areas)
            total_files += len(files)

        distinct_areas_count = len(all_areas)
        total_pr_count = len(prs)
        areas_per_pr = sum_pr_areas / total_pr_count if total_pr_count > 0 else 0.0

        summary = f"Touched {distinct_areas_count} distinct modules"
        details: dict[str, object] = {
            "total_pr_count": total_pr_count,
            "total_files_touched": total_files,
            "distinct_areas_count": distinct_areas_count,
            "distinct_areas": sorted(list(all_areas)),
            "areas_per_pr": areas_per_pr,
            "detection_method": "manifest_aware",
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if not prs or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
