from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_dependency_file


class DependencyChangeRate(Metric):
    @property
    def slug(self) -> str:
        return "dependency_change_rate"

    @property
    def name(self) -> str:
        return "Dependency Change Rate"

    @property
    def description(self) -> str:
        return "Frequency of dep file updates (package.json/requirements/etc.) for repo health."

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        dep_pr_count = 0
        total_dep_changes = 0
        per_pr = []
        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            dep_files = [f for f in files if is_dependency_file(f.filename)]
            dep_count = len(dep_files)
            if dep_count > 0:
                dep_pr_count += 1
            total_dep_changes += dep_count
            per_pr.append({"number": pr.number, "dep_changes": dep_count})
        total_prs = len(prs)
        dep_rate = (dep_pr_count / total_prs * 100) if total_prs else 0.0
        # Period norm: updates/month (low=neutral ok per role)
        start = context.start_date or context.end_date or None
        end = context.end_date
        period_days = (end - start).days if start and end else 30
        months = max(1, period_days / 30.0)
        dep_per_month = total_dep_changes / months
        summary = f"Dep change rate: {dep_rate:.1f}% of PRs; {dep_per_month:.1f}/month (period: {period_days}d)."
        details: dict[str, object] = {
            "dep_rate": dep_rate,
            "dep_per_month": dep_per_month,
            "dep_pr_count": dep_pr_count,
            "total_prs": total_prs,
            "total_dep_changes": total_dep_changes,
            "period_days": period_days,
            "per_pr": per_pr,
            "analyzed_pr_numbers": [p.number for p in all_prs],
        }
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
