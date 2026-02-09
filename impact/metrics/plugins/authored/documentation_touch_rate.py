from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_documentation_file


class DocumentationTouchRate(Metric):
    @property
    def slug(self) -> str:
        return "documentation_touch_rate"

    @property
    def name(self) -> str:
        return "Documentation Touch Rate"

    @property
    def description(self) -> str:
        return "% PRs touching docs (.md/README/etc.; neutral at worst unless role allows bad)."

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        doc_pr_count = 0
        total_doc_lines = 0
        per_pr = []
        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            doc_files = [f for f in files if is_documentation_file(f.filename)]
            doc_count = len(doc_files)
            doc_lines = sum(f.changes for f in doc_files)
            if doc_count > 0:
                doc_pr_count += 1
            total_doc_lines += doc_lines
            per_pr.append({"number": pr.number, "doc_changes": doc_count, "doc_lines": doc_lines})
        total_prs = len(prs)
        doc_rate = (doc_pr_count / total_prs * 100) if total_prs else 0.0
        # Period norm: lines/month (volume-aware; neutral/low ok per role)
        start = context.start_date or context.end_date or None
        end = context.end_date
        period_days = (end - start).days if start and end else 30
        months = max(1, period_days / 30.0)
        doc_per_month = total_doc_lines / months
        summary = f"Doc touch rate: {doc_rate:.1f}% of PRs; {doc_per_month:.1f} lines/month (period: {period_days}d)."
        details: dict[str, object] = {
            "doc_rate": doc_rate,
            "doc_per_month": doc_per_month,
            "doc_pr_count": doc_pr_count,
            "total_prs": total_prs,
            "total_doc_lines": total_doc_lines,
            "period_days": period_days,
            "per_pr": per_pr,
            "analyzed_pr_numbers": [p.number for p in all_prs],
        }
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
