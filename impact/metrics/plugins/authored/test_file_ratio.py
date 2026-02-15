from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_test_file


class TestFileRatio(Metric):
    """Test changes proportion vs source (distinct from breadth; promotes discipline)."""

    @property
    def slug(self) -> str:
        return "test_file_ratio"

    @property
    def name(self) -> str:
        return "Test File Ratio"

    @property
    def description(self) -> str:
        return "% test file changes (target >=25% per quality benchmarks)."

    @property
    def category(self) -> str:
        return "code_quality"

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        # Filter for meaningful (exclude drafts; include closed/merged per assessment).
        # Reviewed: only quality metrics filter drafts; throughput/activity include all.
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        # Assumption: test files by path patterns (e.g. /test/, .test.); ratio on lines changed
        # (not count) for volume; % PRs with tests for breadth; aggregate across PRs; 0 if no files.
        # Distinct from module breadth. Sample shows ~60% PRs have tests overall.
        total_changes = 0
        test_changes = 0
        test_prs = 0
        test_pr_numbers = []
        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            pr_test = 0
            pr_total = 0
            for f in files:
                ch = f.additions + f.deletions
                pr_total += ch
                if is_test_file(f.filename):
                    pr_test += ch
            total_changes += pr_total
            test_changes += pr_test
            if pr_test > 0:
                test_prs += 1
                test_pr_numbers.append(pr.number)

        ratio = (test_changes / total_changes * 100) if total_changes else 0.0
        pr_test_ratio = (test_prs / len(prs) * 100) if prs else 0.0
        analyzed_pr_numbers = [pr.number for pr in all_prs]  # full for verify
        non_test_pr_numbers = [n for n in [p.number for p in prs] if n not in test_pr_numbers]

        summary = f"{ratio:.1f}% test changes ({test_changes}/{total_changes} lines; {pr_test_ratio:.1f}% PRs had tests)."
        details: dict[str, object] = {
            "total_prs": len(prs),
            "test_prs": test_prs,
            "pr_test_ratio": pr_test_ratio,
            "total_changes": total_changes,
            "test_changes": test_changes,
            "ratio": ratio,
            "test_pr_numbers": test_pr_numbers,
            "non_test_pr_numbers": non_test_pr_numbers,
            "analyzed_pr_numbers": analyzed_pr_numbers,
            "period_days": period_days,
        }
        if len(prs) == 0 or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True
        elif total_changes == 0 and len(prs) > 0:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
