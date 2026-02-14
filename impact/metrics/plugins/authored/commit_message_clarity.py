from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_conventional_commit


class ConventionalCommitRate(Metric):
    @property
    def slug(self) -> str:
        return "conventional_commit_rate"

    @property
    def name(self) -> str:
        return "Conventional Commit Rate"

    @property
    def description(self) -> str:
        return "% commits following conventional commit format (type: desc)."

    @property
    def category(self) -> str:
        return "code_quality_size"

    def run(self, context: MetricContext) -> MetricResult:
        all_commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # Filter out merge commits — they penalize merge-commit workflows unfairly
        commits = [c for c in all_commits if not c.message.strip().lower().startswith("merge ")]
        conventional = sum(1 for c in commits if is_conventional_commit(c.message))
        total = len(commits)
        rate = (conventional / total * 100) if total else 0.0
        summary = f"Conventional commit rate: {rate:.1f}% ({conventional}/{total} commits)."
        details: dict[str, object] = {
            "conventional_commit_rate": rate,
            "conventional_count": conventional,
            "total_commits": total,
            "commit_messages_sample": [c.message[:100] for c in commits[:5]],  # for verify
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if total == 0 or (period_days < 14 and total < 5):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
