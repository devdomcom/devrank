from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_conventional_commit


class CommitMessageClarity(Metric):
    @property
    def slug(self) -> str:
        return "commit_message_clarity"

    @property
    def name(self) -> str:
        return "Commit Message Clarity"

    @property
    def description(self) -> str:
        return "Adherence rate to conventional commit formats (industry best practices)."

    def run(self, context: MetricContext) -> MetricResult:
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        conventional = sum(1 for c in commits if is_conventional_commit(c.message))
        total = len(commits)
        clarity_rate = (conventional / total * 100) if total else 0.0
        summary = f"Commit clarity: {clarity_rate:.1f}% conventional ({conventional}/{total} commits)."
        details: dict[str, object] = {
            "clarity_rate": clarity_rate,
            "conventional_count": conventional,
            "total_commits": total,
            "commit_messages_sample": [c.message[:100] for c in commits[:5]],  # for verify
        }
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
