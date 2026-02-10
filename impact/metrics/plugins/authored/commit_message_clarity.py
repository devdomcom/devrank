from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_conventional_commit


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

    def run(self, context: MetricContext) -> MetricResult:
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
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
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
