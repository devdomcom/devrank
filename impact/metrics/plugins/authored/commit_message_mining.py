from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import classify_defect_commit


class CommitMessageMining(Metric):
    """Estimate defect distribution using regex-mined commit messages."""

    @property
    def slug(self) -> str:
        return "commit_message_mining"

    @property
    def name(self) -> str:
        return "Commit Message Mining"

    @property
    def description(self) -> str:
        return "Regex-based defect signals in commit messages to estimate defect distribution."

    @property
    def category(self) -> str:
        return "process_discipline"

    def run(self, context: MetricContext) -> MetricResult:
        all_commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        commits = [c for c in all_commits if not c.message.strip().lower().startswith("merge ")]

        defect_commits = [c for c in commits if classify_defect_commit(c.message)]
        total = len(commits)
        defect_count = len(defect_commits)
        defect_rate = (defect_count / total * 100) if total else 0.0

        summary = (
            f"Defect-related commits: {defect_rate:.1f}% "
            f"({defect_count}/{total} commits)."
        )

        details: dict[str, object] = {
            "defect_commit_rate": defect_rate,
            "defect_commit_count": defect_count,
            "total_commits": total,
            "defect_commit_samples": [c.message[:120] for c in defect_commits[:5]],
        }

        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )
        details["period_days"] = period_days

        if total == 0 or (period_days < 14 and total < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
