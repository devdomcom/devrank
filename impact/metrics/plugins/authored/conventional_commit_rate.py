from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_conventional_commit, is_structured_commit, is_merge_commit


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
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        all_commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # Filter out merge commits via parent count (language-neutral)
        commits = [c for c in all_commits if not is_merge_commit(c)]
        conventional = sum(1 for c in commits if is_conventional_commit(c.message))
        structured = sum(1 for c in commits if is_structured_commit(c.message))
        total = len(commits)
        rate = (conventional / total * 100) if total else 0.0
        structured_rate = (structured / total * 100) if total else 0.0
        summary = f"Conventional commit rate: {rate:.1f}% ({conventional}/{total} commits)."
        details: dict[str, object] = {
            "conventional_commit_rate": rate,
            "conventional_count": conventional,
            # Broad structured rate: any word(scope?): prefix (non-English types credited)
            "structured_rate": structured_rate,
            "structured_count": structured,
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
