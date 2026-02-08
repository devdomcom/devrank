from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_revert_indicator


class RevertIntroductionRate(Metric):
    """Rate of reverts in authored commits (low = stable; see assumptions in docstring)."""

    @property
    def slug(self) -> str:
        return "revert_introduction_rate"

    @property
    def name(self) -> str:
        return "Revert Introduction Rate"

    @property
    def description(self) -> str:
        return "% reverts in commits (low rate = stability; message-based detection)."

    def run(self, context: MetricContext) -> MetricResult:
        commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Assumption: reverts detected via msg prefix/SHA (GitHub format from sample);
        # rate on commits (not PRs) as reverts are commit-level; PR-link via number if present.
        # High rate flags instability (per proposal best practice <5%).
        revert_commits = [c for c in commits if is_revert_indicator(c.message)]
        total_commits = len(commits)
        rate = (len(revert_commits) / total_commits * 100) if total_commits else 0.0

        summary = f"{rate:.1f}% revert rate ({len(revert_commits)}/{total_commits} commits)."
        details: dict[str, object] = {
            "total_commits": total_commits,
            "revert_count": len(revert_commits),
            "rate": rate,
            "revert_shas": [c.sha for c in revert_commits],
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
