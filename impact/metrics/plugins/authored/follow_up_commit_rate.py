from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class FollowUpCommitRate(Metric):
    @property
    def slug(self) -> str:
        return "follow_up_commit_rate"

    @property
    def name(self) -> str:
        return "Follow-Up Commit Rate"

    @property
    def description(self) -> str:
        return "% PRs with additional commits after initial push (self-initiated iteration)."

    @property
    def category(self) -> str:
        return "pr_hygiene_process"

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        follow_up_count = 0
        per_pr: list[dict] = []

        for pr in prs:
            commits = context.ledger.get_commits_for_pr(pr.number)
            # Only count commits by the PR author
            author_commits = sorted(
                [c for c in commits if c.author.login == context.user_login],
                key=lambda c: c.date,
            )
            has_follow_up = len(author_commits) > 1
            if has_follow_up:
                follow_up_count += 1
            per_pr.append({
                "number": pr.number,
                "author_commit_count": len(author_commits),
                "has_follow_up": has_follow_up,
            })

        rate = (follow_up_count / len(prs) * 100) if prs else 0.0
        summary = (
            f"Follow-up commit rate: {rate:.1f}% "
            f"({follow_up_count}/{len(prs)} PRs had additional commits)."
        )
        details: dict[str, object] = {
            "follow_up_rate": rate,
            "follow_up_count": follow_up_count,
            "pr_count": len(prs),
            "per_pr": per_pr,
            "period_days": period_days,
        }
        if not prs or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
