import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_revert_indicator


class RevertIntroductionRate(Metric):
    """Rate of reverts attributed to the user's original code (low = stable)."""

    @property
    def slug(self) -> str:
        return "revert_introduction_rate"

    @property
    def name(self) -> str:
        return "Revert Introduction Rate"

    @property
    def description(self) -> str:
        return "% of user's commits that were reverted (attributed to original author, not reverter)."

    @property
    def category(self) -> str:
        return "risk_sustainability"

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        # Get all commits for the user (denominator: user's total commits)
        user_commits = context.ledger.get_commits_for_user(
            context.user_login, context.start_date, context.end_date
        )
        total_commits = len(user_commits)

        # Look through ALL commits in all PRs to find reverts of the user's code.
        # We scan all commits in the bundle to find revert commits,
        # then parse the original SHA to attribute to the correct author.
        all_commits = list(context.ledger.bundle.commits)

        revert_commits = [c for c in all_commits if is_revert_indicator(c.message)]

        # Build a lookup of all commit SHAs to their author
        sha_to_commit = {c.sha: c for c in all_commits}

        user_reverted = []
        for rc in revert_commits:
            # Parse original SHA from revert message
            # GitHub format: "Revert ... This reverts commit <sha>"
            match = re.search(r'reverts? commit ([a-f0-9]{7,40})', rc.message, re.IGNORECASE)
            if not match:
                continue
            original_sha = match.group(1)
            # Find the original commit (support prefix matching)
            original_commit = sha_to_commit.get(original_sha)
            if not original_commit:
                # Try prefix match; skip if ambiguous (>1 candidate)
                candidates = [c for c in all_commits if c.sha.startswith(original_sha)]
                if len(candidates) == 1:
                    original_commit = candidates[0]
                else:
                    original_commit = None  # ambiguous or not found
            if original_commit and original_commit.author.login == context.user_login:
                user_reverted.append(rc)

        rate = (len(user_reverted) / total_commits * 100) if total_commits else 0.0

        summary = f"{rate:.1f}% revert rate ({len(user_reverted)} reverts of {total_commits} commits by {context.user_login})."
        details: dict[str, object] = {
            "total_commits": total_commits,
            "revert_count": len(user_reverted),
            "rate": rate,
            "revert_shas": [c.sha for c in user_reverted],
            "period_days": period_days,
        }
        if total_commits == 0 or (period_days < 14 and total_commits < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
