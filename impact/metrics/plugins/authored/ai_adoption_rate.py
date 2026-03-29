"""
AI Adoption Rate Metric

Per-engineer metric: indicates whether the assessed engineer has adopted
AI coding tools (Copilot, Cursor, Claude Code, etc.).

Since metrics run per assessed engineer, this shows whether this specific
engineer has used AI tools in their PRs/commits, not a team-wide rate.

This metric cannot directly access license information from GitHub's API,
so it infers adoption by detecting AI tool signatures in commit messages
and PR descriptions (Co-authored-by, Generated-with:, etc.).

Uses DRY principles by reusing detection logic from ai_assisted_pr_rate.
"""

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class AIAdoptionRate(Metric):
    """
    AI Adoption Rate: per-engineer indicator of AI tool usage.

    Checks whether the assessed engineer has used AI coding tools
    (Copilot, Cursor, Claude Code, etc.) by detecting signatures in
    their PR titles, descriptions, and commit messages.

    DRY: Reuses _analyze_pr_for_ai from ai_assisted_pr_rate for detection.
    """

    @property
    def slug(self) -> str:
        return "ai_adoption_rate"

    @property
    def name(self) -> str:
        return "AI Adoption Rate"

    @property
    def description(self) -> str:
        return (
            "Whether this engineer has adopted AI coding tools (Copilot, Cursor, Claude Code, etc.). "
            "Detected via commit/PR signatures. Proxy for license usage since GitHub API "
            "does not expose billing data. Per-engineer metric."
        )

    @property
    def category(self) -> str:
        return "contextual"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        # Get PRs for the assessed engineer
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        # Import here to avoid circular dependency at module load
        from impact.metrics.plugins.authored.ai_assisted_pr_rate import _analyze_pr_for_ai

        # Check each PR for AI assistance
        ai_prs = []
        tools_used: set[str] = set()

        for pr in prs:
            is_ai, tool, evidence = _analyze_pr_for_ai(pr, context.ledger)
            if is_ai:
                ai_prs.append({
                    "number": pr.number,
                    "title": pr.title[:100],
                    "tool": tool,
                })
                if tool:
                    tools_used.add(tool)

        has_adopted = len(ai_prs) > 0

        # Build summary
        if len(prs) == 0:
            summary = "No PRs found in the analysis period."
            details = {
                "has_adopted_ai": False,
                "ai_pr_count": 0,
                "total_pr_count": 0,
                "tools_used": [],
                "period_days": round(period_days, 1),
                "no_data": True,
            }
        elif has_adopted:
            tools_str = ", ".join(sorted(tools_used)) if tools_used else "unknown"
            summary = (
                f"AI adopted via {tools_str}. "
                f"{len(ai_prs)}/{len(prs)} PRs show AI assistance."
            )
            details = {
                "has_adopted_ai": True,
                "ai_pr_count": len(ai_prs),
                "total_pr_count": len(prs),
                "tools_used": sorted(tools_used),
                "ai_prs": ai_prs[:10],  # Cap for payload size
                "period_days": round(period_days, 1),
            }
        else:
            summary = f"No AI adoption detected in {len(prs)} PRs."
            details = {
                "has_adopted_ai": False,
                "ai_pr_count": 0,
                "total_pr_count": len(prs),
                "tools_used": [],
                "period_days": round(period_days, 1),
            }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
