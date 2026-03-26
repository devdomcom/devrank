"""
AI Code Quality Metric

Compares rework rate and review quality between AI-assisted PRs and human PRs.
AI-generated code that requires more iterations to merge indicates lower quality.
"""

from impact.domain.models import MetricContext, MetricResult, ReviewState
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_change_request


def _is_ai_assisted_pr(pr, ledger) -> bool:
    """Check if a PR was created with AI assistance.
    
    Reuses logic from ai_assisted_pr_rate: checks PR title, body, and commits
    for AI tool signatures (Copilot, Cursor, Claude Code, etc.).
    """
    from impact.metrics.plugins.authored.ai_assisted_pr_rate import _analyze_pr_for_ai
    is_ai, _, _ = _analyze_pr_for_ai(pr, ledger)
    return is_ai


def _compute_review_iterations(pr, ledger) -> int:
    """Compute number of change-request cycles for a PR.
    
    Groups changes_requested reviews within 4 hours as 1 cycle.
    """
    reviews = ledger.get_reviews_for_pr(pr.number)
    changes_requested = [r for r in reviews if is_change_request(r, ledger)]
    
    if not changes_requested:
        return 0
    
    cr_times = sorted([r.submitted_at for r in changes_requested])
    cycles = 0
    last_cycle_time = None
    for t in cr_times:
        if last_cycle_time is None or (t - last_cycle_time).total_seconds() > 4 * 3600:
            cycles += 1
            last_cycle_time = t
    return cycles


def _compute_pr_rework_indicator(pr, ledger) -> dict:
    """Compute rework indicators for a PR.
    
    Returns dict with:
    - review_iterations: number of change-request cycles
    - files_changed: number of files
    - additions: lines added
    - deletions: lines deleted
    """
    reviews = ledger.get_reviews_for_pr(pr.number)
    changes_requested = [r for r in reviews if is_change_request(r, ledger)]
    
    # Group CRs by time proximity (4h window = 1 cycle)
    cr_times = sorted([r.submitted_at for r in changes_requested])
    cycles = 0
    last_cycle_time = None
    for t in cr_times:
        if last_cycle_time is None or (t - last_cycle_time).total_seconds() > 4 * 3600:
            cycles += 1
            last_cycle_time = t
    
    files = ledger.get_files_for_pr(pr.number)
    file_count = len(files)
    
    return {
        "review_iterations": cycles,
        "files_changed": file_count,
        "additions": pr.additions or 0,
        "deletions": pr.deletions or 0,
    }


class AICodeQuality(Metric):
    """
    AI Code Quality: Rework comparison between AI-assisted and human PRs.
    
    Measures whether AI-generated code requires more review iterations
    and rework than human-authored code. Higher review iterations on AI PRs
    suggests lower quality / less reliable AI output.
    
    Key insight: AI code that passes review on first attempt (0 iterations)
    is higher quality than AI code requiring multiple change-request cycles.
    """

    @property
    def slug(self) -> str:
        return "ai_code_quality"

    @property
    def name(self) -> str:
        return "AI Code Quality"

    @property
    def description(self) -> str:
        return "Rework rate comparison: AI-assisted PRs vs human PRs (review iterations)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        # Get all PRs for the user
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        ai_prs = []
        human_prs = []

        for pr in prs:
            is_ai = _is_ai_assisted_pr(pr, context.ledger)
            rework = _compute_pr_rework_indicator(pr, context.ledger)
            
            pr_info = {
                "number": pr.number,
                "title": pr.title[:100],
                "is_ai_assisted": is_ai,
                "review_iterations": rework["review_iterations"],
                "files_changed": rework["files_changed"],
                "additions": rework["additions"],
                "deletions": rework["deletions"],
            }
            
            if is_ai:
                ai_prs.append(pr_info)
            else:
                human_prs.append(pr_info)

        # Compute aggregates
        ai_iterations = [p["review_iterations"] for p in ai_prs]
        human_iterations = [p["review_iterations"] for p in human_prs]

        ai_avg_iter = sum(ai_iterations) / len(ai_iterations) if ai_iterations else 0.0
        human_avg_iter = sum(human_iterations) / len(human_iterations) if human_iterations else 0.0

        # Quality ratio: >1 means AI needs more rework (lower quality)
        quality_ratio = ai_avg_iter / human_avg_iter if human_avg_iter > 0 else (ai_avg_iter if ai_avg_iter > 0 else 1.0)

        # First-pass rate: % of PRs with 0 iterations
        ai_first_pass = sum(1 for i in ai_iterations if i == 0) / len(ai_iterations) * 100 if ai_iterations else 0.0
        human_first_pass = sum(1 for i in human_iterations if i == 0) / len(human_iterations) * 100 if human_iterations else 0.0

        # Build summary
        if not prs:
            summary = "No PRs found in the analysis period."
        else:
            summary = (
                f"AI PRs: {len(ai_prs)} (avg {ai_avg_iter:.1f} iterations, {ai_first_pass:.0f}% first-pass). "
                f"Human PRs: {len(human_prs)} (avg {human_avg_iter:.1f} iterations, {human_first_pass:.0f}% first-pass). "
                f"Quality ratio: {quality_ratio:.2f}"
            )
            if quality_ratio > 1.5:
                summary += " (AI PRs require significantly more rework)"
            elif quality_ratio < 0.7:
                summary += " (AI PRs have better quality than human)"

        details: dict[str, object] = {
            "ai_pr_count": len(ai_prs),
            "human_pr_count": len(human_prs),
            "total_pr_count": len(prs),
            "period_days": round(period_days, 1),
            "ai_avg_review_iterations": round(ai_avg_iter, 2),
            "human_avg_review_iterations": round(human_avg_iter, 2),
            "ai_first_pass_rate": round(ai_first_pass, 1),
            "human_first_pass_rate": round(human_first_pass, 1),
            "quality_ratio": round(quality_ratio, 2),
            "ai_pr_numbers": [p["number"] for p in ai_prs],
            "human_pr_numbers": [p["number"] for p in human_prs],
            "per_pr": ai_prs + human_prs,
        }

        # No-data guard
        if len(prs) == 0 or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
