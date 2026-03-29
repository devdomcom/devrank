"""
AI Phantom Ownership Metric

Identifies code primarily touched by AI with low human review depth.

"Phantom ownership" occurs when a developer is credited as author of code that
was actually AI-generated, and no human reviewer engaged deeply enough to
understand the code.  The result: code lives in production that no human
truly comprehends, creating an invisible maintenance and reliability risk.

This is a P0 Knowledge Ownership metric in the AI era (DevRank framework).

Signals used:
  - AI contribution: reuses ``_analyze_pr_for_ai`` from ai_assisted_pr_rate
    plus ``copilot_work_started/finished`` timeline events.
  - Human review depth: counts *only* human (non-bot) reviews and inline
    comments per file, using the centralized ``is_bot_user()`` utility.
  - File criticality: excludes generated files, weights by change volume.
"""

from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_bot_user, is_generated_file


# ---------------------------------------------------------------------------
# AI detection helpers
# ---------------------------------------------------------------------------

_COPILOT_TIMELINE_EVENTS = frozenset({
    "copilot_work_started",
    "copilot_work_finished",
})


def _has_copilot_timeline(ledger, pr_number: int) -> bool:
    """Check if a PR has copilot_work_started/finished timeline events."""
    for evt in ledger.get_timeline_for_pr(pr_number):
        if evt.event in _COPILOT_TIMELINE_EVENTS:
            return True
    return False


def _is_ai_assisted_pr(pr, ledger) -> tuple[bool, float]:
    """Detect AI assistance and return (is_ai, confidence).

    Combines text/heuristic detection from ai_assisted_pr_rate with
    copilot timeline events (gold-standard GitHub-native signal).
    """
    from impact.metrics.plugins.authored.ai_assisted_pr_rate import (
        _analyze_pr_for_ai,
        _calculate_confidence_score,
    )

    is_ai, _tool, evidence = _analyze_pr_for_ai(pr, ledger)
    confidence = _calculate_confidence_score(evidence) if evidence else 0.0

    # Boost: copilot timeline events are a gold-standard signal
    if _has_copilot_timeline(ledger, pr.number):
        if not is_ai:
            is_ai = True
        confidence = max(confidence, 70.0)  # minimum 70 when timeline present

    return is_ai, confidence


# ---------------------------------------------------------------------------
# Human review depth scoring
# ---------------------------------------------------------------------------

def _compute_human_review_depth(ledger, pr_number: int, author_login: str) -> dict:
    """Compute human review depth for a PR, strictly excluding bots.

    Returns a dict with:
      - human_reviews:          count of non-bot, non-author reviews
      - human_approvals:        subset that are APPROVED
      - human_changes_requested: subset that are CHANGES_REQUESTED
      - human_inline_comments:  count of inline review comments from humans
      - human_inline_files:     set of file paths with human inline comments
      - bot_reviews:            count of bot reviews (informational)
      - depth_score:            weighted composite (higher = deeper review)
    """
    human_reviews = 0
    human_approvals = 0
    human_changes_requested = 0
    bot_reviews = 0

    for rev in ledger.get_reviews_for_pr(pr_number):
        if rev.user.login == author_login:
            continue
        if is_bot_user(rev.user):
            bot_reviews += 1
            continue
        human_reviews += 1
        state_val = rev.state.value if hasattr(rev.state, "value") else str(rev.state)
        if state_val == "approved":
            human_approvals += 1
        elif state_val == "changes_requested":
            human_changes_requested += 1

    human_inline_comments = 0
    human_inline_files: set[str] = set()

    for c in ledger.get_comments_for_pr(pr_number):
        if c.user.login == author_login:
            continue
        if is_bot_user(c.user):
            continue
        if getattr(c, "path", None):
            human_inline_comments += 1
            human_inline_files.add(c.path)

    # Weighted depth score: approvals=5, change_requests=4, inline=3, general=1
    depth_score = (
        human_approvals * 5
        + human_changes_requested * 4
        + human_inline_comments * 3
        + human_reviews
    )

    return {
        "human_reviews": human_reviews,
        "human_approvals": human_approvals,
        "human_changes_requested": human_changes_requested,
        "human_inline_comments": human_inline_comments,
        "human_inline_files": human_inline_files,
        "bot_reviews": bot_reviews,
        "depth_score": depth_score,
    }


# ---------------------------------------------------------------------------
# Per-file phantom ownership classification
# ---------------------------------------------------------------------------

# A file is "phantom-owned" when AI confidence >= this AND human depth <= this
_AI_CONFIDENCE_THRESHOLD = 50.0
_LOW_DEPTH_THRESHOLD = 0  # zero human inline comments on the file


def _classify_files(
    files,
    ai_confidence: float,
    human_inline_files: set[str],
) -> tuple[list[dict], list[dict]]:
    """Split files into phantom-owned vs human-reviewed.

    A file is phantom-owned when:
      - The PR is AI-assisted with confidence >= _AI_CONFIDENCE_THRESHOLD
      - AND the specific file received zero human inline review comments
    """
    phantom = []
    reviewed = []

    for f in files:
        filename = f.filename
        if is_generated_file(filename, getattr(f, "patch", None)):
            continue

        file_info = {
            "file": filename,
            "additions": f.additions,
            "deletions": f.deletions,
            "changes": f.changes,
        }

        is_phantom = (
            ai_confidence >= _AI_CONFIDENCE_THRESHOLD
            and filename not in human_inline_files
        )

        if is_phantom:
            phantom.append(file_info)
        else:
            reviewed.append(file_info)

    return phantom, reviewed


# ---------------------------------------------------------------------------
# Metric class
# ---------------------------------------------------------------------------

class AIPhantomOwnership(Metric):
    """
    AI Phantom Ownership: code primarily touched by AI with low human review depth.

    Identifies files modified in AI-assisted PRs that received no meaningful
    human review (only bot reviews, or no inline comments on the specific file).
    These files represent a knowledge risk: the credited author may not truly
    understand code that AI generated.

    Output:
      - phantom_rate: % of AI-touched files with no human inline review
      - phantom_file_count / total_ai_files: absolute counts
      - per_pr: per-PR breakdown with AI confidence + human review depth
      - phantom_files: list of at-risk files (top 20)
    """

    @property
    def slug(self) -> str:
        return "ai_phantom_ownership"

    @property
    def name(self) -> str:
        return "AI Phantom Ownership"

    @property
    def description(self) -> str:
        return (
            "Code primarily touched by AI with low human review depth. "
            "High phantom ownership means code in production that no human "
            "truly understands — a knowledge and maintenance risk."
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

        # Get PRs for the user
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        total_phantom_files = 0
        total_ai_files = 0
        total_phantom_lines = 0
        total_ai_lines = 0
        phantom_file_list: list[dict] = []
        per_pr: list[dict] = []

        for pr in prs:
            is_ai, confidence = _is_ai_assisted_pr(pr, context.ledger)
            if not is_ai:
                continue

            # Compute human review depth
            depth = _compute_human_review_depth(
                context.ledger, pr.number, context.user_login,
            )

            # Get files for this PR
            files = context.ledger.get_files_for_pr(pr.number)
            if not files:
                continue

            phantom, reviewed = _classify_files(
                files, confidence, depth["human_inline_files"],
            )

            pr_phantom_count = len(phantom)
            pr_total = len(phantom) + len(reviewed)
            total_phantom_files += pr_phantom_count
            total_ai_files += pr_total

            # Line counts
            pr_phantom_lines = sum(f["additions"] + f["deletions"] for f in phantom)
            pr_ai_lines = pr_phantom_lines + sum(
                f["additions"] + f["deletions"] for f in reviewed
            )
            total_phantom_lines += pr_phantom_lines
            total_ai_lines += pr_ai_lines

            phantom_file_list.extend(
                {**f, "pr_number": pr.number, "ai_confidence": round(confidence, 1)}
                for f in phantom
            )

            per_pr.append({
                "number": pr.number,
                "title": pr.title[:100],
                "ai_confidence": round(confidence, 1),
                "human_reviews": depth["human_reviews"],
                "human_approvals": depth["human_approvals"],
                "human_inline_comments": depth["human_inline_comments"],
                "bot_reviews": depth["bot_reviews"],
                "depth_score": depth["depth_score"],
                "phantom_files": pr_phantom_count,
                "total_files": pr_total,
            })

        # Aggregate
        phantom_rate = (
            (total_phantom_files / total_ai_files * 100) if total_ai_files else 0.0
        )
        phantom_line_rate = (
            (total_phantom_lines / total_ai_lines * 100) if total_ai_lines else 0.0
        )

        # Sort phantom files by change volume (biggest risk first)
        phantom_file_list.sort(
            key=lambda f: f["additions"] + f["deletions"], reverse=True,
        )

        ai_pr_count = len(per_pr)
        total_pr_count = len(prs)

        # Build summary
        if total_pr_count == 0:
            summary = "No PRs found in the analysis period."
        elif ai_pr_count == 0:
            summary = "No AI-assisted PRs detected in the analysis period."
        else:
            summary = (
                f"{phantom_rate:.1f}% phantom ownership "
                f"({total_phantom_files}/{total_ai_files} AI-touched files lack human review). "
                f"{ai_pr_count} AI-assisted PR(s) analyzed."
            )

        details: dict[str, object] = {
            "phantom_rate": round(phantom_rate, 1),
            "phantom_file_count": total_phantom_files,
            "total_ai_files": total_ai_files,
            "phantom_line_rate": round(phantom_line_rate, 1),
            "phantom_lines": total_phantom_lines,
            "total_ai_lines": total_ai_lines,
            "ai_pr_count": ai_pr_count,
            "total_pr_count": total_pr_count,
            "period_days": round(period_days, 1),
            "phantom_files": phantom_file_list[:20],
            "per_pr": per_pr,
        }

        # No-data guard
        if total_pr_count == 0 or ai_pr_count == 0:
            details["no_data"] = True
        elif period_days < 14 and total_pr_count < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
