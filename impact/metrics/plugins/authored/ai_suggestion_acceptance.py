"""
AI Suggestion Acceptance Metric

Computes the ratio of accepted vs dismissed AI suggestions.
AI suggestions are review comments with ```suggestion blocks from known AI bots.
Acceptance is heuristically determined by checking if the suggested code appears
in the file's final patch.

Detection method: Only matches review comment authors (user.login), not PR titles
or commit messages. See impact/config/ai_bots.yaml for the bot list.

Known limitations:
  - False negatives: Local AI tools (Cursor, Cody) that don't leave review
    comments cannot be detected from GitHub API data.
  - The [bot] suffix catch-all may match non-AI bots.
"""

import re
from pathlib import Path

import yaml

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


def _load_ai_bot_logins() -> frozenset:
    """Load AI bot logins from config, with hardcoded fallback."""
    config_path = Path(__file__).resolve().parents[3] / "config" / "ai_bots.yaml"
    try:
        with open(config_path) as f:
            data = yaml.safe_load(f)
        if data and "ai_review_bots" in data:
            return frozenset(login.lower() for login in data["ai_review_bots"])
    except (OSError, yaml.YAMLError):
        pass
    # Hardcoded fallback if config is missing
    return frozenset({
        "copilot-pull-request-reviewer[bot]",
        "codeant-ai-for-open-source[bot]",
        "bito-code-review[bot]",
        "korbit-ai[bot]",
        "codewhisperer[bot]",
        "tabnine[bot]",
    })


AI_BOT_LOGINS = _load_ai_bot_logins()


def _is_ai_bot(user_login: str) -> bool:
    """Check if a user login is a known AI bot."""
    if not user_login:
        return False
    return user_login.lower() in AI_BOT_LOGINS or user_login.endswith("[bot]")


def _extract_suggestion_blocks(body: str) -> list[str]:
    """Extract code blocks from ```suggestion ... ``` markers.
    
    Returns list of suggested code strings (trimmed).
    """
    if not body:
        return []
    
    # Match ```suggestion ... ``` (case-insensitive, handles various whitespace)
    pattern = r'```suggestion\s*\n(.*?)\n```'
    matches = re.findall(pattern, body, re.DOTALL | re.IGNORECASE)
    return [m.strip() for m in matches if m.strip()]


def _suggestion_accepted(suggested_code: str, file_patch: str | None) -> bool:
    """Heuristic: check if suggested code appears in file patch.
    
    Returns True if a meaningful snippet of the suggestion is found in the patch.
    Handles diff markers (+, -, @@) in patches.
    """
    if not suggested_code or not file_patch:
        return False
    
    # Take first few non-empty lines of suggestion for matching
    lines = [l.strip() for l in suggested_code.splitlines() if l.strip()]
    if not lines:
        return False
    
    # Use first 2-3 lines as the "signature" to match
    signature_lines = lines[:3]
    signature = "\n".join(signature_lines)
    
    # Normalize whitespace for matching
    sig_normalized = " ".join(signature.split())
    
    # Strip diff markers from patch lines (e.g., "+x = 1" -> "x = 1")
    cleaned_patch_lines = []
    for line in file_patch.splitlines():
        stripped = line.strip()
        if stripped.startswith("+") or stripped.startswith("-"):
            # Remove the leading diff marker
            cleaned_patch_lines.append(stripped[1:].strip())
        elif not stripped.startswith("@@") and not stripped.startswith("---") and not stripped.startswith("+++"):
            cleaned_patch_lines.append(stripped)
    
    patch_normalized = " ".join(" ".join(cleaned_patch_lines).split())
    
    # Check if signature appears in cleaned patch
    return sig_normalized in patch_normalized


class AISuggestionAcceptance(Metric):
    """
    AI Suggestion Acceptance: Ratio of accepted vs dismissed AI suggestions.
    
    AI suggestions are review comments with ```suggestion blocks from known
    AI code review bots (Copilot, codeant-ai, bito, korbit). Acceptance is
    heuristically determined by checking if the suggested code appears in
    the file's final patch (indicating it was merged).
    
    Key insight: High acceptance means developers trust AI suggestions;
    low acceptance may indicate low-quality suggestions or poor integration.
    """

    @property
    def slug(self) -> str:
        return "ai_suggestion_acceptance"

    @property
    def name(self) -> str:
        return "AI Suggestion Acceptance"

    @property
    def description(self) -> str:
        return "Ratio of accepted vs dismissed AI suggestions from review bots."

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

        # Get all PRs for the user
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        suggestions_made = 0
        suggestions_accepted = 0
        per_pr: list[dict] = []

        for pr in prs:
            # Get review comments for this PR
            comments = context.ledger.get_comments_for_pr(pr.number)
            
            # Filter to AI bot comments with suggestion blocks
            ai_suggestions = []
            for c in comments:
                if not _is_ai_bot(c.user.login):
                    continue
                blocks = _extract_suggestion_blocks(c.body)
                for block in blocks:
                    ai_suggestions.append({
                        "comment_id": c.id,
                        "bot": c.user.login,
                        "path": c.path,
                        "code": block,
                    })
            
            if not ai_suggestions:
                continue

            # Get file patches for this PR
            files = context.ledger.get_files_for_pr(pr.number)
            file_patches: dict[str, str] = {}
            for f in files:
                if f.patch:
                    file_patches[f.filename] = f.patch

            # Check acceptance for each suggestion
            pr_accepted = 0
            pr_total = 0
            for s in ai_suggestions:
                pr_total += 1
                suggestions_made += 1
                patch = file_patches.get(s["path"])
                if _suggestion_accepted(s["code"], patch):
                    pr_accepted += 1
                    suggestions_accepted += 1

            per_pr.append({
                "number": pr.number,
                "title": pr.title[:100] if pr.title else "",
                "suggestions_made": pr_total,
                "suggestions_accepted": pr_accepted,
                "acceptance_rate": (pr_accepted / pr_total * 100) if pr_total else 0.0,
            })

        # Compute overall rate
        acceptance_rate = (
            (suggestions_accepted / suggestions_made * 100)
            if suggestions_made > 0
            else 0.0
        )

        # Build summary
        if suggestions_made == 0:
            summary = "No AI suggestions found in the analysis period."
        else:
            summary = (
                f"AI suggestions: {suggestions_accepted}/{suggestions_made} accepted "
                f"({acceptance_rate:.1f}%)."
            )
            if acceptance_rate >= 70:
                summary += " (High trust in AI suggestions)"
            elif acceptance_rate < 30:
                summary += " (Low trust in AI suggestions)"

        details: dict[str, object] = {
            "suggestions_made": suggestions_made,
            "suggestions_accepted": suggestions_accepted,
            "suggestions_dismissed": suggestions_made - suggestions_accepted,
            "acceptance_rate": round(acceptance_rate, 1),
            "period_days": round(period_days, 1),
            "per_pr": per_pr,
            "ai_bots_detected": list(AI_BOT_LOGINS),
        }

        # No-data guard
        if suggestions_made == 0 or (period_days < 14 and suggestions_made < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
