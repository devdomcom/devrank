"""
AI-Assisted PR Rate Metric

Detects the percentage of PRs that were created with AI assistance
(Copilot, Cursor, Claude Code, etc.) based on commit message signatures,
PR descriptions, and known AI tool indicators.
"""

import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


# AI tool signatures found in commit messages or PR descriptions
_AI_TOOL_SIGNATURES = [
    # GitHub Copilot
    r"copilot",
    r"github[_\s]?copilot",
    r"co-authored-by:\s*github[_\s]?copilot",
    # Cursor (must be strict to avoid false positives like "cursor movement")
    r"cursor\.sh",
    r"generated[_\s]?by[_\s]?cursor",
    r"cursor[_\s]?generated",
    r"\bcursor\s+(?:ide|editor|ai)",
    r"\(cursor\)",  # e.g., "Feature (Cursor)"
    r"\bcursor\b.*\b(ai|generated|assistant)\b",
    # Claude Code / Claude
    r"claude[_\s]?code",
    r"claude\.ai",
    r"generated[_\s]?with[_\s]?claude",
    r"\bai[_\s]?assistant:\s*claude",
    # OpenAI / ChatGPT
    r"chatgpt",
    r"gpt-?4",
    r"gpt-?3",
    r"openai",
    r"generated[_\s]?by[_\s]?chatgpt",
    # Amazon CodeWhisperer
    r"codewhisperer",
    r"amazon[_\s]?codewhisperer",
    # JetBrains AI Assistant
    r"jetbrains[_\s]?ai",
    r"ai[_\s]?assistant",
    # Tabnine
    r"tabnine",
    # Cody (Sourcegraph)
    r"\bcody\b",
    r"sourcegraph[_\s]?cody",
    # Generic AI indicators
    r"generated[_\s]?by[_\s]?ai",
    r"ai[_\s]?generated",
    r"generated[_\s]?with[_\s]?ai",
]

_AI_SIGNATURE_RE = re.compile(
    r"(?:" + "|".join(_AI_TOOL_SIGNATURES) + r")",
    re.IGNORECASE,
)

# More specific patterns for high-confidence detection
_HIGH_CONFIDENCE_PATTERNS = [
    re.compile(r"co-authored-by:\s*[^<]*copilot", re.IGNORECASE),
    re.compile(r"generated[_\s]by[_\s](?:cursor|claude|chatgpt|copilot)", re.IGNORECASE),
    re.compile(r"(?:cursor|claude[_\s]?code|copilot)[_\s]generated", re.IGNORECASE),
]


def _detect_ai_assistance(text: str | None) -> tuple[bool, str | None]:
    """
    Detect AI assistance indicators in text.

    Returns:
        (is_ai_assisted, detected_tool_name)
    """
    if not text:
        return False, None

    text_lower = text.lower()

    # Check high-confidence patterns first
    for pattern in _HIGH_CONFIDENCE_PATTERNS:
        match = pattern.search(text)
        if match:
            # Extract tool name from match
            matched_text = match.group(0).lower()
            for tool in ["copilot", "cursor", "claude", "chatgpt", "codewhisperer"]:
                if tool in matched_text:
                    return True, tool
            return True, "ai_assisted"

    # Check general signatures
    match = _AI_SIGNATURE_RE.search(text)
    if match:
        matched_text = match.group(0).lower()
        # Map to canonical tool names
        if "copilot" in matched_text:
            return True, "copilot"
        elif "cursor" in matched_text:
            return True, "cursor"
        elif "claude" in matched_text:
            return True, "claude"
        elif "chatgpt" in matched_text or "gpt-4" in matched_text or "gpt-3" in matched_text:
            return True, "chatgpt"
        elif "codewhisperer" in matched_text:
            return True, "codewhisperer"
        elif "tabnine" in matched_text:
            return True, "tabnine"
        elif "cody" in matched_text:
            return True, "cody"
        else:
            return True, "ai_assisted"

    return False, None


def _analyze_pr_for_ai(pr, ledger) -> tuple[bool, str | None, list[dict]]:
    """
    Analyze a PR for AI assistance indicators.

    Returns:
        (is_ai_assisted, tool_name, evidence_list)
    """
    evidence = []

    # Check PR title
    title_ai, title_tool = _detect_ai_assistance(pr.title)
    if title_ai:
        evidence.append({"source": "pr_title", "tool": title_tool, "text": pr.title[:100]})

    # Check PR body
    body_ai, body_tool = _detect_ai_assistance(pr.body)
    if body_ai:
        evidence.append({"source": "pr_body", "tool": body_tool, "text": (pr.body or "")[:200]})

    # Check commit messages
    commits = ledger.get_commits_for_pr(pr.number)
    commit_evidence = []
    for commit in commits:
        commit_ai, commit_tool = _detect_ai_assistance(commit.message)
        if commit_ai:
            commit_evidence.append({
                "source": "commit",
                "tool": commit_tool,
                "sha": commit.sha[:8],
                "text": commit.message[:150],
            })

    evidence.extend(commit_evidence)

    # Determine overall AI assistance status
    if evidence:
        # Use the most specific tool name found
        tools_found = [e["tool"] for e in evidence if e["tool"]]
        specific_tools = [t for t in tools_found if t != "ai_assisted"]
        primary_tool = specific_tools[0] if specific_tools else (tools_found[0] if tools_found else "ai_assisted")
        return True, primary_tool, evidence

    return False, None, []


class AIAssistedPRRate(Metric):
    """
    Percentage of PRs created with AI assistance (Copilot, Cursor, Claude Code, etc.).

    This metric detects AI assistance by analyzing:
    - PR titles and descriptions for AI tool mentions
    - Commit messages for AI-generated signatures
    - Co-authored-by trailers indicating AI tools

    Critical for the AI era: provides baseline visibility into AI adoption
    and enables downstream analysis of AI code quality impact.
    """

    @property
    def slug(self) -> str:
        return "ai_assisted_pr_rate"

    @property
    def name(self) -> str:
        return "AI-Assisted PR Rate"

    @property
    def description(self) -> str:
        return "% PRs created with AI assistance (Copilot/Cursor/Claude/etc.) detected via commit/PR signatures."

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

        # Analyze each PR for AI assistance
        ai_assisted_prs = []
        human_prs = []
        tool_breakdown: dict[str, int] = {}

        for pr in prs:
            is_ai, tool, evidence = _analyze_pr_for_ai(pr, context.ledger)

            pr_info = {
                "number": pr.number,
                "title": pr.title[:100],
                "is_ai_assisted": is_ai,
                "tool": tool,
                "evidence": evidence[:3],  # Cap evidence to reduce payload size
            }

            if is_ai:
                ai_assisted_prs.append(pr_info)
                tool_breakdown[tool] = tool_breakdown.get(tool, 0) + 1
            else:
                human_prs.append(pr_info)

        total_prs = len(prs)
        ai_count = len(ai_assisted_prs)
        human_count = len(human_prs)
        ai_rate = (ai_count / total_prs * 100) if total_prs else 0.0

        # Build summary
        if total_prs == 0:
            summary = "No PRs found in the analysis period."
        else:
            summary = f"{ai_rate:.1f}% AI-assisted ({ai_count}/{total_prs} PRs)"
            if tool_breakdown:
                tool_str = ", ".join(f"{tool}={count}" for tool, count in sorted(tool_breakdown.items(), key=lambda x: -x[1]))
                summary += f". Tools: {tool_str}."

        details: dict[str, object] = {
            "ai_rate": round(ai_rate, 1),
            "ai_pr_count": ai_count,
            "human_pr_count": human_count,
            "total_pr_count": total_prs,
            "period_days": round(period_days, 1),
            "tool_breakdown": tool_breakdown,
            "ai_pr_numbers": [p["number"] for p in ai_assisted_prs],
            "human_pr_numbers": [p["number"] for p in human_prs],
            "per_pr": ai_assisted_prs + [{**p, "evidence": []} for p in human_prs],  # Full details for AI PRs, basic for human
        }

        # No-data guard: insufficient PRs for meaningful analysis
        if total_prs == 0 or (period_days < 14 and total_prs < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
