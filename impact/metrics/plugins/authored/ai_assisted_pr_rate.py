"""
AI-Assisted PR Rate Metric

Detects the percentage of PRs that were created with AI assistance
(Copilot, Cursor, Claude Code, etc.) based on commit message signatures,
PR descriptions, and known AI tool indicators.
"""

import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, _shannon_entropy, _compression_ratio, parse_functions


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
    # Standalone AI mention (catches cross-language: "AIが生成", "generado por AI")
    r"\bAI\b",
    # 2026 standard trailers (generic fallback)
    r"generated[-_\s]with:\s*\w+",
    r"generated[-_\s]by:\s*\w+",
    r"ai[-_\s]generated[-_\s]with:\s*\w+",
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
    # 2026 standard: Generated-with:/Generated-by: trailers (structured signatures)
    re.compile(r"generated[-_\s]with:\s*(?:github[_\s]?copilot|cursor|claude|chatgpt|copilot|codewhisperer|tabnine|cody)", re.IGNORECASE),
    re.compile(r"generated[-_\s]by:\s*(?:github[_\s]?copilot|cursor|claude|chatgpt|copilot|codewhisperer|tabnine|cody)", re.IGNORECASE),
    re.compile(r"ai[-_\s]generated[-_\s]with:\s*(?:github[_\s]?copilot|cursor|claude|chatgpt|copilot|codewhisperer|tabnine|cody)", re.IGNORECASE),
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


# Velocity anomaly thresholds (2026 best practices: AI paste workflows)
# Flag PRs with unrealistically high line counts in short time windows
_VELOCITY_THRESHOLDS = [
    # (max_minutes, min_lines, description)
    (15, 500, "500+ lines in 15 minutes"),
    (30, 1000, "1000+ lines in 30 minutes"),
    (60, 2000, "2000+ lines in 1 hour"),
    (120, 4000, "4000+ lines in 2 hours"),
]


def _detect_velocity_anomaly(pr) -> tuple[bool, dict | None]:
    """
    Detect velocity anomalies: unrealistically high line counts in short time windows.
    
    This is a 2026 best practice signal - AI-assisted "paste" workflows often produce
    large diffs in very short time spans (e.g., 2500 lines in 12 minutes).
    
    Returns:
        (is_suspicious, evidence_dict or None)
    """
    # Calculate total lines changed - handle MagicMock and missing attributes gracefully
    additions = getattr(pr, "additions", 0)
    deletions = getattr(pr, "deletions", 0)
    try:
        additions = int(additions) if additions is not None else 0
    except (TypeError, ValueError):
        additions = 0
    try:
        deletions = int(deletions) if deletions is not None else 0
    except (TypeError, ValueError):
        deletions = 0
    
    total_lines = additions + deletions
    if total_lines == 0:
        return False, None
    
    # Calculate time span (merged_at preferred, else updated_at, else now)
    from datetime import datetime, timezone
    end_time = getattr(pr, "merged_at", None) or getattr(pr, "updated_at", None) or getattr(pr, "closed_at", None)
    
    # Check if we have valid datetime objects (not MagicMock placeholders)
    try:
        if end_time is None or not hasattr(end_time, "timestamp"):
            # PR not yet merged/closed - use current time
            end_time = datetime.now(timezone.utc)
        
        start_time = getattr(pr, "created_at", None)
        if start_time is None or not hasattr(start_time, "timestamp"):
            return False, None
        
        time_span_seconds = (end_time - start_time).total_seconds()
    except (TypeError, AttributeError):
        # MagicMock or other non-datetime objects - can't compute velocity
        return False, None
    
    try:
        time_span_minutes = time_span_seconds / 60
    except TypeError:
        # MagicMock or invalid calculation
        return False, None
    
    # Avoid division by zero and very short times (<1 min)
    try:
        if time_span_minutes < 1:
            time_span_minutes = 1
    except TypeError:
        # MagicMock comparison
        return False, None
    
    # Calculate velocity (lines per minute)
    try:
        velocity = total_lines / time_span_minutes
    except TypeError:
        return False, None
    
    # Check against thresholds - wrap in try/except for MagicMock safety
    try:
        for max_minutes, min_lines, description in _VELOCITY_THRESHOLDS:
            if time_span_minutes <= max_minutes and total_lines >= min_lines:
                return True, {
                    "source": "velocity_anomaly",
                    "tool": "ai_assisted",
                    "text": f"{total_lines} lines in {time_span_minutes:.1f} min ({velocity:.0f} lines/min)",
                    "details": {
                        "additions": getattr(pr, "additions", 0),
                        "deletions": getattr(pr, "deletions", 0),
                        "total_lines": total_lines,
                        "time_span_minutes": round(time_span_minutes, 1),
                        "velocity_lines_per_min": round(velocity, 1),
                        "threshold": description,
                    }
                }
        
        # Also flag extreme velocity regardless of time window
        # e.g., >100 lines/minute is suspicious for any duration
        if velocity >= 100:
            return True, {
                "source": "velocity_anomaly",
                "tool": "ai_assisted",
                "text": f"Extreme velocity: {velocity:.0f} lines/min ({total_lines} lines)",
                "details": {
                    "additions": getattr(pr, "additions", 0),
                    "deletions": getattr(pr, "deletions", 0),
                    "total_lines": total_lines,
                    "time_span_minutes": round(time_span_minutes, 1),
                    "velocity_lines_per_min": round(velocity, 1),
                    "threshold": ">100 lines/min",
                }
            }
    except TypeError:
        # MagicMock comparison in thresholds
        pass
    
    return False, None


# Entropy thresholds for AI-generated code detection (2026 best practices)
# AI code tends to be highly repetitive (low entropy, high compressibility)
# These thresholds align with is_generated_file() in utils.py
_ENTROPY_LOW_THRESHOLD = 3.5  # Below this is suspicious (very predictable)
_COMPRESSION_LOW_THRESHOLD = 0.15  # Below this is suspicious (highly compressible)


def _detect_entropy_anomaly(pr, ledger) -> tuple[bool, dict | None]:
    """
    Detect entropy/compression anomalies in PR patch content.
    
    AI-generated code often has:
    - Low entropy (very predictable, repetitive patterns)
    - Low compression ratio (highly compressible due to repetition)
    
    This is a 2026 best practice signal based on stylometric analysis.
    
    Returns:
        (is_suspicious, evidence_dict or None)
    """
    try:
        files = ledger.get_files_for_pr(pr.number)
    except Exception:
        return False, None
    
    if not files:
        return False, None
    
    # Collect all added lines across all files
    all_added_lines = []
    for f in files:
        patch = getattr(f, "patch", None)
        if not patch:
            continue
        
        added_lines = [
            line[1:] for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]
        all_added_lines.extend(added_lines)
    
    if not all_added_lines:
        return False, None
    
    content = "\n".join(all_added_lines)
    if len(content) < 200:  # Need substantial content for meaningful analysis
        return False, None
    
    try:
        data = content.encode("utf-8", errors="replace")
        entropy = _shannon_entropy(data)
        ratio = _compression_ratio(data)
    except Exception:
        return False, None
    
    # Check for AI-like characteristics
    # Low entropy (<3.0) + low compression ratio (<0.20) = very repetitive code
    is_low_entropy = entropy < _ENTROPY_LOW_THRESHOLD
    is_low_compression = ratio < _COMPRESSION_LOW_THRESHOLD
    
    if is_low_entropy and is_low_compression:
        return True, {
            "source": "entropy_anomaly",
            "tool": "ai_assisted",
            "text": f"Low entropy ({entropy:.2f}) + low compressibility ({ratio:.2f})",
            "details": {
                "entropy": round(entropy, 2),
                "compression_ratio": round(ratio, 2),
                "files_analyzed": len([f for f in files if getattr(f, "patch", None)]),
                "total_lines_analyzed": len(all_added_lines),
                "threshold": f"entropy < {_ENTROPY_LOW_THRESHOLD} and compression < {_COMPRESSION_LOW_THRESHOLD}",
            }
        }
    
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

    # Check velocity anomaly (2026 best practice: AI paste workflows)
    velocity_suspicious, velocity_evidence = _detect_velocity_anomaly(pr)
    if velocity_suspicious and velocity_evidence:
        evidence.append(velocity_evidence)

    # Check entropy anomaly (2026 best practice: stylometric AI detection)
    entropy_suspicious, entropy_evidence = _detect_entropy_anomaly(pr, ledger)
    if entropy_suspicious and entropy_evidence:
        evidence.append(entropy_evidence)

    # Check style uniformity (tree-sitter AST analysis)
    style_suspicious, style_evidence = _detect_style_uniformity(pr, ledger)
    if style_suspicious and style_evidence:
        evidence.append(style_evidence)

    # Check for additional AI signals — pass whether we already have primary evidence
    has_primary = len(evidence) > 0
    additional_signals = _detect_additional_ai_signals(pr, ledger, has_primary_signal=has_primary)
    evidence.extend(additional_signals)

    # Determine overall AI assistance status
    if evidence:
        # Use the most specific tool name found
        tools_found = [e["tool"] for e in evidence if e["tool"]]
        specific_tools = [t for t in tools_found if t != "ai_assisted"]
        primary_tool = specific_tools[0] if specific_tools else (tools_found[0] if tools_found else "ai_assisted")
        return True, primary_tool, evidence

    return False, None, []


# Confidence scoring weights for AI assistance signals
# Higher scores = more reliable indicators of AI assistance
_CONFIDENCE_WEIGHTS = {
    # Explicit attribution (highest confidence)
    "co_author_trailer": 95,  # Co-authored-by: GitHub Copilot
    "generated_with_trailer": 90,  # Generated-with: Claude Code (2026 standard)
    "generated_by_trailer": 90,  # Generated-by: Cursor

    # Explicit tool mentions in text
    "pr_title_explicit": 80,  # Tool name in PR title (e.g., "Fix (Copilot)")
    "pr_body_explicit": 70,  # Tool name in PR body
    "commit_explicit": 75,  # Tool name in commit message

    # Indirect signals (lower confidence - could be fast/repetitive human work)
    "velocity_anomaly": 45,  # High line count in short time
    "entropy_anomaly": 40,  # Low entropy + low compression
    "style_uniformity": 55,  # AST-based uniform style detection

    # Additional signals
    "no_todo_fixme": 35,  # No TODO/FIXME in substantial PR
    "low_comment_density": 30,  # Very low comment density
    "uniform_naming": 40,  # Highly uniform naming patterns

    # Generic AI mentions (no specific tool)
    "generic_ai": 50,  # "AI generated", "generated with AI", etc.
}

# Multi-signal bonus: having multiple signals increases confidence
_MULTI_SIGNAL_BONUS = 15  # +15 points per additional signal (capped at 100)


def _calculate_confidence_score(evidence: list[dict]) -> float:
    """
    Calculate a confidence score (0-100) for AI assistance based on all signals.

    Combines multiple evidence sources with weighted scoring:
    - Explicit attributions (trailers, tool names): high confidence
    - Indirect signals (velocity, entropy): moderate confidence
    - Multiple signals: cumulative bonus

    Returns:
        Confidence score from 0.0 to 100.0
    """
    if not evidence:
        return 0.0

    total_score = 0.0
    signal_count = 0

    for ev in evidence:
        source = ev.get("source", "")
        tool = ev.get("tool", "")
        text = ev.get("text", "")

        # Determine base score based on source and tool specificity
        base_score = 0.0

        # Explicit trailers (Co-authored-by, Generated-with:)
        if source in ("pr_title", "pr_body", "commit"):
            text_lower = text.lower() if text else ""
            if "co-authored-by" in text_lower or "generated-with:" in text_lower or "generated-by:" in text_lower:
                if "copilot" in text_lower or "cursor" in text_lower or "claude" in text_lower or "chatgpt" in text_lower:
                    base_score = _CONFIDENCE_WEIGHTS.get("generated_with_trailer", 90)
                else:
                    base_score = _CONFIDENCE_WEIGHTS.get("generated_with_trailer", 90)
            elif tool and tool != "ai_assisted":
                # Explicit tool name mentioned
                if source == "pr_title":
                    base_score = _CONFIDENCE_WEIGHTS.get("pr_title_explicit", 80)
                elif source == "pr_body":
                    base_score = _CONFIDENCE_WEIGHTS.get("pr_body_explicit", 70)
                elif source == "commit":
                    base_score = _CONFIDENCE_WEIGHTS.get("commit_explicit", 75)
            elif tool == "ai_assisted":
                # Generic AI mention
                base_score = _CONFIDENCE_WEIGHTS.get("generic_ai", 50)

        # Velocity anomaly
        elif source == "velocity_anomaly":
            base_score = _CONFIDENCE_WEIGHTS.get("velocity_anomaly", 45)

        # Entropy anomaly
        elif source == "entropy_anomaly":
            base_score = _CONFIDENCE_WEIGHTS.get("entropy_anomaly", 40)

        # Style uniformity (AST-based)
        elif source == "style_uniformity":
            base_score = _CONFIDENCE_WEIGHTS.get("style_uniformity", 55)

        # Additional signals
        elif source == "no_todo_fixme":
            base_score = _CONFIDENCE_WEIGHTS.get("no_todo_fixme", 35)

        elif source == "low_comment_density":
            base_score = _CONFIDENCE_WEIGHTS.get("low_comment_density", 30)

        elif source == "uniform_naming":
            base_score = _CONFIDENCE_WEIGHTS.get("uniform_naming", 40)

        if base_score > 0:
            total_score += base_score
            signal_count += 1

    if signal_count == 0:
        return 0.0

    # Apply multi-signal bonus: each additional signal beyond the first adds bonus
    if signal_count > 1:
        bonus = (signal_count - 1) * _MULTI_SIGNAL_BONUS
        total_score += bonus

    # Cap at 100
    return min(100.0, total_score)


# Style uniformity detection thresholds
_STYLE_UNIFORMITY_LENGTH_CV_THRESHOLD = 0.25  # Coeff of variation for function lengths (lower = more uniform)
_STYLE_UNIFORMITY_PARAM_CV_THRESHOLD = 0.30  # Coeff of variation for param counts
_STYLE_UNIFORMITY_MIN_FUNCTIONS = 3  # Need at least this many functions to analyze uniformity
_STYLE_UNIFORMITY_TRIVIAL_RATIO_THRESHOLD = 0.5  # If >50% functions are trivial, suspicious


def _detect_style_uniformity(pr, ledger) -> tuple[bool, dict | None]:
    """
    Detect style uniformity using tree-sitter AST analysis.

    AI-generated code often has very uniform patterns:
    - Functions of similar length (low coefficient of variation)
    - Consistent parameter counts
    - High proportion of trivial functions (getters/setters)
    - Consistent naming conventions

    Uses parse_functions() from utils.py for AST-based function analysis.

    Returns:
        (is_suspicious, evidence_dict or None)
    """
    try:
        files = ledger.get_files_for_pr(pr.number)
    except Exception:
        return False, None

    if not files:
        return False, None

    all_functions = []

    for f in files:
        # Try full content first, fall back to patch (added lines only)
        content = getattr(f, "content", None)
        filename = getattr(f, "filename", "")

        if not content:
            # Extract added lines from patch
            patch = getattr(f, "patch", None)
            if not patch:
                continue
            added_lines = [
                line[1:] for line in patch.splitlines()
                if line.startswith("+") and not line.startswith("+++")
            ]
            if not added_lines:
                continue
            content = "\n".join(added_lines)
            # Need to parse as complete file; skip if not parseable
            # For patch-only analysis, try to parse but may fail
            if not any(filename.endswith(ext) for ext in (".py", ".js", ".ts", ".go", ".rs", ".java")):
                continue

        if not content or len(content) < 100:
            continue

        try:
            functions = parse_functions(content, filename)
            all_functions.extend(functions)
        except Exception:
            continue

    if len(all_functions) < _STYLE_UNIFORMITY_MIN_FUNCTIONS:
        return False, None

    # Analyze uniformity
    lengths = [f["end_line"] - f["start_line"] + 1 for f in all_functions]
    param_counts = [f["parameter_count"] for f in all_functions]
    trivial_count = sum(1 for f in all_functions if f["is_trivial"])

    # Calculate coefficient of variation (std / mean)
    def _cv(values: list[int]) -> float:
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        if mean == 0:
            return 0.0
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance ** 0.5
        return std / mean

    length_cv = _cv(lengths)
    param_cv = _cv(param_counts)
    trivial_ratio = trivial_count / len(all_functions)

    # Check for uniformity signals
    is_uniform_length = length_cv < _STYLE_UNIFORMITY_LENGTH_CV_THRESHOLD
    is_uniform_params = param_cv < _STYLE_UNIFORMITY_PARAM_CV_THRESHOLD
    is_high_trivial = trivial_ratio > _STYLE_UNIFORMITY_TRIVIAL_RATIO_THRESHOLD

    # Need at least 2 signals to be suspicious
    signals = sum([is_uniform_length, is_uniform_params, is_high_trivial])

    if signals >= 2:
        return True, {
            "source": "style_uniformity",
            "tool": "ai_assisted",
            "text": f"Uniform style: length_cv={length_cv:.2f}, param_cv={param_cv:.2f}, trivial_ratio={trivial_ratio:.2f}",
            "details": {
                "function_count": len(all_functions),
                "length_cv": round(length_cv, 2),
                "param_cv": round(param_cv, 2),
                "trivial_ratio": round(trivial_ratio, 2),
                "signals_detected": signals,
                "is_uniform_length": is_uniform_length,
                "is_uniform_params": is_uniform_params,
                "is_high_trivial": is_high_trivial,
            }
        }

    return False, None


def _detect_additional_ai_signals(pr, ledger, has_primary_signal: bool = False) -> list[dict]:
    """
    Detect additional AI assistance signals from PR content.

    Signals detected:
    - TODO/FIXME markers (humans leave these, AI often doesn't) — only fires
      as corroborating signal when has_primary_signal is True (§2.3 fix)
    - Comment density (AI may add verbose comments or omit them)
    - Uniform naming patterns (all camelCase, all snake_case)

    Returns:
        List of evidence dicts for detected signals
    """
    evidence = []

    try:
        files = ledger.get_files_for_pr(pr.number)
    except Exception:
        return evidence

    if not files:
        return evidence

    total_todo_fixme = 0
    total_comments = 0
    total_code_lines = 0
    naming_patterns: dict[str, int] = {}  # camelCase, snake_case, etc.

    for f in files:
        patch = getattr(f, "patch", None)
        if not patch:
            continue

        # Extract added lines
        added_lines = [
            line[1:] for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]

        for line in added_lines:
            total_code_lines += 1

            # TODO/FIXME detection — broadened to catch any ALL-CAPS annotation
            # (e.g., TODO:, FIXME:, HACK:, NOTE:, XXX: — universal convention)
            stripped = line.strip()
            if re.search(r'\b[A-Z]{2,}:', stripped):
                total_todo_fixme += 1
            elif "todo" in stripped.lower() or "fixme" in stripped.lower():
                total_todo_fixme += 1

            # Comment detection — expanded prefixes for all major languages (§3.3)
            # #(Python/Ruby/Perl), //(C/Java/JS/Go/Rust), /*(C-family), *(doc),
            # --(SQL/Haskell/Lua), <!--(HTML/XML), %(MATLAB/LaTeX), ;(Lisp/ASM), (*(OCaml/Pascal)
            _comment_prefixes = ("#", "//", "/*", "*", "--", "<!--", "%", ";", "(*")
            if any(stripped.startswith(p) for p in _comment_prefixes):
                total_comments += 1

            # Naming pattern detection (simple heuristic on identifiers)
            # Find potential identifiers
            identifiers = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", stripped)
            for ident in identifiers:
                if "_" in ident and ident.islower():
                    naming_patterns["snake_case"] = naming_patterns.get("snake_case", 0) + 1
                elif ident[0].isupper() and ident[1:].islower():
                    naming_patterns["PascalCase"] = naming_patterns.get("PascalCase", 0) + 1
                elif ident[0].islower() and any(c.isupper() for c in ident[1:]):
                    naming_patterns["camelCase"] = naming_patterns.get("camelCase", 0) + 1
                elif ident.islower():
                    naming_patterns["lowercase"] = naming_patterns.get("lowercase", 0) + 1

    # Signal 1: No TODO/FIXME in substantial PR (AI often doesn't leave these)
    # Downgraded to corroborating-only: only fires when a primary AI signal exists.
    # This avoids false positives for non-English teams that use native-language
    # annotations instead of TODO/FIXME.
    if has_primary_signal and total_code_lines > 100 and total_todo_fixme == 0:
        evidence.append({
            "source": "no_todo_fixme",
            "tool": "ai_assisted",
            "text": f"No TODO/FIXME markers in {total_code_lines} lines",
            "details": {
                "total_lines": total_code_lines,
                "todo_fixme_count": total_todo_fixme,
            }
        })

    # Signal 2: Very low comment density (<2% of lines) in substantial PR
    if total_code_lines > 50:
        comment_ratio = total_comments / total_code_lines
        if comment_ratio < 0.02:  # Less than 2% comments
            evidence.append({
                "source": "low_comment_density",
                "tool": "ai_assisted",
                "text": f"Low comment density: {comment_ratio:.1%}",
                "details": {
                    "comment_ratio": round(comment_ratio, 3),
                    "comment_count": total_comments,
                    "total_lines": total_code_lines,
                }
            })

    # Signal 3: Highly uniform naming (one pattern dominates >80%)
    if naming_patterns:
        total_idents = sum(naming_patterns.values())
        if total_idents > 20:
            dominant = max(naming_patterns.values())
            dominant_ratio = dominant / total_idents
            if dominant_ratio > 0.8:
                dominant_pattern = max(naming_patterns, key=naming_patterns.get)
                evidence.append({
                    "source": "uniform_naming",
                    "tool": "ai_assisted",
                    "text": f"Uniform naming: {dominant_pattern} ({dominant_ratio:.0%})",
                    "details": {
                        "dominant_pattern": dominant_pattern,
                        "dominant_ratio": round(dominant_ratio, 2),
                        "patterns": naming_patterns,
                    }
                })

    return evidence


class AIAssistedPRRate(Metric):
    """
    Percentage of PRs created with AI assistance (Copilot, Cursor, Claude Code, etc.).

    This metric detects AI assistance by analyzing:
    - PR titles and descriptions for AI tool mentions
    - Commit messages for AI-generated signatures
    - Co-authored-by trailers indicating AI tools
    - Generated-with:/Generated-by: trailers (2026 standard)
    - Velocity anomalies: unrealistically high line counts in short time windows
    - Entropy/compression anomalies: stylometric detection of repetitive AI code
    - Style uniformity: tree-sitter AST analysis of uniform code patterns
    - Additional signals: TODO/FIXME absence, low comments, uniform naming

    Includes per-PR confidence scores (0-100) combining all signals.

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
        return "% PRs created with AI assistance (Copilot/Cursor/Claude/etc.) detected via commit/PR signatures and 2026 Generated-with:/Generated-by: trailers. Includes per-PR confidence scores (0-100) combining all signals."

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

            # Calculate confidence score for AI-assisted PRs
            confidence = _calculate_confidence_score(evidence) if evidence else 0.0

            pr_info = {
                "number": pr.number,
                "title": pr.title[:100],
                "is_ai_assisted": is_ai,
                "tool": tool,
                "confidence": round(confidence, 1),
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

        # Calculate aggregate confidence stats for AI-assisted PRs
        ai_confidences = [p["confidence"] for p in ai_assisted_prs]
        ai_confidence_avg = sum(ai_confidences) / len(ai_confidences) if ai_confidences else 0.0
        ai_confidence_high = sum(1 for c in ai_confidences if c >= 80)
        ai_confidence_medium = sum(1 for c in ai_confidences if 50 <= c < 80)
        ai_confidence_low = sum(1 for c in ai_confidences if c < 50)

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
            # Confidence aggregates
            "ai_confidence_avg": round(ai_confidence_avg, 1),
            "ai_confidence_high": ai_confidence_high,  # >= 80%
            "ai_confidence_medium": ai_confidence_medium,  # 50-79%
            "ai_confidence_low": ai_confidence_low,  # < 50%
        }

        # No-data guard: insufficient PRs for meaningful analysis
        if total_prs == 0 or (period_days < 14 and total_prs < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
