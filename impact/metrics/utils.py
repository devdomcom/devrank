import math
import os
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict
import re

import yaml

from impact.domain.models import MetricContext, ReviewState
from impact.ledger.ledger import Ledger


# ---------------------------------------------------------------------------
# Customisation config loader (§P14)
# ---------------------------------------------------------------------------
_customization_cache: dict[str, Any] | None = None


def _load_customization() -> dict[str, Any]:
    """Load organisation-level customisation from ``customization.yaml``.

    Resolution order:
      1. ``DEVRANK_CUSTOMIZATION_PATH`` env var (explicit override).
      2. ``impact/config/customization.yaml`` (repo default).

    Returns an empty dict on any load error (graceful degradation).
    The result is cached for the process lifetime.
    """
    global _customization_cache
    if _customization_cache is not None:
        return _customization_cache

    env_path = os.environ.get("DEVRANK_CUSTOMIZATION_PATH")
    if env_path:
        config_path = Path(env_path)
    else:
        config_path = Path(__file__).resolve().parents[2] / "config" / "customization.yaml"

    try:
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        data = {}

    _customization_cache = data
    return data


def _reset_customization_cache() -> None:
    """Reset the cached customisation data (for testing)."""
    global _customization_cache
    _customization_cache = None


def is_bot_user(user) -> bool:
    """Check whether a user is a bot.

    Relies on the canonical ``is_bot`` flag set by the provider adapter.
    Each adapter applies its own provider-specific detection logic:

    * GitHub: ``type == Bot``, ``[bot]`` suffix, ``BOT_`` node_id prefix
    * GitLab: ``bot: true`` API flag
    * Bitbucket: ``type == "app"``

    This keeps provider knowledge out of the metrics layer (§7.1).
    """
    return getattr(user, "is_bot", False)


class Interaction(TypedDict):
    actor: str
    kind: str  # review|comment_issue|comment_review|timeline
    created_at: Any


def percentile(values: list[float], pct: float) -> float:
    """
    Calculate the percentile of a list of values using linear interpolation.

    Args:
        values: List of numeric values.
        pct: Percentile to compute (0.0 to 1.0, e.g., 0.5 for median, 0.75 for p75).

    Returns:
        The interpolated percentile value, or 0.0 if the list is empty.
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


def calculate_merge_time_hours(pr) -> float | None:
    """Calculate merge time in hours for a PR, or None if not merged."""
    if pr.merged and pr.merged_at and pr.created_at:
        delta = pr.merged_at - pr.created_at
        return delta.total_seconds() / 3600
    return None


def calculate_coding_time_to_pr_hours(ledger, pr) -> float | None:
    """Time (hours) from first commit on PR's branch to PR creation (pre-PR coding)."""
    commits = ledger.get_commits_for_pr(pr.number)
    if not commits or not pr.created_at:
        return None
    # First commit (sorted by date in ledger)
    first_commit = min(commits, key=lambda c: c.date)
    if first_commit.date >= pr.created_at:
        return None  # commit at/after open -- clock skew, rebase, or force-push
    delta = pr.created_at - first_commit.date
    return delta.total_seconds() / 3600


def calculate_merge_delay_hours(ledger, pr) -> float | None:
    """Median post-approval delay: latest APPROVED review to merge (bottleneck isolation)."""
    if not (pr.merged and pr.merged_at):
        return None
    reviews = ledger.get_reviews_for_pr(pr.number)
    approvals = [r for r in reviews if r.state == ReviewState.APPROVED]
    if not approvals:
        return None
    # Latest approval before merge
    last_approval = max(approvals, key=lambda r: r.submitted_at)
    if last_approval.submitted_at >= pr.merged_at:
        return 0.0
    delta = pr.merged_at - last_approval.submitted_at
    return delta.total_seconds() / 3600


def collect_pr_interactions(
    context: MetricContext, pr_number: int, author: str, cutoff_time: datetime | None = None
) -> list[Interaction]:
    """Collect interactions (reviews, comments, timeline events) for a PR up to cutoff_time, excluding bots."""
    interactions: list[Interaction] = []

    # Reviews
    for rev in context.ledger.get_reviews_for_pr(pr_number):
        if rev.user.login == author or is_bot_user(rev.user):
            continue
        if cutoff_time and rev.submitted_at >= cutoff_time:
            continue
        interactions.append(
            {"actor": rev.user.login, "kind": "review", "created_at": rev.submitted_at}
        )

    # Comments (issue + review)
    for c in context.ledger.get_comments_for_pr(pr_number):
        if c.user.login == author or is_bot_user(c.user):
            continue
        ts = c.created_at
        if cutoff_time and ts >= cutoff_time:
            continue
        kind = "comment_review" if c.type.value == "review" else "comment_issue"
        interactions.append({"actor": c.user.login, "kind": kind, "created_at": ts})

    # Timeline fallbacks (covers events not already represented)
    seen_ts_ids = {(i["actor"], i["created_at"]) for i in interactions}
    for evt in context.ledger.get_timeline_for_pr(pr_number):
        if evt.actor.login == author or is_bot_user(evt.actor):
            continue
        if cutoff_time and evt.created_at >= cutoff_time:
            continue
        if evt.event in ("reviewed", "commented"):
            key = (evt.actor.login, evt.created_at)
            if key not in seen_ts_ids:
                interactions.append(
                    {"actor": evt.actor.login, "kind": "timeline", "created_at": evt.created_at}
                )
                seen_ts_ids.add(key)

    interactions.sort(key=lambda i: i["created_at"])
    return interactions


def is_pr_merged_after(ledger: Ledger, pr_number: int, after_time: datetime) -> bool:
    """Check if a PR was merged after a given time."""
    pr = ledger.get_pr(pr_number)
    if not pr or not pr.merged:
        return False
    if pr.merged_at and pr.merged_at >= after_time:
        return True
    # Fallback to timeline events
    for evt in ledger.get_timeline_for_pr(pr_number):
        if evt.event == "merged" and evt.created_at >= after_time:
            return True
    return False


def has_pr_event_after(
    ledger: Ledger, pr_number: int, after_time: datetime, event_type: str | None = None
) -> bool:
    """Check if a PR has any timeline event (or specific type) after a given time."""
    for evt in ledger.get_timeline_for_pr(pr_number):
        if evt.created_at > after_time and (event_type is None or evt.event == event_type):
            return True
    return False


def is_change_request(review, ledger) -> bool:
    """Check if a review is a change request (formal or via inline comments)."""
    if review.state.value == "changes_requested":
        return True
    if review.state.value != "commented":
        return False
    comments = ledger.get_review_comments_for_review(review.id)
    return any(comments)


def get_pr_effective_changes(ledger, pr) -> tuple[int, int, int]:
    """Compute non-generated additions, deletions, and generated lines for a PR.

    Returns (effective_additions, effective_deletions, generated_lines).
    Generated files (lockfiles, vendored, minified, etc.) are excluded.
    Falls back to pr.additions/pr.deletions when no file records exist.
    """
    files = ledger.get_files_for_pr(pr.number)
    if not files:
        # No file-level data; fall back to PR-level counts (no filtering possible)
        return pr.additions, pr.deletions, 0
    eff_add = 0
    eff_del = 0
    gen_lines = 0
    for f in files:
        if is_generated_file(f.filename, f.patch):
            gen_lines += f.additions + f.deletions
        else:
            eff_add += f.additions
            eff_del += f.deletions
    return eff_add, eff_del, gen_lines


def get_pr_size_category(changes: int) -> str:
    """
    Categorize a PR based on total lines changed.

    Categories:
        - trivial: < 10 changes
        - small: 10-99 changes
        - medium: 100-999 changes
        - large: >= 1000 changes
    """
    if changes < 10:
        return "trivial"
    elif changes < 100:
        return "small"
    elif changes < 1000:
        return "medium"
    else:
        return "large"


def get_week_activity_details(
    dates: list[datetime], start_date: datetime | None, end_date: datetime | None
) -> dict:
    """Return granular week details: active/inactive weeks, gaps (disengagement)."""
    if not start_date or not end_date:
        return {
            "active_weeks": [],
            "inactive_weeks": [],
            "active_count": 0,
            "total_weeks": 0,
            "max_gap_weeks": 0,
        }

    all_week_keys = set()
    current = start_date
    while current <= end_date:
        all_week_keys.add(current.isocalendar()[:2])
        current += timedelta(days=1)

    active_week_keys = {dt.isocalendar()[:2] for dt in dates} if dates else set()
    inactive_week_keys = all_week_keys - active_week_keys

    if not inactive_week_keys:
        max_gap = 0
    else:
        sorted_inactive = sorted(inactive_week_keys)
        max_gap = 1
        current_gap = 1
        for i in range(1, len(sorted_inactive)):
            prev_y, prev_w = sorted_inactive[i - 1]
            curr_y, curr_w = sorted_inactive[i]
            if (curr_y == prev_y and curr_w == prev_w + 1) or (
                curr_y == prev_y + 1 and curr_w == 1 and prev_w >= 52
            ):
                current_gap += 1
                max_gap = max(max_gap, current_gap)
            else:
                current_gap = 1
        max_gap = max(max_gap, current_gap)

    def fmt(wk):
        return f"{wk[0]}-W{wk[1]:02d}"

    return {
        "active_weeks": sorted([fmt(wk) for wk in active_week_keys]),
        "inactive_weeks": sorted([fmt(wk) for wk in inactive_week_keys]),
        "active_count": len(active_week_keys),
        "total_weeks": len(all_week_keys),
        "max_gap_weeks": max_gap,
        "active_ratio": len(active_week_keys) / len(all_week_keys) if all_week_keys else 0.0,
    }


def get_weekly_activity_counts(dates: list[datetime]) -> dict:
    """Group dates by ISO week and count activities per week (DRY for burst metrics)."""
    if not dates:
        return {}
    week_counts = Counter()
    for dt in dates:
        wk = dt.isocalendar()[:2]
        week_counts[wk] += 1

    # Return as { "YYYY-WW": count, ... } sorted
    def fmt(wk):
        return f"{wk[0]}-W{wk[1]:02d}"

    return {fmt(wk): count for wk, count in sorted(week_counts.items())}


def compute_coding_days_ratio(
    dates: list[datetime], start_date: datetime | None, end_date: datetime | None
) -> dict:
    """Distinct commit days / working days (Mon-Fri) in period; ratio 0-1."""
    if not start_date or not end_date or not dates:
        return {"active_days": 0, "total_working_days": 0, "ratio": 0.0, "per_day": []}
    # Dedup active days
    active_days = sorted({d.date() for d in dates})  # date() for day-level
    # All working days (exclude weekends)
    working_days = []
    current = start_date.date()
    end_d = end_date.date()
    while current <= end_d:
        if current.weekday() < 5:  # Mon=0 ... Fri=4
            working_days.append(current)
        current += timedelta(days=1)
    # Active working days
    active_working = [d for d in active_days if d.weekday() < 5]
    total_working = len(working_days)
    ratio = len(active_working) / total_working if total_working else 0.0
    return {
        "active_days": len(active_working),
        "total_working_days": total_working,
        "ratio": ratio,
        "per_day": [d.isoformat() for d in active_working],  # for details
    }


def review_led_to_merge(ledger, review, max_hours=48) -> bool:
    """Check if user's review led to merge with close proximity (review -> commit -> merge, no intervening other reviews)."""
    pr_num = review.pull_request_number
    rev_time = review.submitted_at
    # Merged after?
    if not is_pr_merged_after(ledger, pr_num, rev_time):
        return False
    pr = ledger.get_pr(pr_num)
    if not pr:
        return False
    # Author commit after review within window?
    commits = [
        c
        for c in ledger.get_commits_for_pr(pr_num)
        if c.date > rev_time and c.author.login == pr.user.login
    ]
    if not commits:
        return False
    first_commit = min(commits, key=lambda c: c.date)
    if (first_commit.date - rev_time).total_seconds() / 3600 > max_hours:
        return False
    # No other reviews between this review and first commit?
    other_reviews = [
        r
        for r in ledger.get_reviews_for_pr(pr_num)
        if r.submitted_at > rev_time
        and r.submitted_at < first_commit.date
        and r.user.login != review.user.login
    ]
    return not other_reviews


def review_led_to_commit(ledger, review, max_hours=24) -> bool:
    """Check if review led to immediate author commit (clear correlation, no intervening reviews)."""
    pr_num = review.pull_request_number
    rev_time = review.submitted_at
    pr = ledger.get_pr(pr_num)
    if not pr:
        return False
    # Author commit after review within window?
    commits = [
        c
        for c in ledger.get_commits_for_pr(pr_num)
        if c.date > rev_time and c.author.login == pr.user.login
    ]
    if not commits:
        return False
    first_commit = min(commits, key=lambda c: c.date)
    if (first_commit.date - rev_time).total_seconds() / 3600 > max_hours:
        return False
    # No other reviews between?
    other_reviews = [
        r
        for r in ledger.get_reviews_for_pr(pr_num)
        if r.submitted_at > rev_time
        and r.submitted_at < first_commit.date
        and r.user.login != review.user.login
    ]
    return not other_reviews


def approval_was_final(ledger, review, max_hours_to_merge=48) -> bool:
    """Check if approval was last activity (no later commits/CRs, leads to merge)."""
    if review.state.value != "approved":
        return False
    pr_num = review.pull_request_number
    rev_time = review.submitted_at
    # No later commits or CRs? (exclude merge commits via parent count)
    pr = ledger.get_pr(pr_num)
    later_commits = [
        c for c in ledger.get_commits_for_pr(pr_num)
        if c.date > rev_time
        and c.sha != getattr(pr, 'merge_commit_sha', None)
        and not is_merge_commit(c)
    ]
    later_crs = [
        r
        for r in ledger.get_reviews_for_pr(pr_num)
        if r.submitted_at > rev_time and r.state.value == "changes_requested"
    ]
    if later_commits or later_crs:
        return False
    # Merged after within window?
    if not is_pr_merged_after(ledger, pr_num, rev_time):
        return False
    pr = ledger.get_pr(pr_num)
    if not pr or not pr.merged_at:
        return True
    return (pr.merged_at - rev_time).total_seconds() / 3600 <= max_hours_to_merge


def is_immediate_approval(ledger: Ledger, pr_number: int, author_login: str) -> bool:
    """First non-self non-bot review is APPROVED; no prior CR/inline via is_change_request."""
    reviews = ledger.get_reviews_for_pr(pr_number)
    non_self = [
        r for r in reviews
        if r.user.login != author_login and not is_bot_user(r.user)
    ]
    if not non_self:
        return False
    non_self.sort(key=lambda r: r.submitted_at)
    # Find first APPROVED
    first_approved = next((r for r in non_self if r.state == ReviewState.APPROVED), None)
    if not first_approved:
        return False
    # No CR before first approval
    idx = non_self.index(first_approved)
    for r in non_self[:idx]:
        if is_change_request(r, ledger):
            return False
    # Group check: inline comments attached to the SAME review ID as the
    # approval are part of the approval (GitHub's batch-review flow), not a
    # prior rejection.  Only standalone inline comments before the approval
    # should count as blocking.
    approved_review_ids = {r.id for r in non_self if r.state == ReviewState.APPROVED}
    for r in non_self[:idx]:
        # Check if this review has standalone inline comments NOT part of an approval
        if r.state == ReviewState.COMMENTED:
            comments = ledger.get_review_comments_for_review(r.id)
            if comments and r.id not in approved_review_ids:
                return False
    return True


def is_merge_commit(commit) -> bool:
    """Structural merge detection via Git DAG parent count.

    A merge commit has >=2 parents.  This is language-neutral (no English
    message parsing) and works regardless of the developer's Git locale.
    Falls back to English prefix check when parent_count is unavailable.
    """
    parent_count = getattr(commit, "parent_count", None)
    if parent_count is not None:
        return parent_count >= 2
    # Legacy fallback: English merge-commit prefix (for older data without parent_count)
    msg = getattr(commit, "message", "") or ""
    return msg.lower().startswith("merge ")


# Built-in bug-fix label set (team-applied metadata — most reliable signal).
# Covers common label conventions across GitHub, GitLab, Jira, etc.
_BUG_LABELS_BUILTIN: frozenset[str] = frozenset({
    "bug", "fix", "hotfix", "bugfix",
    "type:bug", "type:fix", "kind/bug", "kind/fix",
    "type/bug", "type/fix", "priority/critical",
    "defect", "regression",
})


def _get_bug_labels() -> frozenset[str]:
    """Return the full bug-fix label set (built-in + config extras)."""
    cfg = _load_customization()
    extras = cfg.get("extra_bug_labels") or []
    if not extras:
        return _BUG_LABELS_BUILTIN
    return _BUG_LABELS_BUILTIN | frozenset(l.lower() for l in extras)

# Provider-neutral cross-reference pattern: #NNN, !NNN, AB#NNN
_CROSS_REF_RE = re.compile(r'(?<!\w)(?:AB)?[#!]\d{1,6}\b')


def is_bug_fix_indicator(text: str, labels: list[str] | None = None) -> bool:
    """Multi-signal bug-fix detection — labels-first, then structural, then English.

    Priority:
      1. PR labels (language-neutral, team-applied ground truth)
      2. Conventional commit prefix ``fix:`` (spec-defined protocol token)
      3. Cross-reference pattern (#NNN, !NNN — provider-neutral)
      4. English keyword fallback (backward-compatible)
    """
    # Priority 1: labels
    if labels:
        bug_labels = _get_bug_labels()
        if any(l.lower() in bug_labels for l in labels):
            return True

    if not text:
        return False
    text_lower = text.lower()

    # Priority 2: Conventional commit prefix (spec-defined, not English)
    if re.match(r'^fix(\(.*?\))?!?:', text_lower):
        return True

    # Priority 3: Cross-reference patterns (provider-neutral numeric refs)
    if _CROSS_REF_RE.search(text):
        # Specific prefix patterns that combine a bug keyword with a ref
        prefix_patterns = ["bugfix:", "bug fix", "bug:", "hotfix"]
        if any(p in text_lower for p in prefix_patterns):
            return True

    # Priority 4: English keyword fallback (backward-compatible)
    prefix_patterns = ["bugfix:", "bug fix", "bug:", "hotfix"]
    if any(p in text_lower for p in prefix_patterns):
        return True
    # Issue/PR refs with English verbs
    if re.search(r'(?:fixes|closes|resolves|issue)\s*#\d+', text_lower):
        return True
    # Ambiguous terms need word boundaries
    word_boundary_patterns = [r'\berror\b', r'\bcrash\b', r'\bregression\b']
    return any(re.search(p, text_lower) for p in word_boundary_patterns)


_REVERT_SHA_RE = re.compile(r'[a-f0-9]{40}')


def is_revert_indicator(text: str) -> bool:
    """Detect reverts via Git platform prefix and structural SHA-based fallback.

    Layer 1: English ``Revert`` prefix / ``reverts commit`` phrase (Git default).
    Layer 2: Any 40-char hex SHA in the message body (language-neutral).
    """
    if not text:
        return False
    text_lower = text.lower()
    # Layer 1 (platform default): Git always inserts English "Revert" prefix
    if text_lower.startswith("revert") or "reverts commit" in text_lower:
        return True
    # Layer 2 (structural): body contains a 40-char SHA — strong revert signal
    body = text.split("\n", 1)[1] if "\n" in text else ""
    return bool(_REVERT_SHA_RE.search(body))


def is_test_file(filename: str) -> bool:
    """DRY helper to classify test files (heuristics; distinct from area breadth)."""
    if not filename:
        return False
    f_lower = filename.lower()
    # Patterns with delimiters are safe for substring matching
    substring_patterns = [
        "/test/", "/tests/", "__tests__/", ".test.", ".spec.", "spec_",
        "/testing/",
    ]
    if any(p in f_lower for p in substring_patterns):
        return True
    # Root-level test directories (no leading /)
    if f_lower.startswith("tests/") or f_lower.startswith("test/"):
        return True
    # test_ prefix at start of filename or after /
    if re.search(r'(^|/)test_', f_lower):
        return True
    # _test. suffix (e.g., foo_test.py, bar_test.ts)
    if re.search(r'_test\.', f_lower):
        return True
    # Tool/framework names should match as path segments
    if re.search(r'(^|/)(jest|mocha|pytest)(/|\.|\b)', f_lower):
        return True
    return False


# Language-aware test patterns in patch content (added lines only)
_TEST_CONTENT_PATTERNS: tuple[re.Pattern, ...] = (
    # Python
    re.compile(r"import\s+(?:unittest|pytest)"),
    re.compile(r"from\s+(?:unittest|pytest)\s+import"),
    re.compile(r"class\s+\w*Test\w*\(.*TestCase"),
    re.compile(r"def\s+test_\w+"),
    # JavaScript/TypeScript
    re.compile(r"(?:describe|it|test)\s*\("),
    re.compile(r"import\s+.*(?:from\s+['\"](?:jest|mocha|vitest|@testing-library))"),
    re.compile(r"require\s*\(\s*['\"](?:jest|mocha|chai|sinon)"),
    re.compile(r"expect\s*\(.*\)\.to(?:Be|Equal|Have|Match|Throw)"),
    # Java/Kotlin
    re.compile(r"@(?:Test|ParameterizedTest|RepeatedTest|BeforeEach|AfterEach)\b"),
    re.compile(r"import\s+(?:org\.junit|org\.testng)"),
    # Rust
    re.compile(r"#\[(?:test|cfg\(test\))\]"),
    re.compile(r"mod\s+tests\s*\{"),
    # Go
    re.compile(r"func\s+Test\w+\(t\s+\*testing\.T\)"),
    re.compile(r"import\s+\"testing\""),
    # Ruby
    re.compile(r"require\s+['\"](?:rspec|minitest|test/unit)"),
    re.compile(r"RSpec\.describe"),
    # C#
    re.compile(r"\[(?:Test|TestMethod|Fact|Theory)\]"),
)


def is_test_content(patch: str | None) -> bool:
    """Detect test code by analyzing added lines in a patch for test framework patterns.

    Complements is_test_file (path-based) with content-based detection.
    Only checks added lines (+) to avoid false positives from removed code.
    """
    if not patch:
        return False
    added_lines = [
        line[1:] for line in patch.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]
    if not added_lines:
        return False
    text = "\n".join(added_lines)
    return any(p.search(text) for p in _TEST_CONTENT_PATTERNS)


def is_test_file_or_content(filename: str, patch: str | None = None) -> bool:
    """Combined test detection: path heuristics + patch content analysis."""
    if is_test_file(filename):
        return True
    return is_test_content(patch)


def filter_prs_for_contribution(
    prs: list, exclude_drafts: bool = True, only_merged: bool = False
) -> list:
    """DRY filter for meaningful PR contributions (reviewed all metrics).

    - exclude_drafts: True for quality metrics (drafts incomplete/risky).
    - only_merged: True for cycle/merge stats (delivery focus).
    - Closed non-merged included if not only_merged (attempted contributions).
    - Assumes PR has .draft bool and .merged bool.
    """
    filtered = []
    for pr in prs:
        if exclude_drafts and getattr(pr, "draft", False):
            continue
        if only_merged and not getattr(pr, "merged", False):
            continue
        filtered.append(pr)
    return filtered


def compute_pr_body_quality(body: str | None) -> int:
    """Score PR body 0-100: length, markdown sections (>=1), issue/PR refs bonus."""
    if not body or not body.strip():
        return 0
    body = body.strip()
    score = 0
    length = len(body)
    if length >= 500:
        score += 40
    elif length >= 100:
        score += 25
    elif length >= 50:
        score += 10
    sections = len(re.findall(r"^#{1,4}\s+.+$", body, re.MULTILINE))
    if sections >= 2:
        score += 30
    elif sections >= 1:
        score += 20
    # Provider-neutral cross-reference bonus: #NNN, !NNN (GitLab MR),
    # AB#NNN (Azure DevOps work items).  Replaces English-only "fixes/closes".
    if re.search(r'(?<!\w)(?:AB)?[#!]\d{1,6}\b', body):
        score += 15
    # URL reference fallback (links to any tracker: Jira, Linear, etc.)
    elif re.search(r'https?://\S+', body):
        score += 10
    return min(score, 100)


# DRY dep file detector (for dep change rate; common across langs)
_DEP_BASENAMES: frozenset[str] = frozenset({
    "package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
    "requirements.txt", "requirements-dev.txt", "requirements-test.txt",
    "pipfile", "pipfile.lock", "poetry.lock", "pyproject.toml",
    "gemfile", "gemfile.lock",
    "cargo.toml", "cargo.lock",
    "pom.xml", "build.gradle", "build.gradle.kts",
    "go.mod", "go.sum",
    "composer.json", "composer.lock",
    "pubspec.yaml", "pubspec.lock",
})


def is_dependency_file(filename: str) -> bool:
    if not filename:
        return False
    basename = filename.lower().rsplit("/", 1)[-1]
    return basename in _DEP_BASENAMES


# DRY doc file detector (broad patterns for .md/.rst/docs/README etc across languages/repos)
_DOC_EXTENSIONS: frozenset[str] = frozenset({".md", ".rst", ".adoc"})
_DOC_BASENAMES: frozenset[str] = frozenset({
    "readme", "changelog", "contributing", "license", "notice",
    "authors", "faq", "glossary", "mkdocs.yml",
})
_DOC_DIRS_BUILTIN: tuple[str, ...] = ("docs/", "doc/", "documentation/", "wiki/", "guides/", "manual/")


def _get_doc_dirs() -> tuple[str, ...]:
    """Return the full doc-directory list (built-in + config extras)."""
    cfg = _load_customization()
    extras = cfg.get("extra_doc_dirs") or []
    if not extras:
        return _DOC_DIRS_BUILTIN
    return _DOC_DIRS_BUILTIN + tuple(d.lower() for d in extras)


def is_documentation_file(
    filename: str,
    all_filenames: list[str] | None = None,
) -> bool:
    """Detect documentation files by extension, basename, directory, or context.

    Layers (in priority order):
      1. Extension-based (``.md``, ``.rst``, ``.adoc``) — always language-neutral.
      2. Basename-based (``readme``, ``changelog``, etc.) — de-facto universal.
      3. Directory-based (``docs/``, ``doc/``, etc. + ``extra_doc_dirs`` config).
      4. Context-based fallback: if *all_filenames* is provided and the file's
         sibling directory contains >50% ``.md`` files, treat it as documentation.
    """
    if not filename:
        return False
    f_lower = filename.lower()
    basename = f_lower.rsplit("/", 1)[-1]
    # Layer 1: Extension-based (primary — fully language-neutral)
    dot_idx = basename.rfind(".")
    if dot_idx >= 0 and basename[dot_idx:] in _DOC_EXTENSIONS:
        return True
    # Layer 2: Basename-based (strip extension for comparison)
    name_no_ext = basename[:dot_idx] if dot_idx >= 0 else basename
    if name_no_ext in _DOC_BASENAMES or basename in _DOC_BASENAMES:
        return True
    # Layer 3: Path segment — file lives under a docs directory (built-in + config)
    doc_dirs = _get_doc_dirs()
    for d in doc_dirs:
        if f_lower.startswith(d) or ("/" + d) in f_lower:
            return True
    # Layer 4: Context-based fallback — sibling directory has >50% .md files
    if all_filenames and "/" in f_lower:
        parent_dir = f_lower.rsplit("/", 1)[0] + "/"
        siblings = [
            fn for fn in all_filenames
            if fn.lower().startswith(parent_dir)
            and "/" not in fn.lower()[len(parent_dir):]  # same level only
        ]
        if len(siblings) >= 2:
            md_count = sum(1 for s in siblings if s.lower().endswith(".md"))
            if md_count / len(siblings) > 0.5:
                return True
    return False


# ---------------------------------------------------------------------------
# Generated-file detection (lockfiles, minified, auto-generated, vendor, etc.)
# ---------------------------------------------------------------------------

# Exact basenames that are always generated/machine-managed
_GENERATED_BASENAMES: frozenset[str] = frozenset({
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
    "pipfile.lock", "poetry.lock", "cargo.lock",
    "gemfile.lock", "composer.lock", "pubspec.lock",
    "go.sum", "flake.lock",
    "shrinkwrap.yaml",
})

# Suffix patterns for generated/vendored/minified files
_GENERATED_SUFFIXES: tuple[str, ...] = (
    ".min.js", ".min.css", ".min.map",
    ".bundle.js", ".chunk.js",
    ".generated.ts", ".generated.go", ".generated.rs",
    ".pb.go", ".pb.cc", ".pb.h", ".pb.py",   # protobuf
    ".g.dart",                                 # Dart codegen
    "_generated.go",
    ".snap",                                   # Jest snapshots
    ".svg",                                    # typically exported from tools
)

# Directory segments that indicate vendored/generated trees
_GENERATED_DIRS: tuple[str, ...] = (
    "vendor/", "node_modules/", "dist/", "build/",
    "__generated__/", "generated/", ".gen/",
    "__snapshots__/",
    "migrations/",
)

# Header markers in patch content that signal auto-generated files.
# Ordered by language-neutrality: tool-convention markers first (@generated,
# @auto-generated), English phrases as secondary bonus layer.
_GENERATED_MARKERS_BUILTIN: tuple[str, ...] = (
    # Tool conventions (never localised — primary markers)
    "@generated",
    "@auto-generated",
    "@autogenerated",
    # English phrases (secondary — bonus layer for English codebases)
    "auto-generated",
    "automatically generated",
    "do not edit",
    "do not modify",
    "this file is generated",
    "code generated by",
    "generated by",
)


def _get_generated_markers() -> tuple[str, ...]:
    """Return the full generated-file marker list (built-in + config extras)."""
    cfg = _load_customization()
    extras = cfg.get("extra_generated_markers") or []
    if not extras:
        return _GENERATED_MARKERS_BUILTIN
    return _GENERATED_MARKERS_BUILTIN + tuple(m.lower() for m in extras)


def _shannon_entropy(data: bytes) -> float:
    """Shannon entropy in bits per byte (0-8). High entropy = random/minified."""
    if not data:
        return 0.0
    freq: dict[int, int] = {}
    for b in data:
        freq[b] = freq.get(b, 0) + 1
    length = len(data)
    entropy = 0.0
    for count in freq.values():
        p = count / length
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def _compression_ratio(data: bytes) -> float:
    """Ratio of compressed/original size. Low ratio = highly repetitive/generated."""
    if not data:
        return 1.0
    compressed = zlib.compress(data, level=6)
    return len(compressed) / len(data)


def is_generated_file(filename: str, patch: str | None = None) -> bool:
    """Detect generated/vendored/minified files by name patterns and patch content.

    Uses a layered approach:
    1. Exact basename match (lockfiles)
    2. Suffix patterns (minified, protobuf, snapshots)
    3. Directory segments (vendor, node_modules, dist)
    4. Patch header markers (@generated, DO NOT EDIT)
    5. Entropy + compression ratio on patch content (minified/machine output)
    """
    if not filename:
        return False
    f_lower = filename.lower()
    basename = f_lower.rsplit("/", 1)[-1]

    # 1. Exact basename
    if basename in _GENERATED_BASENAMES:
        return True

    # 2. Suffix patterns
    if any(f_lower.endswith(s) for s in _GENERATED_SUFFIXES):
        return True

    # 3. Directory segments
    if any(d in f_lower for d in _GENERATED_DIRS):
        return True

    # 4-5. Patch-content analysis (only if patch provided)
    if patch:
        # Extract added lines (content lines starting with +, not hunk headers)
        added_lines = [
            line[1:] for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]
        if added_lines:
            header_text = "\n".join(added_lines[:10]).lower()

            # 4. Header markers in first 10 added lines
            if any(marker in header_text for marker in _get_generated_markers()):
                return True

            # 5. Entropy + compression (only on substantial patches)
            content = "\n".join(added_lines)
            if len(content) >= 500:
                data = content.encode("utf-8", errors="replace")
                entropy = _shannon_entropy(data)
                ratio = _compression_ratio(data)
                # Minified JS/CSS: high entropy (>6.0) + poor compressibility
                # Normal code: entropy 3.5-5.5; minified/obfuscated: 6.0+
                # Machine-generated: moderate entropy + very low compression (<0.15)
                if entropy > 6.0 and ratio > 0.7:
                    return True  # minified (high entropy, hard to compress)
                if ratio < 0.15:
                    return True  # extremely repetitive generated code

    return False


# DRY conventional commit checker (industry best practices: type(scope)!: desc; types from conventionalcommits.org)

# Strict: only the 11 spec-defined English type tokens
_CC_STRICT_RE = re.compile(
    r'^(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)'
    r'(?:\(.+?\))?!?:\s', re.IGNORECASE,
)
# Broad: any single-word type + optional scope + colon (covers non-English types
# like 機能:, fonctionnalite:, etc.)
_CC_BROAD_RE = re.compile(r'^[a-zA-Z\u00C0-\u024F\u3000-\u9FFF]+(?:\(.+?\))?!?:\s')


def is_conventional_commit(message: str) -> bool:
    """Strict check: message matches one of the 11 Conventional Commits spec types."""
    if not message:
        return False
    return bool(_CC_STRICT_RE.match(message.strip()))


def is_structured_commit(message: str) -> bool:
    """Broad check: any ``word(scope?):`` prefix, including non-English types.

    This catches teams using non-English type prefixes (e.g., ``機能:``, ``修正:``,
    ``fonctionnalite:``) that follow the structured ``type: description`` pattern
    without being one of the 11 English spec types.
    """
    if not message:
        return False
    return bool(_CC_BROAD_RE.match(message.strip()))


# Commit message defect mining: regex patterns for defect-related changes
_DEFECT_PATTERNS: list[re.Pattern] = [
    # Core defect terms
    re.compile(r"\bfix(?:es|ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bbug(?:s|fix|fixed|fixes)?\b", re.IGNORECASE),
    re.compile(r"\bhotfix(?:es)?\b", re.IGNORECASE),
    re.compile(r"\bdefect(?:s)?\b", re.IGNORECASE),
    re.compile(r"\bissue(?:s)?\b", re.IGNORECASE),
    # Stability/resolution language
    re.compile(r"\bresolve(?:s|d)?\b", re.IGNORECASE),
    re.compile(r"\bpatch(?:es|ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bregression(?:s)?\b", re.IGNORECASE),
    re.compile(r"\bcrash(?:es|ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bflak(?:y|iness)?\b", re.IGNORECASE),
    # Production/support language
    re.compile(r"\bincident(?:s)?\b", re.IGNORECASE),
    re.compile(r"\boutage(?:s)?\b", re.IGNORECASE),
    re.compile(r"\brollback(?:s)?\b", re.IGNORECASE),
    re.compile(r"\bmitigate(?:s|d|ing)?\b", re.IGNORECASE),
    re.compile(r"\btriage(?:s|d|ing)?\b", re.IGNORECASE),
    re.compile(r"\bescalat(?:e|es|ed|ing)\b", re.IGNORECASE),
    # Error or failure signatures
    re.compile(r"\berror(?:s)?\b", re.IGNORECASE),
    re.compile(r"\bfail(?:s|ed|ing|ure|ures)?\b", re.IGNORECASE),
    re.compile(r"\bpanic(?:s|ked|king)?\b", re.IGNORECASE),
    re.compile(r"\bexception(?:s)?\b", re.IGNORECASE),
]


# Built-in defect label set (same infrastructure as bug labels)
_DEFECT_LABELS_BUILTIN: frozenset[str] = frozenset({
    "bug", "defect", "type:bug", "kind/bug", "incident",
    "hotfix", "regression", "type:defect",
})


def _get_defect_labels() -> frozenset[str]:
    """Return the full defect label set (built-in + config extras)."""
    cfg = _load_customization()
    extras = cfg.get("extra_defect_labels") or []
    if not extras:
        return _DEFECT_LABELS_BUILTIN
    return _DEFECT_LABELS_BUILTIN | frozenset(l.lower() for l in extras)


def classify_defect_commit(message: str, labels: list[str] | None = None) -> bool:
    """Return True if commit is defect-related — labels-first, then English regex.

    Priority:
      1. PR labels (language-neutral, team-applied ground truth)
      2. English keyword regex (backward-compatible fallback)
    """
    # Priority 1: labels
    if labels:
        defect_labels = _get_defect_labels()
        if any(l.lower() in defect_labels for l in labels):
            return True
    # Priority 2: English regex fallback
    if not message:
        return False
    normalized = message.strip()
    return any(pattern.search(normalized) for pattern in _DEFECT_PATTERNS)


def compute_code_churn(
    ledger, user_login: str, prs: list, window_days: int = 30,
    *, start_date=None, end_date=None,
) -> dict:
    """Code churn %: lines modifying own prior code (<=30d; line-level; weekly for spikes/period).

    Uses line-level overlap (like rework_rate) instead of file-level to avoid
    inflating churn for frequently-modified files. Excludes generated files.
    """
    # Build file history: file -> list of (date, changed_line_numbers) for author's prior PRs
    file_history: dict[str, list[tuple]] = defaultdict(list)
    for upr in ledger.get_prs_for_user(user_login):
        for f in ledger.get_files_for_pr(upr.number):
            if is_generated_file(f.filename, f.patch):
                continue
            if f.patch:
                changed = _parse_hunk_lines(f.patch, "+")
                file_history[f.filename].append((upr.created_at, changed))
            else:
                # No patch data — record empty set (file was touched but no line info)
                file_history[f.filename].append((upr.created_at, set()))

    churned_lines = 0
    total_lines = 0
    per_pr = []
    # Weekly bins for period-aware spikes/freq (short: spike bad; long: steady high bad)
    weekly_churn: dict[tuple, dict] = defaultdict(lambda: {"churn_lines": 0, "total_lines": 0})
    for pr in prs:
        pr_date = pr.created_at
        pr_churn = 0
        pr_total = 0
        for f in ledger.get_files_for_pr(pr.number):
            if is_generated_file(f.filename, f.patch):
                continue
            # Parse current PR's changed lines
            this_changed = _parse_hunk_lines(f.patch, "+") if f.patch else set()
            line_count = len(this_changed) or f.changes  # fallback when no patch
            pr_total += line_count
            # Find prior author changes in the same file within window
            prior = [
                (t, ch) for t, ch in file_history.get(f.filename, [])
                if t < pr_date and (pr_date - t).days <= window_days
            ]
            if prior:
                if this_changed:
                    # Line-level: count overlapping line numbers
                    prior_lines: set[int] = set()
                    for _, prev_ch in prior:
                        prior_lines |= prev_ch
                    overlap = len(this_changed & prior_lines)
                    pr_churn += overlap
                else:
                    # No patch data — fall back to file-level (all lines count)
                    pr_churn += f.changes
        total_lines += pr_total
        churned_lines += pr_churn
        rate = (pr_churn / pr_total * 100) if pr_total else 0.0
        per_pr.append(
            {"number": pr.number, "churn_lines": pr_churn, "total_lines": pr_total, "churn_rate": rate}
        )
        # Bin to week
        if pr_date:
            wk = pr_date.isocalendar()[:2]
            weekly_churn[wk]["churn_lines"] += pr_churn
            weekly_churn[wk]["total_lines"] += pr_total
    rate = (churned_lines / total_lines * 100) if total_lines else 0.0
    # Weekly rates + stats (max=spike detect; use get_weekly... style)
    weekly_rates = []
    for wk, data in sorted(weekly_churn.items()):
        w_rate = (data["churn_lines"] / data["total_lines"] * 100) if data["total_lines"] else 0.0
        weekly_rates.append(w_rate)
    max_weekly = max(weekly_rates) if weekly_rates else 0.0
    # Use actual analysis period if available, else fall back to PR span
    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 30
    else:
        period_days = 30
    churn_per_week = (churned_lines / max(1, period_days / 7.0))
    result = {
        "churn_rate": rate,
        "churned_lines": churned_lines,
        "total_lines": total_lines,
        "per_pr": per_pr,
        "window_days": window_days,
        "pr_count": len(prs),
        "max_weekly_churn": max_weekly,
        "weekly_rates": weekly_rates,
        "churn_per_week": churn_per_week,
        "period_days": period_days,
    }
    if not prs or (period_days < 14 and len(prs) < 3):
        result["no_data"] = True
    return result


_HUNK_RE = re.compile(
    r"@@\s+-(?P<old_start>\d+)(?:,(?P<old_count>\d+))?"
    r"\s+\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))?\s+@@"
    r"(?:\s+(?P<context>.+))?"
)


def _parse_hunk_lines(patch: str | None, side: str = "-") -> set[int]:
    """Parse unified diff for actually changed line numbers (side='-' for old, '+' for new).

    Walks diff lines individually to count only real additions/removals,
    NOT context lines (which the hunk header counts include).
    """
    if not patch:
        return set()
    lines: set[int] = set()
    old_line = 0
    new_line = 0
    for raw_line in patch.splitlines():
        if raw_line.startswith("@@"):
            match = _HUNK_RE.search(raw_line)
            if match:
                old_line = int(match.group("old_start"))
                new_line = int(match.group("new_start"))
        elif raw_line.startswith("+++") or raw_line.startswith("---"):
            continue  # file header lines
        elif raw_line.startswith("\\"):
            continue  # "\ No newline at end of file" marker — not a content line
        elif raw_line.startswith("+"):
            if side == "+":
                lines.add(new_line)
            new_line += 1
        elif raw_line.startswith("-"):
            if side == "-":
                lines.add(old_line)
            old_line += 1
        else:
            # Context line — advances both counters
            old_line += 1
            new_line += 1
    return lines


class HunkInfo(TypedDict):
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    function_context: str | None
    added_lines: int
    removed_lines: int


def parse_hunks(patch: str | None) -> list[HunkInfo]:
    """Parse unified diff into structured hunk information including function context.

    The function_context field extracts the text after the closing @@, which
    git/GitHub populate with the enclosing function/class name (when available).
    This enables function-level churn and rework tracking without full AST parsing.
    """
    if not patch:
        return []
    hunks: list[HunkInfo] = []
    current_added = 0
    current_removed = 0

    for line in patch.splitlines():
        if line.startswith("@@"):
            # Flush previous hunk's line counts
            if hunks:
                hunks[-1]["added_lines"] = current_added
                hunks[-1]["removed_lines"] = current_removed
                current_added = 0
                current_removed = 0

            match = _HUNK_RE.search(line)
            if match:
                ctx = match.group("context")
                hunks.append(HunkInfo(
                    old_start=int(match.group("old_start")),
                    old_count=int(match.group("old_count") or 1),
                    new_start=int(match.group("new_start")),
                    new_count=int(match.group("new_count") or 1),
                    function_context=ctx.strip() if ctx else None,
                    added_lines=0,
                    removed_lines=0,
                ))
        elif hunks:
            if line.startswith("+") and not line.startswith("+++"):
                current_added += 1
            elif line.startswith("-") and not line.startswith("---"):
                current_removed += 1

    # Flush last hunk
    if hunks:
        hunks[-1]["added_lines"] = current_added
        hunks[-1]["removed_lines"] = current_removed

    return hunks


def get_function_churn_map(ledger, prs: list) -> dict[str, dict]:
    """Build a function-level churn map: {file::function -> {churn_count, total_lines, prs}}.

    Uses hunk context from diffs to track how often the same function is modified.
    High churn on a single function signals instability or rework.
    """
    func_map: dict[str, dict] = defaultdict(lambda: {"churn_count": 0, "total_lines": 0, "prs": []})

    for pr in prs:
        for f in ledger.get_files_for_pr(pr.number):
            if not f.patch:
                continue
            hunks = parse_hunks(f.patch)
            for h in hunks:
                ctx = h["function_context"]
                if not ctx:
                    continue
                key = f"{f.filename}::{ctx}"
                func_map[key]["churn_count"] += 1
                func_map[key]["total_lines"] += h["added_lines"] + h["removed_lines"]
                if pr.number not in func_map[key]["prs"]:
                    func_map[key]["prs"].append(pr.number)

    return dict(func_map)


# ---------------------------------------------------------------------------
# Structural diff classification (Phase 1e)
# ---------------------------------------------------------------------------

# Patterns matched against added lines to classify what a diff structurally does.
# Covers Python, JS/TS, Java/Kotlin, Rust, Go, C/C++, C#, Ruby, PHP, Swift,
# Scala and more.  Cross-language keywords (class, import, if) naturally cover
# many languages beyond those explicitly listed.
_STRUCT_PATTERNS: dict[str, list[re.Pattern]] = {
    # Order matters — more specific patterns must come before general ones.
    "test_code": [
        # Python def test_, JS/TS describe/it/test, Java/Kotlin @Test,
        # Rust #[test], Go func Test*, C# [TestMethod]/[Fact]/[Theory],
        # Ruby RSpec describe/context/it, PHP @test / function test*
        re.compile(r"^\s*(?:def\s+test_|(?:it|test|describe|context)(?:\.each)?\s*\(|@Test\b|#\[test\]|func\s+Test\w+)"),
        re.compile(r"^\s*\[(?:TestMethod|Fact|Theory|TestCase)\]"),  # C#/NUnit
        re.compile(r"^\s*(?:public|protected|private)?\s*function\s+test\w+"),  # PHP
    ],
    "new_class": [
        re.compile(r"^\s*(?:export\s+)?(?:default\s+)?(?:abstract\s+)?class\s+\w+"),
        re.compile(r"^\s*(?:interface|struct|enum|type)\s+\w+"),
        # Ruby module, Scala object/trait, Swift protocol, Rust trait
        re.compile(r"^\s*(?:module|object|trait|protocol)\s+\w+"),
    ],
    "new_function": [
        # Python def, JS function, Go func, Rust fn/pub fn, Kotlin fun,
        # PHP function, Swift func, C/C++/Java return-type patterns
        re.compile(r"^\s*(?:def|function|func|fn|pub\s+fn|fun|sub|async\s+function)\s+\w+"),
        re.compile(r"^\s*(?:export\s+)?(?:default\s+)?function\s+\w+"),
        re.compile(r"^\s*const\s+\w+\s*=\s*(?:\(|async)"),
        # C/C++/C#/Java: return-type + name + parens (common pattern)
        re.compile(r"^\s*(?:public|private|protected|internal|static|virtual|override|async|inline)\s+\w+\s+\w+\s*\("),
    ],
    "conditional_change": [
        re.compile(r"^\s*(?:if|else|elif|else\s+if|switch|case|guard|when|match|unless)\b"),
    ],
    "import_change": [
        re.compile(r"^\s*(?:import|from\s+\S+\s+import|require\s*\(|use\s+)"),
        re.compile(r"^\s*(?:export\s+\{|module\.exports)"),
        # C/C++ #include, C# using, Ruby require/require_relative
        re.compile(r"^\s*#\s*include\s+[<\"]"),
        re.compile(r"^\s*using\s+(?:static\s+)?\w+"),
        re.compile(r"^\s*require(?:_relative)?\s+"),
    ],
    "error_handling": [
        re.compile(r"^\s*(?:try|catch|except|finally|rescue|throw|raise|panic|defer)\b"),
    ],
}


def classify_diff_structure(patch: str | None) -> dict[str, int]:
    """Classify what a diff structurally does by analyzing added lines.

    Returns a dict of {category: line_count} for each structural category detected.
    Categories: new_function, new_class, conditional_change, import_change,
    error_handling, test_code, whitespace_only, other.
    """
    result: dict[str, int] = defaultdict(int)
    if not patch:
        return dict(result)

    added_lines = [
        line[1:] for line in patch.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]
    removed_lines = [
        line[1:] for line in patch.splitlines()
        if line.startswith("-") and not line.startswith("---")
    ]

    if not added_lines and not removed_lines:
        return dict(result)

    # Check for whitespace-only changes
    non_ws_added = [l for l in added_lines if l.strip()]
    non_ws_removed = [l for l in removed_lines if l.strip()]
    if not non_ws_added and not non_ws_removed:
        result["whitespace_only"] = len(added_lines) + len(removed_lines)
        return dict(result)

    # Classify each added line
    for line in added_lines:
        stripped = line.rstrip()
        if not stripped.strip():
            continue
        classified = False
        for category, patterns in _STRUCT_PATTERNS.items():
            if any(p.match(stripped) for p in patterns):
                result[category] += 1
                classified = True
                break
        if not classified:
            result["other"] += 1

    return dict(result)


def classify_pr_by_diff(ledger, pr) -> str:
    """Classify a PR's primary intent by analyzing all file diffs structurally.

    Returns the dominant structural category. Falls back to "other" if
    no clear pattern emerges.
    """
    combined: dict[str, int] = defaultdict(int)
    for f in ledger.get_files_for_pr(pr.number):
        if is_generated_file(f.filename, f.patch):
            continue
        struct = classify_diff_structure(f.patch)
        for cat, count in struct.items():
            combined[cat] += count

    if not combined:
        return "other"

    # Return dominant category
    return max(combined, key=combined.get)  # type: ignore[arg-type]


def compute_rework_rate(ledger, user_login: str, prs: list, window_days: int = 21, *, start_date=None, end_date=None) -> dict:
    """% lines changed that overlap author's prior 21d changes (self-rework on recent code).
    Uses patch hunks for line ranges; falls back if period short/no patches.
    """
    if not prs:
        return {
            "rework_rate": 0.0,
            "reworked_lines": 0,
            "total_changed": 0,
            "per_pr": [],
            "window_days": window_days,
            "pr_count": 0,
            "period_days": 0,
            "no_data": True,
        }
    # File history: file -> list of (date, changed_lines_set) for author's prior PRs
    file_history = defaultdict(list)
    all_user_prs = ledger.get_prs_for_user(user_login)  # all time for history
    for upr in all_user_prs:
        for f in ledger.get_files_for_pr(upr.number):
            if f.patch:
                changed = _parse_hunk_lines(f.patch, "+")  # new lines written
                file_history[f.filename].append((upr.created_at, changed))
    # For target PRs in window
    reworked_lines = 0
    total_changed = 0
    per_pr = []
    for pr in prs:
        pr_date = pr.created_at
        pr_rework = 0
        pr_total = 0
        for f in ledger.get_files_for_pr(pr.number):
            if not f.patch:
                continue
            this_changed = _parse_hunk_lines(f.patch, "+")
            pr_total += len(this_changed) or f.changes  # fallback
            # Prior author changes in window
            prior = [
                (t, ch) for t, ch in file_history.get(f.filename, [])
                if t < pr_date and (pr_date - t).days <= window_days
            ]
            if prior:
                # Union all prior lines (no double-count; accumulate unique overlaps)
                prior_lines = set()
                for _, prev in prior:
                    prior_lines |= prev
                overlap = len(this_changed & prior_lines)
                pr_rework += overlap
        total_changed += pr_total
        reworked_lines += pr_rework
        rate = (pr_rework / pr_total * 100) if pr_total else 0.0
        per_pr.append({
            "number": pr.number,
            "rework_lines": pr_rework,
            "total_lines": pr_total,
            "rework_rate": rate,
        })
    rate = (reworked_lines / total_changed * 100) if total_changed else 0.0
    # Use actual analysis period if available, else fall back to PR span
    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 0
    else:
        period_days = 0
    no_patches = total_changed == 0
    no_data = (period_days < window_days and len(prs) < 3) or no_patches
    no_data_reason = None
    if no_patches:
        no_data_reason = "no patch data available for line-level analysis"
    elif period_days < window_days and len(prs) < 3:
        no_data_reason = f"period ({period_days:.0f}d) shorter than lookback window ({window_days}d)"
    return {
        "rework_rate": rate,
        "reworked_lines": reworked_lines,
        "total_changed": total_changed,
        "per_pr": per_pr,
        "window_days": window_days,
        "pr_count": len(prs),
        "period_days": period_days,
        "no_data": no_data,
        "no_data_reason": no_data_reason,
    }


def compute_self_merge_rate(ledger, user_login: str, *, start_date=None, end_date=None) -> dict:
    """% PRs (own+others) merged by author w/o approval; repo rate for culture check.

    Filters to PRs merged within [start_date, end_date] so the result is
    period-sensitive (a sprint view and a yearly view return different numbers).
    """
    all_prs = ledger.bundle.pull_requests
    # Period filter on merged_at
    if start_date:
        all_prs = [p for p in all_prs if p.merged_at and p.merged_at >= start_date]
    if end_date:
        all_prs = [p for p in all_prs if p.merged_at and p.merged_at <= end_date]
    # Engineer merges (by user, any PR)
    eng_merged = [
        pr for pr in all_prs
        if pr.merged and pr.merged_by and pr.merged_by.login == user_login
    ]
    no_approval_count = 0
    per_pr = []
    for pr in eng_merged:
        # Approval = any APPROVED review from OTHER users before/at merge
        # Self-reviews don't count as valid approval (masks self-merge behavior)
        reviews = ledger.get_reviews_for_pr(pr.number)
        has_approval = any(
            r.state == ReviewState.APPROVED
            for r in reviews
            if r.user.login != pr.user.login
            and (not pr.merged_at or r.submitted_at <= pr.merged_at)
        )
        if not has_approval:
            no_approval_count += 1
        per_pr.append({
            "number": pr.number,
            "merged_by_author": True,
            "has_approval": has_approval,
            "is_own_pr": pr.user.login == user_login,
        })
    eng_rate = (no_approval_count / len(eng_merged) * 100) if eng_merged else 0.0
    # Repo culture: self-merge % across all merged PRs (low=review norm)
    all_merged = [pr for pr in all_prs if pr.merged and pr.merged_by]
    repo_self = sum(1 for pr in all_merged if pr.merged_by.login == pr.user.login)
    repo_rate = (repo_self / len(all_merged) * 100) if all_merged else 0.0
    return {
        "self_merge_rate": eng_rate,
        "no_approval_count": no_approval_count,
        "engineer_merged_count": len(eng_merged),
        "repo_self_merge_rate": repo_rate,
        "repo_merged_count": len(all_merged),
        "per_pr": per_pr,
    }


def is_no_data(details: dict[str, Any]) -> bool:
    """Central no-data artifact (guards zero-activity; covers no_data/no_cr_activity)."""
    return bool(details.get("no_data") or details.get("no_cr_activity"))


# ---------------------------------------------------------------------------
# Pygments-based code block analysis for review comment scoring
# ---------------------------------------------------------------------------

_FENCED_CODE_RE = re.compile(
    r"```(\w+)?\s*\n(.*?)^```",
    re.DOTALL | re.MULTILINE,
)

# Map common markdown fence language tags to Pygments lexer names
_LANG_ALIASES: dict[str, str] = {
    "py": "python", "python3": "python", "python2": "python",
    "js": "javascript", "jsx": "javascript",
    "ts": "typescript", "tsx": "typescript",
    "rb": "ruby", "rs": "rust", "kt": "kotlin",
    "cs": "csharp", "c#": "csharp",
    "cpp": "cpp", "c++": "cpp", "cc": "cpp",
    "sh": "bash", "shell": "bash", "zsh": "bash",
    "yml": "yaml", "dockerfile": "docker",
    "tf": "terraform", "hcl": "terraform",
}


def extract_code_blocks(text: str) -> list[tuple[str | None, str]]:
    """Extract fenced code blocks from markdown text.

    Returns list of (language_hint, code_content) tuples.
    """
    if not text:
        return []
    return [
        (m.group(1) if m.group(1) else None, m.group(2).strip())
        for m in _FENCED_CODE_RE.finditer(text)
        if m.group(2).strip()
    ]


def score_code_block_quality(code: str, language_hint: str | None = None) -> float:
    """Score a code block 0.0-1.0 based on Pygments lexer analysis.

    Uses token type distribution to assess code sophistication:
    - Real code has keywords, names, operators, literals
    - Plain text or trivial content scores low
    - Returns 0.0 for empty/unparseable content
    """
    from pygments import token as T
    from pygments.lexers import get_lexer_by_name, guess_lexer
    from pygments.util import ClassNotFound

    if not code or not code.strip():
        return 0.0

    # Resolve lexer
    lexer = None
    if language_hint:
        canonical = _LANG_ALIASES.get(language_hint.lower(), language_hint.lower())
        try:
            lexer = get_lexer_by_name(canonical)
        except ClassNotFound:
            pass

    if lexer is None:
        try:
            lexer = guess_lexer(code)
        except ClassNotFound:
            return 0.1  # can't even guess — minimal credit

    # Lex and classify tokens
    tokens = list(lexer.get_tokens(code))
    if not tokens:
        return 0.1

    meaningful = 0  # keywords, names, operators, literals
    structural = 0  # function/class defs, return, imports
    total = 0

    for tok_type, tok_value in tokens:
        stripped = tok_value.strip()
        if not stripped:
            continue
        total += 1

        if tok_type in T.Keyword or tok_type in T.Keyword.Declaration:
            meaningful += 1
            if stripped in ("def", "function", "func", "fn", "class", "import", "return",
                            "async", "await", "export", "interface", "struct"):
                structural += 1
        elif tok_type in T.Name or tok_type in T.Name.Function or tok_type in T.Name.Class:
            meaningful += 1
        elif tok_type in T.Operator or tok_type in T.Punctuation:
            meaningful += 1
        elif tok_type in T.Literal or tok_type in T.String or tok_type in T.Number:
            meaningful += 1

    if total == 0:
        return 0.1

    meaningful_ratio = meaningful / total
    lines = [l for l in code.splitlines() if l.strip()]
    line_count = len(lines)

    # Graduated scoring:
    # - base: 0.2 (has code block at all)
    # - +0.2 if meaningful tokens > 40% of total (real code, not plain text)
    # - +0.2 if multi-line (>= 3 lines — shows effort)
    # - +0.2 if has structural tokens (function/class/import definitions)
    # - +0.2 if token count > 10 (non-trivial length)
    score = 0.2
    if meaningful_ratio > 0.4:
        score += 0.2
    if line_count >= 3:
        score += 0.2
    if structural > 0:
        score += 0.2
    if total > 10:
        score += 0.2

    return score


def score_comment_code_quality(body: str) -> int:
    """Score the code quality aspect of a review comment (0-25).

    Replaces the flat 25-point "has code block" heuristic with
    graduated Pygments-based analysis.
    """
    if not body:
        return 0

    blocks = extract_code_blocks(body)
    if not blocks:
        # Inline backticks only
        if "`" in body:
            return 10
        return 0

    # Score each block, take the best one
    best = max(score_code_block_quality(code, lang) for lang, code in blocks)

    # Map 0.0-1.0 quality to 10-25 point range
    # Floor of 10 (having any code block is worth at least as much as inline backticks)
    return round(10 + best * 15)


# ---------------------------------------------------------------------------
# Manifest-aware module detection for breadth analysis
# ---------------------------------------------------------------------------

# Directories that typically contain independent sub-packages in monorepos
_WORKSPACE_DIRS: frozenset[str] = frozenset({
    "packages", "plugins", "apps", "services", "modules", "libs",
    "crates", "workspaces", "projects",
})

# Files that indicate a package/module root when present in a directory
_MANIFEST_FILENAMES: frozenset[str] = frozenset({
    "package.json", "pyproject.toml", "setup.py", "setup.cfg",
    "cargo.toml", "go.mod", "pom.xml", "build.gradle", "build.gradle.kts",
    "gemfile", "composer.json", "pubspec.yaml", "mix.exs",
    "build.sbt", "project.clj", "dune-project",
})


def detect_module_boundary(filepath: str) -> str:
    """Detect the logical module boundary for a file path.

    Uses workspace directory patterns (packages/, plugins/, services/, etc.)
    to identify real package boundaries in monorepo structures, instead of
    naively truncating to the first directory level.

    Args:
        filepath: Relative file path (e.g., "frontend/packages/core/src/utils.ts")

    Returns:
        Module path (e.g., "frontend/packages/core" instead of just "frontend")
    """
    if not filepath:
        return "unknown"

    parts = filepath.split("/")
    parts = [p for p in parts if p]
    if not parts:
        return "root"

    if len(parts) == 1:
        return "root"  # root-level file (not its own module)

    # Check if the file IS a manifest (its directory is a module root)
    if parts[-1].lower() in _MANIFEST_FILENAMES:
        if len(parts) >= 2:
            return "/".join(parts[:-1])
        return parts[0]

    # Walk path segments looking for workspace directory boundaries
    for i, part in enumerate(parts[:-1]):
        if part.lower() in _WORKSPACE_DIRS and i + 1 < len(parts) - 1:
            # This is a workspace dir — the module is parent + workspace + child
            return "/".join(parts[: i + 2])

    # Fallback: top-level directory
    return parts[0]


def build_module_map(filepaths: list[str]) -> dict[str, str]:
    """Build a mapping of file paths to their detected modules.

    Also enriches detection by recognizing manifest files that confirm
    module boundaries in the file set.

    Args:
        filepaths: List of all file paths in the changeset.

    Returns:
        Dict mapping each filepath to its module name.
    """
    # Phase 1: detect manifest-confirmed module roots from the file set
    confirmed_roots: set[str] = set()
    for fp in filepaths:
        parts = fp.split("/")
        if parts and parts[-1].lower() in _MANIFEST_FILENAMES and len(parts) >= 2:
            confirmed_roots.add("/".join(parts[:-1]))

    # Phase 2: map each file to its module
    result: dict[str, str] = {}
    for fp in filepaths:
        # First check if file falls under a confirmed manifest root
        module = None
        for root in sorted(confirmed_roots, key=len, reverse=True):
            if fp.startswith(root + "/") or fp == root:
                module = root
                break

        if module is None:
            module = detect_module_boundary(fp)

        result[fp] = module

    return result


# ---------------------------------------------------------------------------
# tree-sitter AST analysis for function identity + trivial detection
# ---------------------------------------------------------------------------

class FunctionInfo(TypedDict):
    name: str
    kind: str  # "function", "method", "constructor"
    start_line: int
    end_line: int
    parameter_count: int
    body_statement_count: int
    is_trivial: bool


# Extension → (module_import, language_factory) for lazy loading
_LANGUAGE_REGISTRY: dict[str, tuple[str, str]] = {
    ".py": ("tree_sitter_python", "language"),
    ".js": ("tree_sitter_javascript", "language"),
    ".jsx": ("tree_sitter_javascript", "language"),
    ".ts": ("tree_sitter_typescript", "language_typescript"),
    ".tsx": ("tree_sitter_typescript", "language_tsx"),
    ".go": ("tree_sitter_go", "language"),
    ".rs": ("tree_sitter_rust", "language"),
    ".java": ("tree_sitter_java", "language"),
    # Phase 3 expansion (TODO §3.1): ~90% GitHub repo coverage by language.
    # Each grammar is an optional dependency; _get_language() returns None
    # when the pip package is not installed (graceful degradation).
    ".c": ("tree_sitter_c", "language"),
    ".h": ("tree_sitter_c", "language"),
    ".cc": ("tree_sitter_cpp", "language"),
    ".cpp": ("tree_sitter_cpp", "language"),
    ".cxx": ("tree_sitter_cpp", "language"),
    ".hpp": ("tree_sitter_cpp", "language"),
    ".cs": ("tree_sitter_c_sharp", "language"),
    ".rb": ("tree_sitter_ruby", "language"),
    ".php": ("tree_sitter_php", "language_php"),
    ".kt": ("tree_sitter_kotlin", "language"),
    ".kts": ("tree_sitter_kotlin", "language"),
    ".swift": ("tree_sitter_swift", "language"),
    ".scala": ("tree_sitter_scala", "language"),
}

# Node types that represent function/method definitions per language
_FUNCTION_NODE_TYPES: frozenset[str] = frozenset({
    # Python, C, C++, PHP, Scala
    "function_definition",
    # JavaScript/TypeScript, Kotlin, Swift
    "function_declaration",
    # JavaScript/TypeScript
    "method_definition", "arrow_function",
    # Go, Java, C#, PHP
    "method_declaration",
    # Java, C#, Kotlin
    "constructor_declaration",
    # Rust
    "function_item",
    # Ruby
    "method", "singleton_method",
    # Swift
    "init_declaration",
})

# Node types that count as body statements
_STATEMENT_TYPES: frozenset[str] = frozenset({
    "expression_statement", "return_statement", "if_statement",
    "for_statement", "while_statement", "try_statement",
    "with_statement", "assert_statement", "raise_statement",
    "delete_statement", "assignment", "augmented_assignment",
    # JS/TS
    "variable_declaration", "lexical_declaration", "throw_statement",
    "switch_statement",
    # Go
    "short_var_declaration", "assignment_statement", "go_statement",
    "defer_statement", "select_statement", "var_declaration",
    # Rust
    "let_declaration", "macro_invocation",
    # Java
    "local_variable_declaration", "enhanced_for_statement",
    # C/C++ (§3.1 expansion)
    "declaration",          # C/C++ variable and type declarations
    "do_statement",         # C/C++/C#/PHP do-while
    # C# (§3.1 expansion)
    "foreach_statement",    # C#/PHP foreach
    "using_statement",      # C# using blocks
    "lock_statement",       # C# lock blocks
    # Ruby (§3.1 expansion)
    "call",                 # Ruby method calls as statements
    # PHP (§3.1 expansion)
    "echo_statement",       # PHP echo
    # Kotlin (§3.1 expansion)
    "property_declaration", # Kotlin val/var
    "return_expression",    # Kotlin return (expression, not statement)
    # Swift (§3.1 expansion)
    "guard_statement",      # Swift guard-let
    "for_in_statement",     # Swift for-in
    "control_transfer_statement",  # Swift return/break/continue
    # Scala (§3.1 expansion)
    "val_definition",       # Scala val
    "var_definition",       # Scala var
    "match_expression",     # Scala pattern matching
    "call_expression",      # Scala/Kotlin function calls as statements
})

# Cache parsed languages to avoid repeated imports
_language_cache: dict[str, Any] = {}


def _get_language(ext: str) -> Any:
    """Get a tree-sitter Language object for a file extension."""
    from importlib import import_module

    from tree_sitter import Language

    if ext in _language_cache:
        return _language_cache[ext]

    entry = _LANGUAGE_REGISTRY.get(ext)
    if not entry:
        return None

    mod_name, factory_name = entry
    try:
        mod = import_module(mod_name)
        factory = getattr(mod, factory_name)
        lang = Language(factory())
        _language_cache[ext] = lang
        return lang
    except (ImportError, AttributeError):
        return None


def _count_body_statements(node: Any) -> int:
    """Count meaningful statements in a function body node."""
    count = 0
    for child in node.children:
        if child.type in _STATEMENT_TYPES:
            count += 1
        elif child.type in _BLOCK_NODE_TYPES:
            count += _count_body_statements(child)
    return count


# Node types that represent block containers (recursed into by _count_body_statements)
_BLOCK_NODE_TYPES: frozenset[str] = frozenset({
    "block", "statement_block", "statement_list",  # Python/JS/TS/Go/Rust/Java/Scala
    "compound_statement",  # C/C++/PHP
    "body_statement",      # Ruby
    "function_body",       # Kotlin/Swift
    "statements",          # Swift (inside function_body)
    "declaration_list",    # C#
})


def _get_parameter_count(node: Any) -> int:
    """Extract parameter count from a function definition node."""
    params = node.child_by_field_name("parameters")
    if not params:
        # Try alternate field names used by different grammars
        for alt in ("parameter_list", "formal_parameters",
                    "function_value_parameters", "method_parameters",
                    "class_parameters"):
            params = node.child_by_field_name(alt)
            if params:
                break
    if not params:
        # C/C++: parameters live inside the declarator node
        declarator = node.child_by_field_name("declarator")
        if declarator:
            params = declarator.child_by_field_name("parameters")
    if not params:
        # Kotlin/Swift/etc.: param list may be a child node, not a named field
        _PARAM_NODE_TYPES = {"parameters", "parameter_list", "formal_parameters",
                             "function_value_parameters", "method_parameters",
                             "class_parameters"}
        for child in node.children:
            if child.type in _PARAM_NODE_TYPES:
                params = child
                break
    if not params:
        # Swift: parameters are direct children of the function node (no wrapper)
        return sum(
            1 for c in node.children
            if c.type in ("parameter", "simple_parameter", "class_parameter")
        )
    # Count actual parameter nodes (skip punctuation like commas/parens)
    return sum(
        1 for c in params.children
        if c.type not in ("(", ")", ",", "comment", "&", "*")
        and not c.type.startswith("//")
    )


def _classify_function_kind(node: Any) -> str:
    """Classify a function node as function, method, or constructor."""
    # AST node type is the most reliable signal (Java, C#, Kotlin)
    if node.type == "constructor_declaration":
        return "constructor"
    # Swift initialiser
    if node.type == "init_declaration":
        return "constructor"
    if node.type in ("method_definition", "method_declaration"):
        name_node = node.child_by_field_name("name")
        if name_node and name_node.text:
            name = name_node.text.decode("utf-8", errors="replace")
            # Explicit constructor names: Python (__init__), JS/TS (constructor),
            # Swift (init), Rust convention (new), PHP (__construct)
            if name in ("__init__", "constructor", "init", "new", "__construct"):
                return "constructor"
            # Go convention: NewFoo() is a constructor
            if re.match(r'^New[A-Z]', name):
                return "constructor"
        return "method"
    # Ruby: method and singleton_method are always inside a class
    if node.type in ("method", "singleton_method"):
        name_node = node.child_by_field_name("name")
        if name_node and name_node.text:
            name = name_node.text.decode("utf-8", errors="replace")
            if name == "initialize":
                return "constructor"
        return "method"
    # Go convention: standalone NewFoo() functions are constructors by convention.
    # In Go, constructors are package-level function_declaration nodes (not methods).
    if node.type == "function_declaration":
        name_node = node.child_by_field_name("name")
        if name_node and name_node.text:
            name = name_node.text.decode("utf-8", errors="replace")
            if re.match(r'^New[A-Z]', name):
                return "constructor"
    # Check if inside a class (Python, C, C++, PHP, Kotlin, Scala, etc.)
    parent = node.parent
    while parent:
        if parent.type in ("class_definition", "class_declaration", "class_body",
                           "class_specifier", "field_declaration_list",
                           "declaration_list", "template_body"):
            return "method"
        parent = parent.parent
    return "function"


def _is_trivial_body(node: Any, stmt_count: int) -> bool:
    """Determine if a function body is trivial.

    Trivial means: empty body (pass/noop), single return,
    single assignment, or single expression (getter/setter/delegation).

    Exception: expression-bodied arrow functions (no statement block) with
    substantial content (>3 lines) are NOT trivial — common in React components.
    """
    if stmt_count == 0:
        # Check for expression-bodied arrow functions with substantial content
        if node is not None and node.type not in ("block", "statement_block", "statement_list"):
            # Expression body (e.g., arrow function returning JSX) — check line span
            line_span = node.end_point[0] - node.start_point[0] + 1
            if line_span > 3:
                return False  # multi-line expression body is non-trivial
        return True  # genuinely empty body
    if stmt_count == 1:
        return True  # single-statement function
    return False


def parse_functions(content: str, filename: str) -> list[FunctionInfo]:
    """Parse source code and extract function/method definitions with metadata.

    Uses tree-sitter for accurate, language-aware AST parsing.
    Returns empty list if language is unsupported or content is unparseable.

    Args:
        content: Full source file content.
        filename: File name/path (used to determine language from extension).

    Returns:
        List of FunctionInfo dicts with name, kind, line range, parameters,
        body statement count, and triviality classification.
    """
    from tree_sitter import Parser

    # Determine language from file extension
    ext = ""
    dot_idx = filename.rfind(".")
    if dot_idx >= 0:
        ext = filename[dot_idx:]

    lang = _get_language(ext)
    if not lang:
        return []

    parser = Parser(lang)
    try:
        tree = parser.parse(content.encode("utf-8"))
    except Exception:  # noqa: BLE001
        return []

    functions: list[FunctionInfo] = []

    def _walk(node: Any) -> None:
        if node.type in _FUNCTION_NODE_TYPES:
            # --- Name extraction ---
            name_node = node.child_by_field_name("name")
            if not name_node:
                # C/C++: name is inside the declarator node
                declarator = node.child_by_field_name("declarator")
                if declarator:
                    name_node = (declarator.child_by_field_name("name")
                                 or declarator.child_by_field_name("declarator"))
                    # function_declarator wraps identifier + params
                    if name_node and name_node.type == "function_declarator":
                        name_node = name_node.child_by_field_name("declarator")
            name = name_node.text.decode("utf-8", errors="replace") if name_node else "<anonymous>"

            # --- Body extraction ---
            body = node.child_by_field_name("body")
            if not body:
                # Kotlin/Swift: body is a function_body child, not a "body" field
                for child in node.children:
                    if child.type in _BLOCK_NODE_TYPES:
                        body = child
                        break
            stmt_count = _count_body_statements(body) if body else 0
            param_count = _get_parameter_count(node)
            kind = _classify_function_kind(node)

            functions.append(FunctionInfo(
                name=name,
                kind=kind,
                start_line=node.start_point[0] + 1,  # 1-indexed
                end_line=node.end_point[0] + 1,
                parameter_count=param_count,
                body_statement_count=stmt_count,
                is_trivial=_is_trivial_body(body, stmt_count),
            ))

        for child in node.children:
            _walk(child)

    _walk(tree.root_node)
    return functions


def compute_trivial_ratio(functions: list[FunctionInfo]) -> float:
    """Compute the ratio of trivial functions to total functions.

    Returns 0.0 if no functions are found.
    """
    if not functions:
        return 0.0
    trivial = sum(1 for f in functions if f["is_trivial"])
    return trivial / len(functions)


def analyze_file_complexity(content: str, filename: str) -> dict[str, Any]:
    """Analyze a file's function-level complexity using tree-sitter.

    Returns a summary dict with function count, trivial ratio,
    and per-function details. Returns empty dict if language is unsupported.
    """
    functions = parse_functions(content, filename)
    if not functions:
        return {}

    return {
        "function_count": len(functions),
        "trivial_count": sum(1 for f in functions if f["is_trivial"]),
        "trivial_ratio": compute_trivial_ratio(functions),
        "functions": functions,
    }


def compute_change_proximity(
    ledger, prs: list, *, start_date=None, end_date=None
) -> dict:
    """Compute change proximity score: sum of distances between changed lines.

    High proximity score = scattered changes across files = risky.
    Low proximity score = concentrated changes in specific areas = safer.

    The algorithm:
    1. For each file in each PR, extract all changed line numbers
    2. Calculate the sum of distances between consecutive changed lines
    3. Normalize by total changes to get average distance per changed line
    4. Higher average distance = more scattered changes = higher risk

    Returns:
        dict with:
        - total_proximity: sum of all distances across all files
        - avg_proximity_per_change: average distance per changed line
        - per_file: list of per-file proximity details
        - per_pr: list of per-PR proximity details
        - total_files: number of files touched
        - total_changes: total lines changed
    """
    if not prs:
        return {
            "total_proximity": 0,
            "avg_proximity_per_change": 0.0,
            "per_file": [],
            "per_pr": [],
            "total_files": 0,
            "total_changes": 0,
            "pr_count": 0,
            "period_days": 0,
            "no_data": True,
        }

    per_file = []
    per_pr = []
    total_proximity = 0
    total_changes = 0
    total_files = 0

    for pr in prs:
        files = ledger.get_files_for_pr(pr.number)
        pr_proximity = 0
        pr_changes = 0
        pr_files = 0

        for f in files:
            if is_generated_file(f.filename, f.patch):
                continue

            # Get changed line numbers from the new file (where code ends up)
            # We focus on the new file positions since that's where the changes will be
            added_lines = _parse_hunk_lines(f.patch, "+") if f.patch else set()

            # Combine all changed lines and sort
            all_changed = sorted(added_lines)

            if len(all_changed) < 2:
                # Single line or no changes - proximity is 0 (concentrated)
                if all_changed:
                    pr_changes += 1
                    pr_files += 1
                    total_changes += 1
                    total_files += 1
                    per_file.append({
                        "filename": f.filename,
                        "pr_number": pr.number,
                        "proximity": 0,
                        "avg_proximity": 0.0,
                        "change_count": 1,
                        "line_range": (all_changed[0], all_changed[0]),
                        "hunk_count": len(parse_hunks(f.patch)) if f.patch else 0,
                    })
                continue

            # Calculate sum of distances between consecutive changed lines
            file_proximity = 0
            for i in range(1, len(all_changed)):
                file_proximity += all_changed[i] - all_changed[i - 1]

            file_changes = len(all_changed)
            avg_file_proximity = file_proximity / (file_changes - 1) if file_changes > 1 else 0

            pr_proximity += file_proximity
            pr_changes += file_changes
            pr_files += 1
            total_proximity += file_proximity
            total_changes += file_changes
            total_files += 1

            per_file.append({
                "filename": f.filename,
                "pr_number": pr.number,
                "proximity": file_proximity,
                "avg_proximity": round(avg_file_proximity, 1),
                "change_count": file_changes,
                "line_range": (all_changed[0], all_changed[-1]),
                "hunk_count": len(parse_hunks(f.patch)) if f.patch else 0,
            })

        if pr_files > 0:
            avg_pr_proximity = pr_proximity / pr_changes if pr_changes > 0 else 0
            per_pr.append({
                "number": pr.number,
                "proximity": pr_proximity,
                "avg_proximity": round(avg_pr_proximity, 1),
                "change_count": pr_changes,
                "file_count": pr_files,
            })

    # Calculate overall average proximity per change
    # Use (total_changes - total_files) as denominator since each file with N changes
    # contributes N-1 distances
    total_distances = total_changes - total_files if total_changes > total_files else 1
    avg_proximity = total_proximity / total_distances if total_distances > 0 else 0.0

    # Normalize by number of changes to get a meaningful metric
    # This gives us average distance per changed line
    avg_proximity_per_change = total_proximity / total_changes if total_changes > 0 else 0.0

    # Calculate period
    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 30
    else:
        period_days = 30

    # Sort per_file by proximity descending (most scattered first)
    per_file.sort(key=lambda x: x["proximity"], reverse=True)

    result = {
        "total_proximity": total_proximity,
        "avg_proximity_per_change": round(avg_proximity_per_change, 1),
        "per_file": per_file[:20],  # Top 20 most scattered files
        "per_pr": per_pr,
        "total_files": total_files,
        "total_changes": total_changes,
        "pr_count": len(prs),
        "period_days": period_days,
        "max_file_proximity": per_file[0]["proximity"] if per_file else 0,
        "max_avg_file_proximity": per_file[0]["avg_proximity"] if per_file else 0.0,
    }

    # Guard: no data if no changes or very short period with few PRs
    if total_changes == 0 or (period_days < 7 and len(prs) < 3):
        result["no_data"] = True

    return result


def compute_sum_of_coupling(
    ledger, prs: list, *, start_date=None, end_date=None
) -> dict:
    """Compute Sum of Coupling (SoC) per entity across all revisions.

    SoC per entity = sum of times it changed together with any other entity
    across all PRs in the period. Higher SoC indicates more coupling (risk).

    Returns:
        dict with:
        - per_entity: list of entity coupling scores
        - total_entities: number of entities touched
        - total_coupling: total coupling events across all entities
        - max_coupling_score: highest SoC observed
    """
    if not prs:
        return {
            "per_entity": [],
            "total_entities": 0,
            "total_coupling": 0,
            "max_coupling_score": 0,
            "pr_count": 0,
            "period_days": 0,
            "no_data": True,
        }

    per_entity_scores: dict[str, int] = defaultdict(int)
    entity_revision_counts: dict[str, int] = defaultdict(int)
    total_coupling = 0

    for pr in prs:
        files = ledger.get_files_for_pr(pr.number)
        entities = sorted({f.filename for f in files if not is_generated_file(f.filename, f.patch)})
        if len(entities) < 2:
            # Single-entity changes have no coupling in this PR
            for ent in entities:
                entity_revision_counts[ent] += 1
            continue

        # Count revisions per entity
        for ent in entities:
            entity_revision_counts[ent] += 1

        # For each entity, it is coupled with all other entities in this PR
        # SoC increment is number of other entities in the same PR
        for ent in entities:
            coupling_increment = len(entities) - 1
            per_entity_scores[ent] += coupling_increment
            total_coupling += coupling_increment

    # Build per-entity list sorted by coupling score
    per_entity = []
    for ent, score in per_entity_scores.items():
        per_entity.append({
            "entity": ent,
            "coupling_score": score,
            "revision_count": entity_revision_counts.get(ent, 0),
        })

    per_entity.sort(key=lambda x: x["coupling_score"], reverse=True)
    max_coupling = per_entity[0]["coupling_score"] if per_entity else 0

    # Calculate period
    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 30
    else:
        period_days = 30

    result = {
        "per_entity": per_entity[:20],
        "total_entities": len(per_entity_scores),
        "total_coupling": total_coupling,
        "max_coupling_score": max_coupling,
        "pr_count": len(prs),
        "period_days": period_days,
    }

    # Guard: no data if too few PRs in short period
    if len(prs) < 2 and period_days < 7:
        result["no_data"] = True

    return result


def compute_absolute_churn_trend(
    ledger, prs: list, *, start_date=None, end_date=None
) -> dict:
    """Compute absolute churn (adds+deletes) per date.

    Aggregates line additions and deletions per PR creation date to highlight
    periods with heavy integration activity (potential bottlenecks).
    """
    if not prs:
        return {
            "per_day": [],
            "total_additions": 0,
            "total_deletions": 0,
            "total_churn": 0,
            "max_daily_churn": 0,
            "pr_count": 0,
            "period_days": 0,
            "no_data": True,
        }

    daily_stats: dict[str, dict[str, int]] = defaultdict(lambda: {
        "additions": 0,
        "deletions": 0,
        "churn": 0,
        "pr_count": 0,
    })
    total_additions = 0
    total_deletions = 0

    for pr in prs:
        if not pr.created_at:
            continue
        day = pr.created_at.date().isoformat()
        pr_additions = 0
        pr_deletions = 0

        files = ledger.get_files_for_pr(pr.number)
        if files:
            for f in files:
                if is_generated_file(f.filename, f.patch):
                    continue
                pr_additions += f.additions
                pr_deletions += f.deletions
        else:
            pr_additions = pr.additions
            pr_deletions = pr.deletions

        daily_stats[day]["additions"] += pr_additions
        daily_stats[day]["deletions"] += pr_deletions
        daily_stats[day]["churn"] += pr_additions + pr_deletions
        daily_stats[day]["pr_count"] += 1

        total_additions += pr_additions
        total_deletions += pr_deletions

    per_day = []
    max_daily_churn = 0
    for day, stats in sorted(daily_stats.items()):
        max_daily_churn = max(max_daily_churn, stats["churn"])
        per_day.append({
            "date": day,
            "additions": stats["additions"],
            "deletions": stats["deletions"],
            "churn": stats["churn"],
            "pr_count": stats["pr_count"],
        })

    total_churn = total_additions + total_deletions

    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 30
    else:
        period_days = 30

    result = {
        "per_day": per_day,
        "total_additions": total_additions,
        "total_deletions": total_deletions,
        "total_churn": total_churn,
        "max_daily_churn": max_daily_churn,
        "pr_count": len(prs),
        "period_days": period_days,
    }

    if len(prs) < 2 and period_days < 7:
        result["no_data"] = True

    return result


def _parse_hunk_content_hashes(patch: str | None, side: str = "+") -> Counter:
    """Parse unified diff and return a Counter of content hashes for added/removed lines.

    Unlike _parse_hunk_lines() which tracks line *numbers* (fragile to
    subsequent insertions/deletions shifting positions), this function hashes
    the *content* of each changed line.  Two patches that add/remove the same
    text will produce matching hashes regardless of where the text ends up.

    Args:
        patch: Unified diff text.
        side:  "+" to collect added lines, "-" to collect removed lines.

    Returns:
        Counter mapping ``hash(line.strip())`` -> occurrence count.
    """
    if not patch:
        return Counter()
    hashes: Counter = Counter()
    for raw_line in patch.splitlines():
        if raw_line.startswith("@@") or raw_line.startswith("+++") or raw_line.startswith("---"):
            continue
        if raw_line.startswith("\\"):
            continue  # "\ No newline at end of file"
        if side == "+" and raw_line.startswith("+"):
            content = raw_line[1:].strip()
            if content:  # skip blank lines -- they match everything
                hashes[hash(content)] += 1
        elif side == "-" and raw_line.startswith("-"):
            content = raw_line[1:].strip()
            if content:
                hashes[hash(content)] += 1
    return hashes


def compute_code_survival(
    ledger, user_login: str, prs: list, *, start_date=None, end_date=None
) -> dict:
    """Analyze code survival (durability) over the period.

    Identifies how much code contributed by the user in the past remains
    untouched (survives) by subsequent changes (both own and others)
    within the target period.

    Uses **content-fingerprint tracking** instead of line numbers so that
    subsequent insertions/deletions shifting line positions do not corrupt
    the survival calculation.
    """
    if not prs:
        return {
            "survival_rate": 0.0,
            "total_contributed": 0,
            "total_survived": 0,
            "total_churned": 0,
            "per_cohort": [],
            "pr_count": 0,
            "period_days": 0,
            "no_data": True,
        }

    # 1. Build cohorts: PR -> { filename -> Counter[content_hash] }
    cohorts: dict[int, dict[str, Counter]] = {}
    for pr in prs:
        cohorts[pr.number] = {}
        for f in ledger.get_files_for_pr(pr.number):
            if is_generated_file(f.filename, f.patch):
                continue
            if f.patch:
                added_hashes = _parse_hunk_content_hashes(f.patch, "+")
                if added_hashes:
                    cohorts[pr.number][f.filename] = added_hashes

    # 2. Track subsequent changes -- subtract removed content hashes
    all_prs = sorted(ledger.bundle.pull_requests, key=lambda p: p.created_at or datetime.min)

    cohort_survival = []
    total_contributed = 0
    total_survived = 0

    for pr_num, cohort_files in cohorts.items():
        cohort_pr = next(p for p in prs if p.number == pr_num)
        pr_date = cohort_pr.created_at

        initial_count = sum(c.total() for c in cohort_files.values())
        if initial_count == 0:
            continue

        # Deep-copy counters so subtraction is isolated per cohort
        current = {fn: Counter(cnts) for fn, cnts in cohort_files.items()}

        subsequent_prs = [p for p in all_prs if p.created_at and p.created_at > pr_date]

        for sub_pr in subsequent_prs:
            for sf in ledger.get_files_for_pr(sub_pr.number):
                if sf.filename in current and sf.patch:
                    removed_hashes = _parse_hunk_content_hashes(sf.patch, "-")
                    current[sf.filename].subtract(removed_hashes)
                    # Clamp negatives to zero (a hash can't survive less than 0 times)
                    current[sf.filename] = +current[sf.filename]

        survived_count = sum(c.total() for c in current.values())
        total_contributed += initial_count
        total_survived += survived_count

        survival_rate = (survived_count / initial_count * 100) if initial_count else 0.0
        cohort_survival.append({
            "pr_number": pr_num,
            "date": pr_date.date().isoformat() if pr_date else None,
            "initial_lines": initial_count,
            "survived_lines": survived_count,
            "survival_rate": round(survival_rate, 1),
        })

    overall_rate = (total_survived / total_contributed * 100) if total_contributed else 0.0

    if start_date and end_date:
        period_days = (end_date - start_date).total_seconds() / 86400
    elif prs:
        dates = [p.created_at for p in prs if p.created_at]
        period_days = (max(dates) - min(dates)).days or 1 if dates else 30
    else:
        period_days = 30

    result = {
        "survival_rate": round(overall_rate, 1),
        "total_contributed": total_contributed,
        "total_survived": total_survived,
        "total_churned": total_contributed - total_survived,
        "per_cohort": cohort_survival,
        "pr_count": len(prs),
        "period_days": period_days,
    }

    if total_contributed == 0 or (period_days < 14 and len(prs) < 3):
        result["no_data"] = True

    return result


# ---------------------------------------------------------------------------
# Shared private functions promoted from metric plugins
# ---------------------------------------------------------------------------

def indent_level(line: str) -> float:
    """Compute indentation level (in units of 4 spaces) for a code line.

    Tabs are converted to 4 spaces. Used for whitespace-based complexity estimation.
    """
    expanded = line.replace("\t", "    ")
    indent_spaces = len(expanded) - len(expanded.lstrip(" "))
    return indent_spaces / 4


def complexity_from_patch(patch: str | None) -> float | None:
    """Estimate average nesting depth (complexity proxy) from a unified diff patch.

    Returns the mean indentation level of added lines, or None if no added lines.
    """
    if not patch:
        return None
    indent_levels: list[float] = []
    for line in patch.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        content = line[1:]
        if not content.strip():
            continue
        indent_levels.append(indent_level(content))
    if not indent_levels:
        return None
    return sum(indent_levels) / len(indent_levels)


def build_file_contributors(
    files_in_scope: set[str],
    bundle,
    ledger,
    *,
    by: str,
) -> dict[str, dict[str, float]]:
    """Build a filename -> author -> contribution map.

    Args:
        files_in_scope: Set of filenames to analyze.
        bundle: The CanonicalBundle containing all repo data.
        ledger: The Ledger for querying PRs/files.
        by: "revisions" counts commits per author; "lines" weights by file.changes.

    Returns:
        A dict mapping each filename to a dict of author -> contribution score.
    """
    file_contributors: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    # Build file -> set of PR numbers
    file_to_prs: dict[str, set[int]] = defaultdict(set)
    for f in getattr(bundle, "files", []):
        if f.filename in files_in_scope and not is_generated_file(f.filename, getattr(f, "patch", None)):
            file_to_prs[f.filename].add(f.pull_request_number)

    # Build PR -> commits mapping
    pr_to_commits: dict[int, list] = defaultdict(list)
    for commit in bundle.commits:
        if commit.pull_request_number is not None:
            pr_to_commits[commit.pull_request_number].append(commit)

    if by == "revisions":
        # Count commits per author per file
        for filename, pr_numbers in file_to_prs.items():
            for pr_num in pr_numbers:
                for commit in pr_to_commits.get(pr_num, []):
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        file_contributors[filename][author] += 1

        # Fallback: credit PR authors for files with no linked commits
        for pr in bundle.pull_requests:
            for f in ledger.get_files_for_pr(pr.number):
                if f.filename in files_in_scope and not is_generated_file(f.filename, getattr(f, "patch", None)):
                    if not file_contributors[f.filename]:
                        file_contributors[f.filename][pr.user.login] += 1

    elif by == "lines":
        # Weight by file.changes per PR author
        for pr in bundle.pull_requests:
            pr_author = pr.user.login
            for f in ledger.get_files_for_pr(pr.number):
                if f.filename not in files_in_scope:
                    continue
                if is_generated_file(f.filename, getattr(f, "patch", None)):
                    continue
                change_weight = f.changes if f.changes else (f.additions + f.deletions)
                contribution_score = max(1.0, float(change_weight))
                file_contributors[f.filename][pr_author] += contribution_score

    return file_contributors
