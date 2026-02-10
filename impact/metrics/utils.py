from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, TypedDict
import re

from impact.domain.models import MetricContext, ReviewState
from impact.ledger.ledger import Ledger


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


def collect_pr_interactions(
    context: MetricContext, pr_number: int, author: str, cutoff_time: datetime | None = None
) -> list[Interaction]:
    """Collect interactions (reviews, comments, timeline events) for a PR up to cutoff_time, excluding bots."""
    interactions: list[Interaction] = []

    # Reviews
    for rev in context.ledger.get_reviews_for_pr(pr_number):
        if rev.user.login == author or rev.user.type == "Bot":
            continue
        if cutoff_time and rev.submitted_at >= cutoff_time:
            continue
        interactions.append(
            {"actor": rev.user.login, "kind": "review", "created_at": rev.submitted_at}
        )

    # Comments (issue + review)
    for c in context.ledger.get_comments_for_pr(pr_number):
        if c.user.login == author or c.user.type == "Bot":
            continue
        ts = c.created_at
        if cutoff_time and ts >= cutoff_time:
            continue
        kind = "comment_review" if c.type.value == "review" else "comment_issue"
        interactions.append({"actor": c.user.login, "kind": kind, "created_at": ts})

    # Timeline fallbacks (covers events not already represented)
    seen_ts_ids = {(i["actor"], i["created_at"]) for i in interactions}
    for evt in context.ledger.get_timeline_for_pr(pr_number):
        if evt.actor.login == author or evt.actor.type == "Bot":
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
    # No later commits or CRs? (exclude merge commits)
    pr = ledger.get_pr(pr_num)
    later_commits = [
        c for c in ledger.get_commits_for_pr(pr_num)
        if c.date > rev_time
        and c.sha != getattr(pr, 'merge_commit_sha', None)
        and not c.message.lower().startswith("merge ")
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


def is_bug_fix_indicator(text: str) -> bool:
    """DRY helper to detect bug-fix focus in titles/bodies/messages."""
    if not text:
        return False
    text_lower = text.lower()
    # Specific prefix patterns (no false positives)
    prefix_patterns = ["fix:", "bugfix:", "bug fix", "fixes #", "closes #", "resolves #", "bug:", "hotfix", "issue #"]
    if any(p in text_lower for p in prefix_patterns):
        return True
    # Ambiguous terms need word boundaries
    word_boundary_patterns = [r'\berror\b', r'\bcrash\b', r'\bregression\b']
    return any(re.search(p, text_lower) for p in word_boundary_patterns)


def is_revert_indicator(text: str) -> bool:
    """DRY helper to detect reverts (per proposal; assumes Git revert msg/SHA)."""
    if not text:
        return False
    text_lower = text.lower()
    # Common GitHub revert patterns (from sample data)
    return text_lower.startswith("revert") or "reverts commit" in text_lower


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
    # test_ should only match at start of filename or after /
    if re.search(r'(^|/)test_', f_lower):
        return True
    # Tool/framework names should match as path segments
    if re.search(r'(^|/)(jest|mocha|pytest)(/|\.|\b)', f_lower):
        return True
    return False


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
    if re.search(r"(?i)(?:fixes|closes|resolves|refs?)\s*#?\d+", body) or re.search(r"(?<!\w)#\d{3,}", body):
        score += 15
    elif re.search(r"(?i)pr\s*#?\d+", body) or "pull request" in body.lower():
        score += 15
    elif re.search(r"#\d+", body):
        score += 10
    return min(score, 100)


# DRY dep file detector (for dep change rate; common across langs)
def is_dependency_file(filename: str) -> bool:
    if not filename:
        return False
    f_lower = filename.lower()
    dep_patterns = [
        "package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
        "requirements.txt", "requirements-dev.txt", "requirements-test.txt", "pipfile", "pipfile.lock",
        "poetry.lock", "pyproject.toml",
        "gemfile", "gemfile.lock",
        "cargo.toml", "cargo.lock",
        "pom.xml", "build.gradle", "build.gradle.kts",
        "go.mod", "go.sum",
        "composer.json", "composer.lock",
        "pubspec.yaml", "pubspec.lock",
    ]
    return any(p in f_lower for p in dep_patterns)


# DRY doc file detector (broad patterns for .md/.rst/docs/README etc across languages/repos)
def is_documentation_file(filename: str) -> bool:
    if not filename:
        return False
    f_lower = filename.lower()
    doc_patterns = [
        # Common doc files
        ".md", ".rst", ".adoc", ".txt", "readme", "changelog", "contributing",
        "docs/", "doc/", "documentation/", "wiki/", "guides/", "manual/",
        # Project-specific
        "license", "notice", "authors", "faq", "glossary",
        # Web/docsite
        "index.html", "mkdocs.yml", "sphinx", "docusaurus",
    ]
    return any(p in f_lower for p in doc_patterns)


# DRY conventional commit checker (industry best practices: type(scope)!: desc; types from conventionalcommits.org)
def is_conventional_commit(message: str) -> bool:
    if not message:
        return False
    msg = message.strip().lower()
    types = ["feat", "fix", "docs", "style", "refactor", "perf", "test", "build", "ci", "chore", "revert"]
    # Match type!(scope): or type: 
    return any(msg.startswith(t + ":") or msg.startswith(t + "!:") or msg.startswith(t + "(") for t in types)


def compute_code_churn(
    ledger, user_login: str, prs: list, window_days: int = 30
) -> dict:
    """Code churn %: lines modifying own prior code (<=30d; file-level; weekly for spikes/period)."""
    file_history = defaultdict(list)
    for upr in ledger.get_prs_for_user(user_login):
        for f in ledger.get_files_for_pr(upr.number):
            file_history[f.filename].append(upr.created_at)
    churned_lines = 0
    total_lines = 0
    per_pr = []
    # Weekly bins for period-aware spikes/freq (short: spike bad; long: steady high bad)
    weekly_churn = defaultdict(lambda: {"churn_lines": 0, "total_lines": 0})
    for pr in prs:
        pr_date = pr.created_at
        pr_churn = 0
        pr_total = 0
        for f in ledger.get_files_for_pr(pr.number):
            lines = f.changes
            pr_total += lines
            prev = [
                t for t in file_history.get(f.filename, [])
                if t < pr_date and (pr_date - t).days <= window_days
            ]
            if prev:
                pr_churn += lines
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
    # Normalize churn/week (period length aware)
    period_days = 30  # fallback
    if prs:
        dates = [p.created_at for p in prs if p.created_at]
        if dates:
            period_days = (max(dates) - min(dates)).days or 1
    churn_per_week = (churned_lines / max(1, period_days / 7.0))
    return {
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


def compute_self_merge_rate(ledger, user_login: str) -> dict:
    """% PRs (own+others) merged by author w/o approval; repo rate for culture check."""
    all_prs = ledger.bundle.pull_requests
    # Engineer merges (by user, any PR)
    eng_merged = [
        pr for pr in all_prs
        if pr.merged and pr.merged_by and pr.merged_by.login == user_login
    ]
    no_approval_count = 0
    per_pr = []
    for pr in eng_merged:
        # Approval = any APPROVED review before/ at merge
        reviews = ledger.get_reviews_for_pr(pr.number)
        has_approval = any(
            r.state == ReviewState.APPROVED
            for r in reviews
            if not pr.merged_at or r.submitted_at <= pr.merged_at
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
