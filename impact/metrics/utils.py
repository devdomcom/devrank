from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TypedDict
from collections import Counter

from impact.domain.models import MetricContext
from impact.ledger.ledger import Ledger


class Interaction(TypedDict):
    actor: str
    kind: str  # review|comment_issue|comment_review|timeline
    created_at: Any


def percentile(values: List[float], pct: float) -> float:
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


def calculate_merge_time_hours(pr) -> Optional[float]:
    """Calculate merge time in hours for a PR, or None if not merged."""
    if pr.merged and pr.merged_at and pr.created_at:
        delta = pr.merged_at - pr.created_at
        return delta.total_seconds() / 3600
    return None


def collect_pr_interactions(context: MetricContext, pr_number: int, author: str, cutoff_time: Optional[datetime] = None) -> List[Interaction]:
    """Collect interactions (reviews, comments, timeline events) for a PR up to cutoff_time, excluding bots."""
    interactions: List[Interaction] = []

    # Reviews
    for rev in context.ledger.get_reviews_for_pr(pr_number):
        if rev.user.login == author or rev.user.type == "Bot":
            continue
        if cutoff_time and rev.submitted_at >= cutoff_time:
            continue
        interactions.append({"actor": rev.user.login, "kind": "review", "created_at": rev.submitted_at})

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
    seen_ts_ids = {(i["kind"], i["actor"], i["created_at"]) for i in interactions}
    for evt in context.ledger.get_timeline_for_pr(pr_number):
        if evt.actor.login == author or evt.actor.type == "Bot":
            continue
        if cutoff_time and evt.created_at >= cutoff_time:
            continue
        if evt.event in ("reviewed", "commented"):
            key = ("timeline", evt.actor.login, evt.created_at)
            if key not in seen_ts_ids:
                interactions.append({"actor": evt.actor.login, "kind": "timeline", "created_at": evt.created_at})
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


def has_pr_event_after(ledger: Ledger, pr_number: int, after_time: datetime, event_type: Optional[str] = None) -> bool:
    """Check if a PR has any timeline event (or specific type) after a given time."""
    for evt in ledger.get_timeline_for_pr(pr_number):
        if evt.created_at > after_time and (event_type is None or evt.event == event_type):
            return True
    return False


def is_change_request(review, ledger) -> bool:
    """Check if a review is a change request (formal or via inline comments)."""
    if review.state.value == "changes_requested":
        return True
    # Check for inline comments
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
        return 'trivial'
    elif changes < 100:
        return 'small'
    elif changes < 1000:
        return 'medium'
    else:
        return 'large'


def get_week_activity_details(dates: List[datetime], start_date: Optional[datetime], end_date: Optional[datetime]) -> dict:
    """Return granular week details: active/inactive weeks, gaps (disengagement)."""
    if not start_date or not end_date:
        return {"active_weeks": [], "inactive_weeks": [], "active_count": 0, "total_weeks": 0, "max_gap_weeks": 0}

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
            prev_y, prev_w = sorted_inactive[i-1]
            curr_y, curr_w = sorted_inactive[i]
            if (curr_y == prev_y and curr_w == prev_w + 1) or (curr_y == prev_y + 1 and curr_w == 1 and prev_w >= 52):
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

def get_weekly_activity_counts(dates: List[datetime]) -> dict:
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
    # Author commit after review within window?
    commits = [c for c in ledger.get_commits_for_pr(pr_num) if c.date > rev_time and c.author.login != review.user.login]
    if not commits:
        return False
    first_commit = min(commits, key=lambda c: c.date)
    if (first_commit.date - rev_time).total_seconds() / 3600 > max_hours:
        return False
    # No other reviews between this review and first commit?
    other_reviews = [r for r in ledger.get_reviews_for_pr(pr_num) if r.submitted_at > rev_time and r.submitted_at < first_commit.date and r.user.login != review.user.login]
    return not other_reviews

def review_led_to_commit(ledger, review, max_hours=24) -> bool:
    """Check if review led to immediate author commit (clear correlation, no intervening reviews)."""
    pr_num = review.pull_request_number
    rev_time = review.submitted_at
    # Author commit after review within window?
    commits = [c for c in ledger.get_commits_for_pr(pr_num) if c.date > rev_time and c.author.login != review.user.login]
    if not commits:
        return False
    first_commit = min(commits, key=lambda c: c.date)
    if (first_commit.date - rev_time).total_seconds() / 3600 > max_hours:
        return False
    # No other reviews between?
    other_reviews = [r for r in ledger.get_reviews_for_pr(pr_num) if r.submitted_at > rev_time and r.submitted_at < first_commit.date and r.user.login != review.user.login]
    return not other_reviews

def approval_was_final(ledger, review, max_hours_to_merge=48) -> bool:
    """Check if approval was last activity (no later commits/CRs, leads to merge)."""
    if review.state.value != "approved":
        return False
    pr_num = review.pull_request_number
    rev_time = review.submitted_at
    # No later commits or CRs?
    later_commits = [c for c in ledger.get_commits_for_pr(pr_num) if c.date > rev_time]
    later_crs = [r for r in ledger.get_reviews_for_pr(pr_num) if r.submitted_at > rev_time and r.state.value == "changes_requested"]
    if later_commits or later_crs:
        return False
    # Merged after within window?
    return is_pr_merged_after(ledger, pr_num, rev_time) and (ledger.get_pr(pr_num).merged_at - rev_time).total_seconds() / 3600 <= max_hours_to_merge
