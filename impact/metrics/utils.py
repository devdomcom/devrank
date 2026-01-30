from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict

from impact.domain.models import MetricContext
from impact.ledger.ledger import Ledger


class Interaction(TypedDict):
    actor: str
    kind: str  # review|comment_issue|comment_review|timeline
    created_at: Any


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