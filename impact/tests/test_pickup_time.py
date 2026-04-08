"""Tests for PickupTime metric."""

from datetime import UTC, datetime, timedelta

from impact.metrics.plugins.authored.pickup_time import PickupTime
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)
from impact.domain.models import ReviewState, CommentType, TimelineEvent


def test_basic_pickup_time():
    """Pickup time = time from PR creation to first non-author review."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    review = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=3))

    bundle = make_bundle(pull_requests=[pr], reviews=[review])
    # Use 30-day window to avoid short-period no_data guard
    ctx = make_context(bundle, "alice", end_date=DEFAULT_START + timedelta(days=30))
    result = PickupTime().run(ctx)

    assert result.details["picked_up_prs"] == 1
    assert result.details["median_hours"] == 3.0
    assert "no_data" not in result.details


def test_multiple_prs_median():
    """Median across multiple PRs."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr1 = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    pr2 = make_pr(2, alice, repo, created_at=DEFAULT_START + timedelta(hours=1), merged_at=DEFAULT_START + timedelta(hours=12))

    # PR1 picked up after 2h, PR2 picked up after 6h
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=2))
    r2 = make_review(2, 2, bob, submitted_at=DEFAULT_START + timedelta(hours=7))

    bundle = make_bundle(pull_requests=[pr1, pr2], reviews=[r1, r2])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["picked_up_prs"] == 2
    # Median of [2, 6] = 4
    assert result.details["median_hours"] == 4.0


def test_comment_counts_as_activity():
    """A non-author comment should count as pickup activity."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    comment = make_comment(1, 1, bob, created_at=DEFAULT_START + timedelta(hours=1))

    bundle = make_bundle(pull_requests=[pr], comments=[comment])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["picked_up_prs"] == 1
    assert result.details["median_hours"] == 1.0


def test_earliest_activity_wins():
    """When both comment and review exist, earliest one is used."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    comment = make_comment(1, 1, bob, created_at=DEFAULT_START + timedelta(hours=1))
    review = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=5))

    bundle = make_bundle(pull_requests=[pr], comments=[comment], reviews=[review])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["median_hours"] == 1.0  # comment was earlier


def test_author_activity_ignored():
    """Author's own reviews/comments should not count as pickup."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    # Alice comments on her own PR — not pickup
    self_comment = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=1))
    # Bob reviews later — this is the real pickup
    review = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=4))

    bundle = make_bundle(pull_requests=[pr], comments=[self_comment], reviews=[review])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["median_hours"] == 4.0


def test_no_activity_marks_none():
    """PR with no non-author activity should have hours=None in per_pr."""
    alice = make_user(id=1, login="alice")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START)

    bundle = make_bundle(pull_requests=[pr])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["picked_up_prs"] == 0
    assert result.details["per_pr"][0]["hours"] is None
    assert result.details["no_data"] is True


def test_no_prs_is_no_data():
    """No PRs at all should be no_data."""
    bundle = make_bundle()
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["no_data"] is True
    assert result.details["picked_up_prs"] == 0


def test_review_requested_timeline_counts():
    """A review_requested timeline event by non-author counts as pickup."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    evt = TimelineEvent(
        id=100,
        event="review_requested",
        actor=bob,
        created_at=DEFAULT_START + timedelta(hours=2),
        pull_request_number=1,
    )

    bundle = make_bundle(pull_requests=[pr], timeline=[evt])
    ctx = make_context(bundle, "alice")
    result = PickupTime().run(ctx)

    assert result.details["picked_up_prs"] == 1
    assert result.details["median_hours"] == 2.0
