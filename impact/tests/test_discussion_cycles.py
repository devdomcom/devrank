"""Tests for DiscussionCycles metric."""

from datetime import UTC, datetime, timedelta

from impact.metrics.plugins.authored.discussion_cycles import DiscussionCycles
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
from impact.domain.models import ReviewState


def test_zero_cycles_no_comments():
    """Merged PR with no comments/reviews has 0 cycles."""
    alice = make_user(id=1, login="alice")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=5))

    bundle = make_bundle(pull_requests=[pr])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["merged_prs"] == 1
    assert result.details["per_pr"][0]["cycles"] == 0


def test_single_reviewer_comment_no_alternation():
    """Only reviewer comments, no author reply = 0 cycles (no alternation)."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=2))

    bundle = make_bundle(pull_requests=[pr], reviews=[r1])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["per_pr"][0]["cycles"] == 0


def test_one_cycle_author_reviewer():
    """Author comment then reviewer comment = 1 cycle (one alternation)."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=1))
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=2))

    bundle = make_bundle(pull_requests=[pr], comments=[c1], reviews=[r1])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["per_pr"][0]["cycles"] == 1


def test_full_back_and_forth():
    """reviewer -> author -> reviewer -> author = 3 alternations."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    # bob(reviewer) -> alice(author) -> bob(reviewer) -> alice(author)
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=2))
    r2 = make_review(2, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=3))
    c2 = make_comment(2, 1, alice, created_at=DEFAULT_START + timedelta(hours=4))

    bundle = make_bundle(pull_requests=[pr], reviews=[r1, r2], comments=[c1, c2])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["per_pr"][0]["cycles"] == 3


def test_consecutive_same_speaker_collapsed():
    """Multiple comments by same person in a row count as one turn."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    # bob, bob, alice, bob = 2 alternations (bob->alice, alice->bob)
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    r2 = make_review(2, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1, minutes=5))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=2))
    r3 = make_review(3, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=3))

    bundle = make_bundle(pull_requests=[pr], reviews=[r1, r2, r3], comments=[c1])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["per_pr"][0]["cycles"] == 2


def test_average_across_prs():
    """Average cycles across multiple merged PRs."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr1 = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    pr2 = make_pr(2, alice, repo, created_at=DEFAULT_START + timedelta(hours=1), merged_at=DEFAULT_START + timedelta(hours=12))

    # PR1: 2 cycles (bob->alice->bob)
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=2))
    r2 = make_review(2, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=3))

    # PR2: 0 cycles (no comments)

    bundle = make_bundle(pull_requests=[pr1, pr2], reviews=[r1, r2], comments=[c1])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["merged_prs"] == 2
    assert result.details["average_cycles"] == 1.0  # (2 + 0) / 2


def test_unmerged_prs_excluded():
    """Only merged PRs should be counted."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    merged_pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    open_pr = make_pr(2, alice, repo, created_at=DEFAULT_START)

    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    r2 = make_review(2, 2, bob, submitted_at=DEFAULT_START + timedelta(hours=1))

    bundle = make_bundle(pull_requests=[merged_pr, open_pr], reviews=[r1, r2])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["merged_prs"] == 1


def test_bot_comments_excluded():
    """Bot comments should not count as discussion."""
    alice = make_user(id=1, login="alice")
    bot = make_user(id=3, login="ci-bot", is_bot=True)
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))
    # bot comment between alice and bob should not count as alternation
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=1))
    c_bot = make_comment(2, 1, bot, created_at=DEFAULT_START + timedelta(hours=1, minutes=30))
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=2))

    bundle = make_bundle(pull_requests=[pr], comments=[c1, c_bot], reviews=[r1])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    # alice -> bob = 1 cycle (bot ignored)
    assert result.details["per_pr"][0]["cycles"] == 1


def test_no_data_short_period():
    """Short period with fewer than 3 merged PRs triggers no_data."""
    alice = make_user(id=1, login="alice")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=5))

    bundle = make_bundle(pull_requests=[pr])
    ctx = make_context(bundle, "alice", end_date=DEFAULT_START + timedelta(days=7))
    result = DiscussionCycles().run(ctx)

    assert result.details["no_data"] is True


def test_no_merged_prs_is_no_data():
    """No merged PRs at all should be no_data."""
    bundle = make_bundle()
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["no_data"] is True
    assert result.details["merged_prs"] == 0
