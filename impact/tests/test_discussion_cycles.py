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
    """reviewer -> author -> reviewer -> author with >1h gaps = 3 cycles."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=20))
    # Each event >1h apart so they form separate turns
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=2))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=5))
    r2 = make_review(2, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=8))
    c2 = make_comment(2, 1, alice, created_at=DEFAULT_START + timedelta(hours=11))

    bundle = make_bundle(pull_requests=[pr], reviews=[r1, r2], comments=[c1, c2])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    assert result.details["per_pr"][0]["cycles"] == 3


def test_consecutive_same_speaker_collapsed():
    """Multiple comments by same person within 1h count as one turn."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=20))
    # bob posts 2 reviews 5min apart (one turn), then alice, then bob again >1h later
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    r2 = make_review(2, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1, minutes=5))
    c1 = make_comment(1, 1, alice, created_at=DEFAULT_START + timedelta(hours=4))
    r3 = make_review(3, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=7))

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


def test_interleaved_thread_replies_collapsed():
    """Interleaved replies to multiple inline comments should be 1 cycle, not N.

    Real GitHub pattern: reviewer posts 3 inline comments (one review round),
    author replies to each one individually over a few minutes.  Naive flat
    counting would see R-A-R-A-R-A = 5 alternations.  Turn-based counting
    collapses to: 1 reviewer turn + 1 author turn = 1 cycle.
    """
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=10))

    # Bob posts 3 inline review comments within minutes (one review round)
    r1 = make_review(1, 1, bob, submitted_at=DEFAULT_START + timedelta(hours=1))
    c1 = make_comment(1, 1, bob, created_at=DEFAULT_START + timedelta(hours=1, minutes=1))
    c2 = make_comment(2, 1, bob, created_at=DEFAULT_START + timedelta(hours=1, minutes=2))

    # Alice replies to each one over the next 10 minutes
    c3 = make_comment(3, 1, alice, created_at=DEFAULT_START + timedelta(hours=1, minutes=15))
    c4 = make_comment(4, 1, alice, created_at=DEFAULT_START + timedelta(hours=1, minutes=20))
    c5 = make_comment(5, 1, alice, created_at=DEFAULT_START + timedelta(hours=1, minutes=25))

    bundle = make_bundle(
        pull_requests=[pr],
        reviews=[r1],
        comments=[c1, c2, c3, c4, c5],
    )
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    # One reviewer turn + one author turn = 1 cycle (not 5)
    assert result.details["per_pr"][0]["cycles"] == 1


def test_interleaved_across_hours_counts_separately():
    """If author and reviewer alternate with >1h gaps, each is a separate turn."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo()

    pr = make_pr(1, alice, repo, created_at=DEFAULT_START, merged_at=DEFAULT_START + timedelta(hours=20))

    # Bob comments, 2 hours later alice replies, 2 hours later bob again
    c1 = make_comment(1, 1, bob, created_at=DEFAULT_START + timedelta(hours=1))
    c2 = make_comment(2, 1, alice, created_at=DEFAULT_START + timedelta(hours=3))
    c3 = make_comment(3, 1, bob, created_at=DEFAULT_START + timedelta(hours=5))

    bundle = make_bundle(pull_requests=[pr], comments=[c1, c2, c3])
    ctx = make_context(bundle, "alice")
    result = DiscussionCycles().run(ctx)

    # 3 distinct turns, 2 transitions = 2 cycles
    assert result.details["per_pr"][0]["cycles"] == 2


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
