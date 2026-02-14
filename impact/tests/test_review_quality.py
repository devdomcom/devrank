from datetime import timedelta

from impact.domain.models import ReviewState
from impact.metrics.plugins.authored.review_quality import (
    ReviewIterations,
    SlowReviewResponse,
    TimeToFirstReview,
)
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_review_quality_metrics():
    author = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=10, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, author, repo, created_at=start, merged_at=start + timedelta(hours=24))
    pr2 = make_pr(
        2,
        author,
        repo,
        created_at=start + timedelta(hours=1),
        merged_at=start + timedelta(hours=48),
    )

    review1 = make_review(
        101, 1, reviewer, start + timedelta(hours=5), ReviewState.CHANGES_REQUESTED, body=None
    )
    review2 = make_review(
        102, 1, reviewer, start + timedelta(hours=6), ReviewState.APPROVED, body=None
    )
    review3 = make_review(
        103, 2, reviewer, start + timedelta(hours=3), ReviewState.COMMENTED, body=None
    )

    commit_after_review = make_commit(
        sha="c1",
        author=author,
        date=start + timedelta(hours=7),
        pr_number=1,
        message="fix",
    )

    bundle = make_bundle(
        users=[author, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit_after_review],
        reviews=[review1, review2, review3],
    )
    ctx = make_context(
        bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=5)
    )

    iterations = ReviewIterations().run(ctx)
    assert iterations.details["average_iterations"] == 0.5
    assert iterations.details["per_pr"][0]["iterations"] == 1

    tfr = TimeToFirstReview().run(ctx)
    # PR1: 5h, PR2: 2h -> median 3.5h, p75 4.25h
    assert abs(tfr.details["median_hours"] - 3.5) < 1e-6
    assert abs(tfr.details["p75_hours"] - 4.25) < 1e-6

    slow = SlowReviewResponse().run(ctx)
    assert slow.details["samples"] == 1
    assert abs(slow.details["median_hours"] - 2.0) < 1e-6
    assert slow.details["per_review"][0]["pr"] == 1


def test_review_iterations_groups_simultaneous_crs_as_one_cycle():
    """Bug 10: Multiple reviewers requesting changes within 4 hours should count as 1 cycle.

    If 3 reviewers all request changes at roughly the same time, that's 1 round
    of feedback, not 3 separate iterations.
    """
    author = make_user(id=1, login="alice")
    reviewer1 = make_user(id=2, login="bob")
    reviewer2 = make_user(id=3, login="carol")
    reviewer3 = make_user(id=4, login="dave")
    owner = make_user(id=5, login="org")
    repo = make_repo(id=10, name="repo", owner=owner)

    start = DEFAULT_START
    pr = make_pr(1, author, repo, created_at=start, merged_at=start + timedelta(hours=48))

    # 3 reviewers request changes within 2 hours of each other (< 4h threshold)
    cr1 = make_review(101, 1, reviewer1, start + timedelta(hours=5), ReviewState.CHANGES_REQUESTED)
    cr2 = make_review(102, 1, reviewer2, start + timedelta(hours=6), ReviewState.CHANGES_REQUESTED)
    cr3 = make_review(103, 1, reviewer3, start + timedelta(hours=7), ReviewState.CHANGES_REQUESTED)

    bundle = make_bundle(
        users=[author, reviewer1, reviewer2, reviewer3, owner],
        repositories=[repo],
        pull_requests=[pr],
        reviews=[cr1, cr2, cr3],
    )
    ctx = make_context(
        bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=5)
    )

    result = ReviewIterations().run(ctx)
    # All 3 CRs within 4 hours -> 1 cycle, not 3
    assert result.details["per_pr"][0]["iterations"] == 1
    assert result.details["average_iterations"] == 1.0


def test_review_iterations_separate_cycles():
    """Bug 10: CRs more than 4 hours apart should count as separate cycles."""
    author = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=10, name="repo", owner=owner)

    start = DEFAULT_START
    pr = make_pr(1, author, repo, created_at=start, merged_at=start + timedelta(hours=48))

    # Two CRs separated by 10 hours (> 4h threshold) -> 2 cycles
    cr1 = make_review(101, 1, reviewer, start + timedelta(hours=5), ReviewState.CHANGES_REQUESTED)
    cr2 = make_review(102, 1, reviewer, start + timedelta(hours=15), ReviewState.CHANGES_REQUESTED)

    bundle = make_bundle(
        users=[author, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr],
        reviews=[cr1, cr2],
    )
    ctx = make_context(
        bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=5)
    )

    result = ReviewIterations().run(ctx)
    assert result.details["per_pr"][0]["iterations"] == 2
