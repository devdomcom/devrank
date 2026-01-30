from datetime import timedelta

from impact.metrics.plugins.review_quality import ReviewIterations, TimeToFirstReview, SlowReviewResponse
from impact.domain.models import ReviewState
from impact.tests.conftest import (
    DEFAULT_START,
    make_user,
    make_repo,
    make_pr,
    make_review,
    make_commit,
    make_bundle,
    make_context,
)


def test_review_quality_metrics():
    author = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=10, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, author, repo, created_at=start, merged_at=start + timedelta(hours=24))
    pr2 = make_pr(2, author, repo, created_at=start + timedelta(hours=1), merged_at=start + timedelta(hours=48))

    review1 = make_review(101, 1, reviewer, start + timedelta(hours=5), ReviewState.CHANGES_REQUESTED, body=None)
    review2 = make_review(102, 1, reviewer, start + timedelta(hours=6), ReviewState.APPROVED, body=None)
    review3 = make_review(103, 2, reviewer, start + timedelta(hours=3), ReviewState.COMMENTED, body=None)

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
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=5))

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
