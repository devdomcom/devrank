from datetime import timedelta

from impact.metrics.plugins.influence.reviews_given import ReviewsGiven
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_reviews_given_collaborative_volume():
    """Test review count and normalized rate (DRY with throughput style)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # 5 reviews over ~10 days (~3.5/week)
    pr1 = make_pr(1, owner, repo)  # other PR
    pr2 = make_pr(2, owner, repo, base_time=start, created_delta_hours=48)  # another PR by owner
    review1 = make_review(10, 1, user, start + timedelta(hours=2))
    review2 = make_review(11, 1, user, start + timedelta(hours=4))
    review3 = make_review(12, 1, user, start + timedelta(days=3))
    review4 = make_review(13, 2, user, start + timedelta(days=5))  # diff PR
    review5 = make_review(14, 2, user, start + timedelta(days=7))

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        reviews=[review1, review2, review3, review4, review5],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ReviewsGiven()
    res = metric.run(context)

    assert res.metric_slug == "reviews_given"
    assert res.details["review_count"] == 5
    assert res.details["reviews_per_week"] > 3.0
    assert len(res.details["review_pr_numbers"]) == 2  # distinct PRs
    assert "reviews given" in res.summary


def test_reviews_given_minimal():
    """Low volume."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr = make_pr(1, owner, repo)
    review = make_review(10, 1, user, DEFAULT_START)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
        reviews=[review],
    )
    context = make_context(bundle, user_login="alice")

    metric = ReviewsGiven()
    res = metric.run(context)

    assert res.details["review_count"] == 1
    assert res.details["reviews_per_week"] < 1.0  # over default 10 days


def test_reviews_given_none():
    """Edge: no reviews."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = ReviewsGiven()
    res = metric.run(context)

    assert res.details["review_count"] == 0
    assert res.details["reviews_per_week"] == 0.0
    assert res.details["review_pr_numbers"] == []
