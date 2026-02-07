from datetime import timedelta

from impact.metrics.plugins.influence.review_turnaround_time import ReviewTurnaroundTime
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_review_turnaround_fast():
    """Fast turnaround on PRs."""
    user = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Fast response
    pr1 = make_pr(1, author, repo, base_time=start, created_delta_hours=0)
    review1 = make_review(10, 1, user, start + timedelta(hours=6))

    pr2 = make_pr(2, author, repo, base_time=start, created_delta_hours=24)
    review2 = make_review(20, 2, user, start + timedelta(hours=30))  # 6h after open

    bundle = make_bundle(
        users=[user, author, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        reviews=[review1, review2],
    )
    end = start + timedelta(days=2)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ReviewTurnaroundTime()
    res = metric.run(context)

    assert res.metric_slug == "review_turnaround_time"
    assert res.details["reviewed_prs"] == 2
    assert res.details["median_hours"] <= 6.0  # fast
    assert "median" in res.summary


def test_review_turnaround_slow():
    """Slow/no response."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = ReviewTurnaroundTime()
    res = metric.run(context)

    assert res.details["reviewed_prs"] == 0
    assert res.details["median_hours"] == 0.0
