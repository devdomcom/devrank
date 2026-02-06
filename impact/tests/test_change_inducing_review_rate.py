from datetime import timedelta

from impact.metrics.plugins.influence.change_inducing_review_rate import ChangeInducingReviewRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_user,
    make_repo,
    make_pr,
    make_commit,
    make_review,
    make_bundle,
    make_context,
)


def test_change_inducing_high():
    """Reviews inducing commits with proximity/correlation."""
    user = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Inducing: review -> quick commit
    pr1 = make_pr(1, author, repo, base_time=start)
    review1 = make_review(10, 1, user, start + timedelta(hours=1))
    commit1 = make_commit("sha1", author, start + timedelta(hours=3), 1)

    # Non-inducing: intervening other review
    pr2 = make_pr(2, author, repo, base_time=start, created_delta_hours=24)
    review2 = make_review(20, 2, user, start + timedelta(hours=25))
    other_review = make_review(21, 2, make_user(id=4, login="charlie"), start + timedelta(hours=26))
    # commit after other

    bundle = make_bundle(
        users=[user, author, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit1],
        reviews=[review1, review2, other_review],
    )
    end = start + timedelta(days=2)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeInducingReviewRate()
    res = metric.run(context)

    assert res.metric_slug == "change_inducing_review_rate"
    assert res.details["total_reviews"] == 2
    assert res.details["inducing_count"] == 1
    assert res.details["inducing_rate"] == 0.5
    assert any(p["induced_change"] for p in res.details["per_review"] if p["pr_number"] == 1)


def test_change_inducing_none():
    """No inducing commits."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = ChangeInducingReviewRate()
    res = metric.run(context)

    assert res.details["total_reviews"] == 0
    assert res.details["inducing_count"] == 0
    assert res.details["inducing_rate"] == 0.0
