from datetime import timedelta

from impact.metrics.plugins.influence.pr_merge_rate import PRMergeRate
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


def test_pr_merge_rate_high_influence():
    """Reviews leading to merge with proximity (no interveners)."""
    user = make_user(id=1, login="alice")  # reviewer
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR merged after user's review + commit (proximity)
    pr1 = make_pr(1, author, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
    review1 = make_review(10, 1, user, start + timedelta(hours=2))  # user's review
    commit1 = make_commit("sha1", author, start + timedelta(hours=5), 1)  # after review

    # Another PR with intervening other review (should not count)
    pr2 = make_pr(2, author, repo, base_time=start, created_delta_hours=48, merged_delta_hours=72)
    review2 = make_review(20, 2, user, start + timedelta(hours=50))  # user's
    other_review = make_review(21, 2, make_user(id=4, login="charlie"), start + timedelta(hours=52))
    commit2 = make_commit("sha2", author, start + timedelta(hours=55), 2)

    bundle = make_bundle(
        users=[user, author, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit1, commit2],
        reviews=[review1, review2, other_review],
    )
    end = start + timedelta(days=4)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRMergeRate()
    res = metric.run(context)

    assert res.metric_slug == "pr_merge_rate"
    assert res.details["total_reviews"] == 2
    assert res.details["effective_merges"] == 1  # only first
    assert res.details["merge_rate"] == 0.5
    assert any(p["led_to_merge"] for p in res.details["per_review"] if p["pr_number"] == 1)
    assert not any(p["led_to_merge"] for p in res.details["per_review"] if p["pr_number"] == 2)


def test_pr_merge_rate_none():
    """No effective merges."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = PRMergeRate()
    res = metric.run(context)

    assert res.details["total_reviews"] == 0
    assert res.details["effective_merges"] == 0
    assert res.details["merge_rate"] == 0.0
