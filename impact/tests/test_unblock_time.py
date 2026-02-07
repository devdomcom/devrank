from datetime import timedelta

from impact.metrics.plugins.influence.unblock_time import UnblockTime
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


def test_unblock_fast():
    """Fast unblock after CR + commit."""
    user = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr = make_pr(1, author, repo, base_time=start)
    cr = make_review(10, 1, user, start + timedelta(hours=1), state="changes_requested")
    commit = make_commit("sha1", author, start + timedelta(hours=3), 1)  # after CR
    re_review = make_review(11, 1, user, start + timedelta(hours=5))  # fast unblock

    bundle = make_bundle(
        users=[user, author, owner],
        repositories=[repo],
        pull_requests=[pr],
        commits=[commit],
        reviews=[cr, re_review],
    )
    end = start + timedelta(days=1)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = UnblockTime()
    res = metric.run(context)

    assert res.metric_slug == "unblock_time"
    assert res.details["cr_count"] == 1
    assert res.details["median_hours"] <= 2.0  # fast (excludes author lag)
    assert "unblock" in res.summary


def test_unblock_none():
    """No CRs."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = UnblockTime()
    res = metric.run(context)

    assert res.details["cr_count"] == 0
    assert res.details["median_hours"] == 0.0
