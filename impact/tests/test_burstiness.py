from datetime import timedelta

from impact.metrics.plugins.authored.burstiness import Burstiness
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


def test_burstiness_steady():
    """Steady: even activity spread (low burst ratio)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Even spread across weeks (larger deltas ensure separation regardless of start weekday)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    commit1 = make_commit("sha1", user, start + timedelta(days=3), 1)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24 * 14)
    commit2 = make_commit("sha2", user, start + timedelta(days=17), 2)
    review1 = make_review(1, 2, user, start + timedelta(days=18))

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit1, commit2],
        reviews=[review1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = Burstiness()
    res = metric.run(context)

    assert res.metric_slug == "burstiness"
    assert res.details["active_weeks"] >= 2
    assert res.details["burst_ratio"] <= 2.0  # even/steady
    assert res.details["max_weekly"] <= 3
    assert len(res.details["per_week"]) >= 2


def test_burstiness_bursty():
    """Bursty: uneven, one heavy week."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Heavy in one week, sparse in another (deltas ensure distinct weeks)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    commit1 = make_commit("sha1", user, start + timedelta(hours=2), 1)
    commit2 = make_commit("sha2", user, start + timedelta(hours=4), 1)
    review1 = make_review(1, 1, user, start + timedelta(hours=6))
    # Sparse later
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24 * 14)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit1, commit2],
        reviews=[review1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = Burstiness()
    res = metric.run(context)

    assert res.details["active_weeks"] >= 2
    assert res.details["burst_ratio"] > 1.5  # bursty (uneven)
    assert res.details["max_weekly"] >= 3
    assert "per_week" in res.details


def test_burstiness_no_activity():
    """Edge: no activity."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = Burstiness()
    res = metric.run(context)

    assert res.details["active_weeks"] == 0
    assert res.details["burst_ratio"] == 0.0
    assert res.details["total_activities"] == 0
    assert res.details["per_week"] == {}
