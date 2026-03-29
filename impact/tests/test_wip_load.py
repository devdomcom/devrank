"""Tests for WIPLoad (Lean WIP) metric."""

from datetime import timedelta

from impact.metrics.plugins.authored.wip_load import WIPLoad
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_wip_load_basic():
    """Two overlapping PRs should produce max_wip >= 2."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: open day 0 – closed day 3
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(days=3),
    )
    # PR2: open day 1 – closed day 4
    pr2 = make_pr(
        2, user, repo,
        created_at=start + timedelta(days=1),
        merged_at=start + timedelta(days=4),
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
    )
    end = start + timedelta(days=5)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = WIPLoad().run(ctx)

    assert result.metric_slug == "wip_load"
    assert result.details["max_concurrent_prs"] >= 2
    assert result.details["avg_concurrent_prs"] > 0
    assert result.details["total_prs_in_period"] == 2
    # Daily WIP list should have 6 entries (day 0-5 inclusive)
    assert len(result.details["daily_wip"]) == 6


def test_wip_load_no_overlap():
    """Sequential non-overlapping PRs should have max_wip == 1."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: day 0 – closed day 1
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(days=1),
    )
    # PR2: open day 3 – closed day 4  (gap on day 2)
    pr2 = make_pr(
        2, user, repo,
        created_at=start + timedelta(days=3),
        merged_at=start + timedelta(days=4),
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
    )
    end = start + timedelta(days=5)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = WIPLoad().run(ctx)

    assert result.details["max_concurrent_prs"] == 1


def test_wip_load_open_pr_still_counts():
    """A PR still open at the end of the window should count."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR open entire window, never closed
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=None)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
    )
    end = start + timedelta(days=3)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = WIPLoad().run(ctx)

    # Open PR counts every day
    for day_entry in result.details["daily_wip"]:
        assert day_entry["wip"] >= 1


def test_wip_load_pre_window_pr():
    """PR created before the window but still open during it should count."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR created 5 days before the window, closed on day 2 of window
    pr1 = make_pr(
        1, user, repo,
        created_at=start - timedelta(days=5),
        merged_at=start + timedelta(days=2),
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
    )
    end = start + timedelta(days=4)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = WIPLoad().run(ctx)

    # Days 0 and 1 should have WIP 1, day 2+ should have 0
    wip_list = result.details["daily_wip"]
    assert wip_list[0]["wip"] == 1  # day 0
    assert wip_list[1]["wip"] == 1  # day 1


def test_wip_load_no_dates():
    """No date range returns no_data."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    ctx = make_context(bundle, user_login="alice", start_date=None, end_date=None)

    result = WIPLoad().run(ctx)

    assert result.details["no_data"] is True


def test_wip_load_no_prs():
    """No PRs in the window should return no_data."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    start = DEFAULT_START
    end = start + timedelta(days=10)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = WIPLoad().run(ctx)

    assert result.details["no_data"] is True
    assert result.details["max_concurrent_prs"] == 0
    assert result.details["avg_concurrent_prs"] == 0
