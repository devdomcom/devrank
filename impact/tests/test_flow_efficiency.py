"""Tests for FlowEfficiency (Kanban) metric."""

from datetime import timedelta

from impact.metrics.plugins.authored.flow_efficiency import FlowEfficiency
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_flow_efficiency_single_pr():
    """A single merged PR with commits should compute efficiency."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR created at hour 0, merged at hour 24 = 24h lead time
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(hours=24),
    )

    # Two commits 2 hours apart → 2h gap + 1h first-commit bonus = 3h active
    c1 = make_commit("sha1", user, start + timedelta(hours=1), 1)
    c2 = make_commit("sha2", user, start + timedelta(hours=3), 1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[c1, c2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    assert result.metric_slug == "flow_efficiency"
    assert result.details["pr_count"] == 1
    # 3h active / 24h total ≈ 12.5%
    assert result.details["median_efficiency"] > 0
    per_pr = result.details["per_pr"]
    assert len(per_pr) == 1
    assert per_pr[0]["total_hours"] == 24.0
    assert per_pr[0]["active_hours"] == 3.0  # 2h gap + 1h first-commit
    assert abs(per_pr[0]["efficiency"] - 12.5) < 0.1


def test_flow_efficiency_gap_cap():
    """Inter-commit gaps > 4h should be capped at 4h."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # 48h lead time
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(hours=48),
    )

    # Two commits 10 hours apart → capped at 4h + 1h first-commit = 5h active
    c1 = make_commit("sha1", user, start + timedelta(hours=2), 1)
    c2 = make_commit("sha2", user, start + timedelta(hours=12), 1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[c1, c2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    per_pr = result.details["per_pr"]
    # 4h (capped gap) + 1h (first-commit) = 5h
    assert per_pr[0]["active_hours"] == 5.0


def test_flow_efficiency_no_commits():
    """A merged PR with no commits should have 0% efficiency."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(hours=12),
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    per_pr = result.details["per_pr"]
    assert per_pr[0]["efficiency"] == 0.0
    assert per_pr[0]["active_hours"] == 0


def test_flow_efficiency_only_other_author_commits():
    """Commits by other authors should not count for the PR author's active time."""
    user = make_user(id=1, login="alice")
    other = make_user(id=3, login="bob")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(hours=24),
    )

    # Commits by "bob", not "alice"
    c1 = make_commit("sha1", other, start + timedelta(hours=1), 1)

    bundle = make_bundle(
        users=[user, other, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[c1],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    per_pr = result.details["per_pr"]
    # No author commits → 0% efficiency
    assert per_pr[0]["efficiency"] == 0.0


def test_flow_efficiency_multiple_prs_median():
    """Median efficiency across multiple PRs."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: 10h lead time, single commit → 1h active / 10h = 10%
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=10))
    c1 = make_commit("sha1", user, start + timedelta(hours=1), 1)

    # PR2: 2h lead time, single commit → 1h active / 2h = 50%
    pr2 = make_pr(2, user, repo,
                  created_at=start + timedelta(hours=20),
                  merged_at=start + timedelta(hours=22))
    c2 = make_commit("sha2", user, start + timedelta(hours=21), 2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[c1, c2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    assert result.details["pr_count"] == 2
    # Median of 10% and 50% = 30%
    assert abs(result.details["median_efficiency"] - 30.0) < 0.1


def test_flow_efficiency_no_merged_prs():
    """No merged PRs returns no_data."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Open PR, not merged
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=None)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    assert result.details["no_data"] is True
    assert result.details["pr_count"] == 0


def test_flow_efficiency_active_capped_at_total():
    """Active hours should never exceed total lead time."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR with only 30min lead time but a commit at minute 10
    pr1 = make_pr(
        1, user, repo,
        created_at=start,
        merged_at=start + timedelta(minutes=30),
    )
    c1 = make_commit("sha1", user, start + timedelta(minutes=10), 1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[c1],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = FlowEfficiency().run(ctx)

    per_pr = result.details["per_pr"]
    # active_hours should be capped at total_hours (0.5h)
    assert per_pr[0]["active_hours"] <= per_pr[0]["total_hours"]
    assert per_pr[0]["efficiency"] == 100.0
