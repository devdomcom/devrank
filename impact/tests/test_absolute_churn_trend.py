from datetime import timedelta

from impact.metrics.plugins.authored.absolute_churn_trend import AbsoluteChurnTrend
from impact.metrics.utils import compute_absolute_churn_trend
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_absolute_churn_trend_basic():
    """Churn should aggregate additions/deletions per day."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # Day 1: PR1 adds 10 deletes 5, PR2 adds 3 deletes 2
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", additions=10, deletions=5, changes=15, pr_number=1)

    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=1)
    f2 = make_file("sha2", "src/b.py", additions=3, deletions=2, changes=5, pr_number=2)

    # Day 2: PR3 adds 4 deletes 1
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=24)
    f3 = make_file("sha3", "src/c.py", additions=4, deletions=1, changes=5, pr_number=3)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        files=[f1, f2, f3],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = AbsoluteChurnTrend()
    res = metric.run(context)

    assert res.metric_slug == "absolute_churn_trend"
    assert res.details["total_additions"] == 17
    assert res.details["total_deletions"] == 8
    assert res.details["total_churn"] == 25
    assert res.details["max_daily_churn"] == 20  # Day 1: 15 + 5

    per_day = res.details["per_day"]
    assert len(per_day) == 2
    assert per_day[0]["date"] == start.date().isoformat()
    assert per_day[0]["additions"] == 13
    assert per_day[0]["deletions"] == 7
    assert per_day[0]["churn"] == 20
    assert per_day[0]["pr_count"] == 2

    assert per_day[1]["additions"] == 4
    assert per_day[1]["deletions"] == 1
    assert per_day[1]["churn"] == 5
    assert per_day[1]["pr_count"] == 1


def test_absolute_churn_trend_excludes_generated_files():
    """Generated files should not contribute to churn."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", additions=4, deletions=2, changes=6, pr_number=1)
    f2 = make_file("sha2", "package-lock.json", additions=100, deletions=50, changes=150, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = AbsoluteChurnTrend()
    res = metric.run(context)

    assert res.details["total_additions"] == 4
    assert res.details["total_deletions"] == 2
    assert res.details["total_churn"] == 6
    assert res.details["max_daily_churn"] == 6


def test_absolute_churn_trend_no_data():
    """No PRs should return no_data flag."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = AbsoluteChurnTrend()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["total_churn"] == 0


def test_compute_absolute_churn_trend_helper():
    """Direct test of the helper function."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", additions=2, deletions=3, changes=5, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    result = compute_absolute_churn_trend(context.ledger, [pr1], start_date=start, end_date=start + timedelta(days=30))

    assert result["total_additions"] == 2
    assert result["total_deletions"] == 3
    assert result["total_churn"] == 5
    assert result["max_daily_churn"] == 5
    assert result["per_day"][0]["pr_count"] == 1


def test_absolute_churn_trend_no_file_data_fallback():
    """Fallback to PR-level additions/deletions when file data missing."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=7, deletions=4)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = AbsoluteChurnTrend()
    res = metric.run(context)

    assert res.details["total_additions"] == 7
    assert res.details["total_deletions"] == 4
    assert res.details["total_churn"] == 11


def test_absolute_churn_trend_short_period():
    """Short period with single PR should flag no_data."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", additions=3, deletions=2, changes=5, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=3)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = AbsoluteChurnTrend()
    res = metric.run(context)

    assert res.details["no_data"] is True
