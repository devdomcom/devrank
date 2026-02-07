from datetime import timedelta

from impact.metrics.plugins.authored.active_weeks import ActiveWeeks
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


def test_active_weeks_steady_contributions():
    """Test with activity spread across multiple weeks (steady)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Activity in 4 different weeks: PR create, commit, review, merged
    # Spaced ~7 days apart to cross weeks
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    commit1 = make_commit("sha1", user, start + timedelta(days=7), 1)
    review1 = make_review(1, 1, user, start + timedelta(days=14))
    # merged later
    pr2 = make_pr(
        2, user, repo, base_time=start, created_delta_hours=24 * 21, merged_delta_hours=24 * 22
    )  # ~3 weeks later

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit1],
        reviews=[review1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ActiveWeeks()
    res = metric.run(context)

    assert res.metric_slug == "active_weeks"
    # Granular lists surface absences (repurposed)
    assert len(res.details["active_weeks"]) >= 4
    assert len(res.details["inactive_weeks"]) > 0
    assert res.details["total_weeks"] > 0
    assert res.details["max_gap_weeks"] <= 2  # small gaps = steady
    assert res.details["active_ratio"] > 0.1
    assert res.details["activity_sources"]["prs"] == 2
    assert res.details["activity_sources"]["commits"] == 1
    assert res.details["activity_sources"]["reviews"] == 1
    # Summary contains numbers, not key (keep minimal)
    assert "active weeks" in res.summary


def test_active_weeks_bursty_activity():
    """Test with all activity in short burst (1 week)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # All in same week
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=2)
    commit1 = make_commit("sha1", user, start + timedelta(hours=1), 1)
    review1 = make_review(1, 1, user, start + timedelta(hours=3))

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[commit1],
        reviews=[review1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ActiveWeeks()
    res = metric.run(context)

    # Granular: 1 active, large gap marks burst/disengagement
    assert len(res.details["active_weeks"]) == 1
    assert res.details["max_gap_weeks"] >= 3
    assert res.details["active_ratio"] <= 0.3


def test_active_weeks_no_activity():
    """Edge case: no PRs/commits/reviews."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = ActiveWeeks()
    res = metric.run(context)

    # No activity: all weeks inactive, max_gap = total
    assert len(res.details["active_weeks"]) == 0
    assert len(res.details["inactive_weeks"]) > 0
    assert res.details["max_gap_weeks"] >= 1
    assert res.details["active_ratio"] == 0.0
    assert res.details["activity_sources"]["prs"] == 0
    assert "active weeks" in res.summary
