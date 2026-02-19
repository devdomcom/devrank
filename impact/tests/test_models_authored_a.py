"""
Tests for bug fixes:
  - Issue 2:  PullRequest model `draft` field
  - Issue 3:  burstiness uses total period weeks (not active weeks only)
  - Issue 23: pr_size_distribution trivial PRs not double-counted in small
  - Issue 28: active_weeks clamps dates to analysis window
"""

from datetime import UTC, datetime, timedelta

from impact.domain.models import PullRequest, PullRequestState
from impact.metrics.plugins.authored.active_weeks import ActiveWeeks
from impact.metrics.plugins.authored.burstiness import Burstiness
from impact.metrics.plugins.authored.pr_size_distribution import PRSizeDistribution
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


# --------------------------------------------------------------------------- #
# Issue 2 – PullRequest.draft field
# --------------------------------------------------------------------------- #

def test_pull_request_draft_defaults_false():
    """PullRequest.draft should default to False when not specified."""
    user = make_user(id=1, login="alice")
    repo = make_repo(id=1, name="repo", owner=make_user(id=2, login="org"))
    pr = make_pr(1, user, repo)
    assert pr.draft is False


def test_pull_request_draft_true():
    """PullRequest.draft should accept True."""
    user = make_user(id=1, login="alice")
    repo = make_repo(id=1, name="repo", owner=make_user(id=2, login="org"))
    pr = make_pr(1, user, repo)
    pr.draft = True
    assert pr.draft is True


def test_pull_request_draft_field_on_model():
    """PullRequest model can be constructed with draft=True directly."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    from impact.domain.models import Branch
    base = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head = Branch(label="head", ref="feature-1", sha="sha2", user=user, repo=repo)

    pr = PullRequest(
        id=1,
        number=1,
        title="Draft PR",
        state=PullRequestState.OPEN,
        user=user,
        created_at=DEFAULT_START,
        repository=repo,
        base=base,
        head=head,
        commits=1,
        additions=10,
        deletions=5,
        changed_files=1,
        comments=0,
        review_comments=0,
        draft=True,
    )
    assert pr.draft is True


# --------------------------------------------------------------------------- #
# Issue 3 – burstiness avg uses active weeks (not total period weeks)
# --------------------------------------------------------------------------- #

def test_burstiness_uses_active_weeks():
    """
    With a 4-week analysis window but activity in only 1 week,
    avg_weekly should be total_activities / active_weeks (1 / 1 = 1.0).
    burst_ratio = max_weekly / avg_weekly = 1.0.
    """
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(weeks=4)  # 28-day window -> 4 total weeks

    # Single PR created in week 1
    pr = make_pr(1, user, repo, created_at=start + timedelta(days=1), merged_delta_hours=None)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = Burstiness()
    res = metric.run(context)

    assert res.metric_slug == "burstiness"
    # total_weeks should be 4 (28 days / 7)
    assert res.details["total_weeks"] == 4
    assert res.details["active_weeks"] == 1
    assert res.details["total_activities"] == 1
    # avg_weekly = 1 / 1 = 1.0; max_weekly = 1; burst_ratio = 1 / 1 = 1.0
    assert res.details["avg_weekly"] == 1.0
    assert res.details["burst_ratio"] == 1.0


def test_burstiness_no_activity():
    """Edge: no activity -> burst_ratio = 0."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    metric = Burstiness()
    res = metric.run(context)
    assert res.details["burst_ratio"] == 0.0
    assert res.details["total_activities"] == 0


# --------------------------------------------------------------------------- #
# Issue 23 – pr_size_distribution trivial vs small mutual exclusivity
# --------------------------------------------------------------------------- #

def test_pr_size_distribution_trivial_not_in_small():
    """A trivial PR (< 10 changes) should appear in trivial_prs but NOT in small_prs."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    # Trivial PR: 3 additions + 2 deletions = 5 total changes (< 10)
    pr_trivial = make_pr(1, user, repo, additions=3, deletions=2, merged_delta_hours=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr_trivial],
    )
    start = DEFAULT_START
    end = start + timedelta(days=7)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRSizeDistribution()
    res = metric.run(context)

    assert res.metric_slug == "pr_size_distribution"
    assert 1 in res.details["trivial_pr_numbers"]
    assert 1 not in res.details["small_pr_numbers"]
    assert 1 not in res.details["medium_pr_numbers"]
    assert 1 not in res.details["large_pr_numbers"]


def test_pr_size_distribution_small_not_trivial():
    """A small PR (10-99 changes) should appear in small_prs but NOT in trivial_prs."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    # Small PR: 30 additions + 20 deletions = 50 total (10-99 range)
    pr_small = make_pr(2, user, repo, additions=30, deletions=20, merged_delta_hours=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr_small],
    )
    start = DEFAULT_START
    end = start + timedelta(days=7)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRSizeDistribution()
    res = metric.run(context)

    assert 2 in res.details["small_pr_numbers"]
    assert 2 not in res.details["trivial_pr_numbers"]
    assert 2 not in res.details["medium_pr_numbers"]
    assert 2 not in res.details["large_pr_numbers"]


def test_pr_size_distribution_categories_mutually_exclusive():
    """Each PR should appear in exactly one category."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr_trivial = make_pr(1, user, repo, additions=3, deletions=2, merged_delta_hours=1)    # 3+2*0.5=4 weighted
    pr_small = make_pr(2, user, repo, additions=30, deletions=20, merged_delta_hours=1)    # 30+20*0.5=40 weighted
    pr_medium = make_pr(3, user, repo, additions=300, deletions=200, merged_delta_hours=1)  # 300+200*0.5=400 weighted
    pr_large = make_pr(4, user, repo, additions=900, deletions=500, merged_delta_hours=1)   # 900+500*0.5=1150 weighted

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr_trivial, pr_small, pr_medium, pr_large],
    )
    start = DEFAULT_START
    end = start + timedelta(days=7)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRSizeDistribution()
    res = metric.run(context)

    # Each PR in exactly one bucket
    all_numbers = (
        res.details["trivial_pr_numbers"]
        + res.details["small_pr_numbers"]
        + res.details["medium_pr_numbers"]
        + res.details["large_pr_numbers"]
    )
    assert sorted(all_numbers) == [1, 2, 3, 4]
    assert len(all_numbers) == 4  # no duplicates


# --------------------------------------------------------------------------- #
# Issue 28 – active_weeks clamps dates to analysis window
# --------------------------------------------------------------------------- #

def test_active_weeks_excludes_merged_at_outside_window():
    """
    A PR with merged_at outside the analysis window should not contribute
    that date to active weeks.
    """
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = datetime(2026, 3, 1, tzinfo=UTC)
    end = datetime(2026, 3, 14, tzinfo=UTC)  # 2-week window

    # PR created inside window, but merged outside window (after end)
    pr = make_pr(
        1, user, repo,
        created_at=start + timedelta(days=2),
        merged_at=end + timedelta(days=30),  # way outside end
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ActiveWeeks()
    res = metric.run(context)

    # The created_at is in the window, so 1 active week from that.
    # The merged_at is outside the window and should NOT add another active week.
    active_week_list = res.details["active_weeks"]
    # created_at is March 3 -> week 10 of 2026
    # merged_at is April 13 -> week 16 of 2026 (should be excluded)
    for wk in active_week_list:
        # No week should be outside the March 1-14 window
        year_str, week_str = wk.split("-W")
        year = int(year_str)
        week_num = int(week_str)
        assert year == 2026
        # March 1 is week 9, March 14 is week 11
        assert 9 <= week_num <= 11, f"Week {wk} is outside the analysis window"


def test_active_weeks_excludes_commit_outside_window():
    """Commits with dates outside the analysis window should not be counted."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = datetime(2026, 6, 1, tzinfo=UTC)
    end = datetime(2026, 6, 14, tzinfo=UTC)  # 2-week window

    # PR inside window
    pr = make_pr(1, user, repo, created_at=start + timedelta(days=1))

    # Commit outside window (before start)
    commit_outside = make_commit(
        sha="abc123",
        author=user,
        date=start - timedelta(days=60),  # way before start
        pr_number=1,
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
        commits=[commit_outside],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ActiveWeeks()
    res = metric.run(context)

    # Only the PR created_at contributes; the commit outside window should not
    for wk in res.details["active_weeks"]:
        year_str, week_str = wk.split("-W")
        year = int(year_str)
        week_num = int(week_str)
        # June 1 is week 23, June 14 is week 24
        assert year == 2026
        assert 22 <= week_num <= 25, f"Week {wk} is outside the analysis window"


def test_active_weeks_all_inside_window():
    """When all dates are inside the window, they should all count."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = datetime(2026, 4, 1, tzinfo=UTC)
    end = datetime(2026, 4, 21, tzinfo=UTC)  # 3-week window

    pr1 = make_pr(1, user, repo, created_at=start + timedelta(days=1),
                  merged_at=start + timedelta(days=3))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(days=8),
                  merged_at=start + timedelta(days=10))
    pr3 = make_pr(3, user, repo, created_at=start + timedelta(days=15),
                  merged_at=start + timedelta(days=17))

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ActiveWeeks()
    res = metric.run(context)

    # 3 weeks of activity
    assert res.details["active_count"] >= 3
