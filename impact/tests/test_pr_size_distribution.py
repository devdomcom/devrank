from datetime import timedelta

from impact.metrics.plugins.authored.pr_size_distribution import PRSizeDistribution
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_pr_size_distribution_with_various_sizes():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PRs with different sizes
    pr1 = make_pr(
        1, user, repo, base_time=start, created_delta_hours=0, additions=10, deletions=5
    )  # small, trivial
    pr2 = make_pr(
        2, user, repo, base_time=start, created_delta_hours=24, additions=50, deletions=20
    )  # small
    pr3 = make_pr(
        3, user, repo, base_time=start, created_delta_hours=48, additions=200, deletions=100
    )  # medium
    pr4 = make_pr(
        4, user, repo, base_time=start, created_delta_hours=72, additions=500, deletions=600
    )  # large (1100 changes)
    pr5 = make_pr(
        5, user, repo, base_time=start, created_delta_hours=96, additions=0, deletions=0
    )  # trivial

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4, pr5],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRSizeDistribution()
    res = metric.run(context)

    assert res.metric_slug == "pr_size_distribution"
    assert res.details["pr_count"] == 5
    # Additions: 10,50,200,500,0 -> sorted: 0,10,50,200,500 median=50
    assert res.details["additions_median"] == 50.0
    # Deletions: 5,20,100,600,0 -> 0,5,20,100,600 median=20
    assert res.details["deletions_median"] == 20.0
    # Changes: 15,70,300,1100,0 -> 0,15,70,300,1100 median=70
    assert res.details["changes_median"] == 70.0
    # P75: additions p75 = 200 (75% of 4th index? wait, for 5 items: k=4*0.75=3, f=3, c=4, interp 200*(4-3)+500*(3-3)=200
    # Actually, percentile function: for pct=0.75, k=(5-1)*0.75=3, f=3, c=4, since f!=c, 200*(4-3) + 500*(3-3)=200? Wait, formula is sorted[f]*(c-k) + sorted[c]*(k-f), wait let's check code:
    # return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)
    # For k=3, f=3, c=4, sorted[3]=200, sorted[4]=500, 200*(4-3) + 500*(3-3) = 200*1 + 500*0 = 200
    # Yes, 200
    assert res.details["additions_p75"] == 200.0
    # Deletions p75: sorted 0,5,20,100,600 -> same 100
    assert res.details["deletions_p75"] == 100.0
    # Changes p75: 0,15,70,300,1100 -> 300
    assert res.details["changes_p75"] == 300.0
    assert res.details["small_pr_count"] == 2  # pr1(15), pr2(70) — trivial pr5 no longer double-counted
    assert res.details["medium_pr_count"] == 1  # pr3(300)
    assert res.details["large_pr_count"] == 1  # pr4(1100)
    assert res.details["small_pr_percent"] == 40.0
    assert res.details["medium_pr_percent"] == 20.0
    assert res.details["large_pr_percent"] == 20.0
    assert res.details["small_pr_numbers"] == [1, 2]
    assert res.details["medium_pr_numbers"] == [3]
    assert res.details["large_pr_numbers"] == [4]
    assert res.details["trivial_pr_numbers"] == [5]  # <10 changes


def test_pr_size_distribution_no_prs():
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = PRSizeDistribution()
    res = metric.run(context)

    assert res.metric_slug == "pr_size_distribution"
    assert res.details["pr_count"] == 0
    assert res.details["additions_median"] == 0.0
    assert res.summary == "No PRs found in the window."


def test_pr_size_distribution_single_pr():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr = make_pr(1, user, repo, additions=100, deletions=50)
    bundle = make_bundle(users=[user, owner], repositories=[repo], pull_requests=[pr])
    context = make_context(bundle, user_login="alice")

    metric = PRSizeDistribution()
    res = metric.run(context)

    assert res.details["pr_count"] == 1
    assert res.details["additions_median"] == 100.0
    assert res.details["deletions_median"] == 50.0
    assert res.details["changes_median"] == 150.0
    assert res.details["small_pr_count"] == 0
    assert res.details["medium_pr_count"] == 1
    assert res.details["large_pr_count"] == 0
    assert res.details["small_pr_numbers"] == []
    assert res.details["medium_pr_numbers"] == [1]
    assert res.details["large_pr_numbers"] == []
    assert res.details["trivial_pr_numbers"] == []  # 150 >10
