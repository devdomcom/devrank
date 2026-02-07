from datetime import timedelta

from impact.metrics.plugins.authored.trivial_contribution_rate import TrivialContributionRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_trivial_contribution_rate_with_mixed_prs():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PRs: 2 trivial (<10), 1 small (10-99), 2 medium (100-999)
    pr1 = make_pr(1, user, repo, additions=5, deletions=3)  # 8 trivial
    pr2 = make_pr(2, user, repo, additions=0, deletions=0)  # 0 trivial
    pr3 = make_pr(3, user, repo, additions=20, deletions=10)  # 30 small
    pr4 = make_pr(4, user, repo, additions=50, deletions=20)  # 70 small
    pr5 = make_pr(5, user, repo, additions=200, deletions=100)  # 300 medium

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4, pr5],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = TrivialContributionRate()
    res = metric.run(context)

    assert res.metric_slug == "trivial_contribution_rate"
    assert res.details["total_pr_count"] == 5
    assert res.details["trivial_pr_count"] == 2  # pr1, pr2
    assert res.details["trivial_rate"] == 40.0
    assert res.details["period_days"] == 10.0
    assert res.details["trivial_prs_per_day"] == 0.2
    assert res.details["trivial_pr_numbers"] == [1, 2]
    assert res.summary == "0.20 trivial PRs per day"


def test_trivial_contribution_rate_no_trivial_prs():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr = make_pr(1, user, repo, additions=50, deletions=20)  # 70 small
    bundle = make_bundle(users=[user, owner], repositories=[repo], pull_requests=[pr])
    context = make_context(bundle, user_login="alice")

    metric = TrivialContributionRate()
    res = metric.run(context)

    assert res.details["total_pr_count"] == 1
    assert res.details["trivial_pr_count"] == 0
    assert res.details["trivial_rate"] == 0.0
    assert res.details["period_days"] == 10.0
    assert res.details["trivial_prs_per_day"] == 0.0
    assert res.details["trivial_pr_numbers"] == []
    assert res.summary == "0.00 trivial PRs per day"


def test_trivial_contribution_rate_all_trivial():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr1 = make_pr(1, user, repo, additions=1, deletions=1)  # 2 trivial
    pr2 = make_pr(2, user, repo, additions=0, deletions=5)  # 5 trivial
    bundle = make_bundle(users=[user, owner], repositories=[repo], pull_requests=[pr1, pr2])
    context = make_context(bundle, user_login="alice")

    metric = TrivialContributionRate()
    res = metric.run(context)

    assert res.details["total_pr_count"] == 2
    assert res.details["trivial_pr_count"] == 2
    assert res.details["trivial_rate"] == 100.0
    assert res.details["period_days"] == 10.0
    assert res.details["trivial_prs_per_day"] == 0.2
    assert res.details["trivial_pr_numbers"] == [1, 2]
    assert res.summary == "0.20 trivial PRs per day"


def test_trivial_contribution_rate_no_prs():
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = TrivialContributionRate()
    res = metric.run(context)

    assert res.details["total_pr_count"] == 0
    assert res.details["trivial_pr_count"] == 0
    assert res.details["trivial_rate"] == 0.0
    assert res.details["period_days"] == 10.0
    assert res.details["trivial_prs_per_day"] == 0.0
    assert res.details["trivial_pr_numbers"] == []
    assert res.summary == "0.00 trivial PRs per day"
