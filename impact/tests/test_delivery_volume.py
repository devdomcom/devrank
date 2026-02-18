from datetime import timedelta

from impact.metrics.plugins.authored.delivery_volume import DeliveryVolume
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_counts_nontrivial_merged_prs():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: merged, 50 additions (small, counted)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0,
                  merged_delta_hours=48, additions=50, deletions=10)
    # PR2: merged, 200 additions (medium, counted)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24,
                  merged_delta_hours=72, additions=200, deletions=30)
    # PR3: merged, trivial (5 additions, excluded)
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=48,
                  merged_delta_hours=96, additions=5, deletions=2)
    # PR4: not merged (excluded)
    pr4 = make_pr(4, user, repo, base_time=start, created_delta_hours=72,
                  merged_delta_hours=None, additions=100, deletions=20)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4],
    )
    end = start + timedelta(weeks=2)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryVolume()
    res = metric.run(context)

    assert res.metric_slug == "delivery_volume"
    assert res.details["total_merged"] == 3
    assert res.details["nontrivial_merged"] == 2
    assert res.details["trivial_excluded"] == 1
    assert res.details["merged_per_week"] == 1.0  # 2 non-trivial / 2 weeks


def test_empty_bundle_produces_no_data():
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    start = DEFAULT_START
    end = start + timedelta(weeks=6)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryVolume()
    res = metric.run(context)

    assert res.details["nontrivial_merged"] == 0
    assert res.details["merged_per_week"] == 0.0
    assert res.details.get("no_data") is True


def test_short_period_low_count_triggers_no_data():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0,
                  merged_delta_hours=24, additions=50, deletions=10)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
    )
    # Short period: 10 days < 14 day threshold
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryVolume()
    res = metric.run(context)

    assert res.details["nontrivial_merged"] == 1
    assert res.details.get("no_data") is True  # <14 days AND <3 merged


def test_long_period_scores_normally():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    prs = []
    for i in range(9):
        prs.append(make_pr(
            i + 1, user, repo, base_time=start,
            created_delta_hours=i * 48,
            merged_delta_hours=i * 48 + 24,
            additions=100, deletions=20,
        ))

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=prs,
    )
    end = start + timedelta(weeks=6)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryVolume()
    res = metric.run(context)

    assert res.details["nontrivial_merged"] == 9
    assert res.details["trivial_excluded"] == 0
    assert res.details.get("no_data") is None
    # 9 PRs / 6 weeks = 1.5/week
    assert res.details["merged_per_week"] == 1.5


def test_all_trivial_prs_produce_no_data():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # All PRs are trivial (<10 lines)
    prs = [
        make_pr(i + 1, user, repo, base_time=start,
                created_delta_hours=i * 24, merged_delta_hours=i * 24 + 12,
                additions=3, deletions=2)
        for i in range(5)
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=prs,
    )
    end = start + timedelta(weeks=6)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryVolume()
    res = metric.run(context)

    assert res.details["total_merged"] == 5
    assert res.details["nontrivial_merged"] == 0
    assert res.details["trivial_excluded"] == 5
    assert res.details.get("no_data") is True
