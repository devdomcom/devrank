from datetime import timedelta

from impact.metrics.plugins.authored.pr_throughput import PRThroughput
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_pr_throughput_counts_opened_and_merged():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: created in window, merged in window
    pr1 = make_pr(
        1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=48
    )
    # PR2: created in window, not merged
    pr2 = make_pr(
        2, user, repo, base_time=start, created_delta_hours=120, merged_delta_hours=None
    )
    # PR3: created before window — excluded from both counts
    pr3 = make_pr(
        3, user, repo, base_time=start, created_delta_hours=-120, merged_delta_hours=None
    )
    # PR4/PR5: created before window — excluded (Issue 8 fix: both counts
    # use same PR list filtered by created_at to avoid disjoint populations)
    pr4 = make_pr(4, user, repo, base_time=start, created_delta_hours=-240, merged_delta_hours=24)
    pr5 = make_pr(5, user, repo, base_time=start, created_delta_hours=-360, merged_delta_hours=72)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4, pr5],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRThroughput()
    res = metric.run(context)

    assert res.metric_slug == "pr_throughput"
    assert res.details["opened_count"] == 2  # pr1 and pr2 (in-window only)
    assert res.details["merged_count"] == 1  # only pr1 (created in window and merged)
    assert res.details["merge_ratio"] == 0.5  # 1/2, always <= 1.0
    assert res.details["merged_pr_numbers"] == [1]
