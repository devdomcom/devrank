from datetime import datetime, timezone, timedelta

from impact.metrics.plugins.pr_throughput import PRThroughput
from impact.tests.conftest import (
    DEFAULT_START,
    make_user,
    make_repo,
    make_pr,
    make_bundle,
    make_context,
)


def test_pr_throughput_counts_opened_and_merged():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Using delta-based creation for cleaner test code
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=48)  # merged inside window
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=120, merged_delta_hours=None)  # open only (5 days in)
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=-120, merged_delta_hours=None)  # before window

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRThroughput()
    res = metric.run(context)

    assert res.metric_slug == "pr_throughput"
    assert res.details["opened_count"] == 2  # pr1 and pr2
    assert res.details["merged_count"] == 1  # only pr1
    assert res.details["merge_ratio"] == 0.5
    assert res.details["merged_pr_numbers"] == [1]
