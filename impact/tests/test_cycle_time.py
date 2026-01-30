from datetime import timedelta

from impact.metrics.plugins.cycle_time import CycleTime
from impact.tests.conftest import (
    DEFAULT_START,
    make_user,
    make_repo,
    make_pr,
    make_bundle,
    make_context,
)


def test_cycle_time_median_and_p75():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=10))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=1), merged_at=start + timedelta(hours=20))
    pr3 = make_pr(3, user, repo, created_at=start + timedelta(hours=2), merged_at=None)  # open, ignored

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=10))

    metric = CycleTime()
    res = metric.run(context)

    assert res.metric_slug == "cycle_time"
    assert res.details["merged_count"] == 2
    # durations: 10h, 19h => median 14.5, p75 16.75
    assert res.details["median_hours"] == 14.5
    assert abs(res.details["p75_hours"] - 16.75) < 1e-6
