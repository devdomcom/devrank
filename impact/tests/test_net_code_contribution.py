from datetime import timedelta

from impact.metrics.plugins.authored.net_code_contribution import NetCodeContribution
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_net_code_contribution():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: net +100 (create)
    pr1 = make_pr(1, user, repo, base_time=start, additions=150, deletions=50)
    # PR2: net -20 (refactor)
    pr2 = make_pr(2, user, repo, base_time=start, additions=80, deletions=100)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = NetCodeContribution()
    res = metric.run(context)

    assert res.metric_slug == "net_code_contribution"
    assert res.details["net_lines"] == 80  # 100 + (-20)
    assert res.details["add_to_del_ratio"] == 1.5333333333333334  # 230/150
    assert res.details["net_per_month"] == 80.0  # 80 /1 (months max1)
    assert "Net code:" in res.summary
    assert any(p["net"] == 100 for p in res.details["per_pr"])
