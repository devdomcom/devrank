from datetime import timedelta

from impact.metrics.plugins.authored.dependency_change_rate import DependencyChangeRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_dependency_change_rate():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: has dep change (package.json)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "package.json", pr_number=1)
    f2 = make_file("sha2", "src/app.py", pr_number=1)  # non-dep
    # PR2: no deps
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=10)
    f3 = make_file("sha3", "src/main.py", pr_number=2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2, f3],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DependencyChangeRate()
    res = metric.run(context)

    assert res.metric_slug == "dependency_change_rate"
    assert res.details["dep_rate"] == 50.0  # 1/2 PRs
    assert res.details["dep_per_month"] == 1.0  # 1 change / (10/30 month)
    assert "Dep change rate" in res.summary
    assert any(p["dep_changes"] == 1 for p in res.details["per_pr"])
