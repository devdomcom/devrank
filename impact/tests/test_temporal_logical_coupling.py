from datetime import timedelta

from impact.metrics.plugins.authored.temporal_logical_coupling import TemporalLogicalCoupling
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_temporal_logical_coupling():
    """Coupling ratio should reflect shared revisions over average revisions."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1 touches A + B
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "src/b.py", changes=8, pr_number=1)

    # PR2 touches A + B
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
    f3 = make_file("sha3", "src/a.py", changes=5, pr_number=2)
    f4 = make_file("sha4", "src/b.py", changes=7, pr_number=2)

    # PR3 touches A only
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=48)
    f5 = make_file("sha5", "src/a.py", changes=6, pr_number=3)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        files=[f1, f2, f3, f4, f5],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = TemporalLogicalCoupling()
    res = metric.run(context)

    assert res.metric_slug == "temporal_logical_coupling"

    # file A revisions = 3, file B revisions = 2
    # shared_revisions = 2
    # avg_revisions = (3 + 2) / 2 = 2.5
    # coupling_ratio = 2 / 2.5 * 100 = 80
    top_pairs = res.details["top_coupled_pairs"]
    assert len(top_pairs) == 1
    assert top_pairs[0]["file_a"] == "src/a.py"
    assert top_pairs[0]["file_b"] == "src/b.py"
    assert top_pairs[0]["shared_revisions"] == 2
    assert top_pairs[0]["avg_revisions"] == 2.5
    assert top_pairs[0]["coupling_ratio"] == 80.0

    assert res.details["max_coupling_ratio"] == 80.0
    assert res.details["pair_count"] == 1
    assert "Top pair: src/a.py + src/b.py" in res.summary


def test_temporal_logical_coupling_excludes_generated_files():
    """Generated files should not contribute to coupling pairs."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=5, pr_number=1)
    f2 = make_file("sha2", "package-lock.json", changes=5000, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

    metric = TemporalLogicalCoupling()
    res = metric.run(context)

    assert res.details["pair_count"] == 0
    assert res.details["max_coupling_ratio"] == 0.0
    assert res.details["top_coupled_pairs"] == []


def test_temporal_logical_coupling_no_data():
    """No PRs should return no_data flag."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = TemporalLogicalCoupling()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["max_coupling_ratio"] == 0.0
    assert res.details["pair_count"] == 0
