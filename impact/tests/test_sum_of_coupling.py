from datetime import timedelta

from impact.metrics.plugins.authored.sum_of_coupling import SumOfCoupling
from impact.metrics.utils import compute_sum_of_coupling
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_sum_of_coupling_basic():
    """SoC sums coupling counts per entity across PRs."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: A + B (each gets +1 coupling)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "src/b.py", changes=8, pr_number=1)

    # PR2: A + C (each gets +1 coupling)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
    f3 = make_file("sha3", "src/a.py", changes=5, pr_number=2)
    f4 = make_file("sha4", "src/c.py", changes=7, pr_number=2)

    # PR3: A only (no coupling)
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

    metric = SumOfCoupling()
    res = metric.run(context)

    assert res.metric_slug == "sum_of_coupling"

    # SoC per entity:
    # A: PR1 (1) + PR2 (1) = 2
    # B: PR1 (1) = 1
    # C: PR2 (1) = 1
    per_entity = res.details["per_entity"]
    assert len(per_entity) == 3
    assert per_entity[0]["entity"] == "src/a.py"
    assert per_entity[0]["coupling_score"] == 2

    max_score = res.details["max_coupling_score"]
    assert max_score == 2
    assert res.details["total_entities"] == 3
    assert res.details["total_coupling"] == 4  # A(2) + B(1) + C(1)


def test_sum_of_coupling_multi_entity_pr():
    """PR with multiple entities increments SoC by n-1 per entity."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: A + B + C (each gets +2 coupling)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "src/b.py", changes=8, pr_number=1)
    f3 = make_file("sha3", "src/c.py", changes=5, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2, f3],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = SumOfCoupling()
    res = metric.run(context)

    per_entity = res.details["per_entity"]
    assert len(per_entity) == 3
    # Each entity gets +2 (paired with 2 others)
    assert per_entity[0]["coupling_score"] == 2
    assert res.details["total_coupling"] == 6  # 3 entities * 2 coupling each


def test_sum_of_coupling_excludes_generated():
    """Generated files should not contribute to coupling counts."""
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

    metric = SumOfCoupling()
    res = metric.run(context)

    assert res.details["total_entities"] == 0
    assert res.details["total_coupling"] == 0
    assert res.details["per_entity"] == []


def test_sum_of_coupling_no_data():
    """No PRs should return no_data flag."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = SumOfCoupling()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["total_entities"] == 0
    assert res.details["total_coupling"] == 0


def test_compute_sum_of_coupling_helper():
    """Direct test of the helper function."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "src/b.py", changes=8, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    result = compute_sum_of_coupling(context.ledger, [pr1], start_date=start, end_date=start + timedelta(days=30))

    assert result["total_entities"] == 2
    assert result["total_coupling"] == 2
    assert result["max_coupling_score"] == 1
    assert result["per_entity"][0]["coupling_score"] == 1


def test_sum_of_coupling_short_period():
    """Short period with a single PR should flag no_data."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/a.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "src/b.py", changes=8, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    # Short period (3 days) with only 1 PR
    end = start + timedelta(days=3)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = SumOfCoupling()
    res = metric.run(context)

    assert res.details["no_data"] is True
