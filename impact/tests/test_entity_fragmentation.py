from datetime import timedelta

from impact.metrics.plugins.authored.entity_fragmentation import EntityFragmentation
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_entity_fragmentation_balanced_entities():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)

    files = [
        make_file("sha1", "frontend/app.py", changes=50, pr_number=1),
        make_file("sha2", "backend/api.py", changes=50, pr_number=1),
        make_file("sha3", "frontend/widget.py", changes=50, pr_number=2),
        make_file("sha4", "backend/db.py", changes=50, pr_number=2),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = EntityFragmentation()
    res = metric.run(context)

    # 2 entities, each with 100 changes => shares 0.5/0.5 => HHI 0.5 => fragmentation 0.5
    assert res.metric_slug == "entity_fragmentation"
    assert res.details["entity_count"] == 2
    assert res.details["total_changes"] == 200
    assert res.details["hhi"] == 0.5
    assert res.details["fragmentation_index"] == 0.5
    assert res.summary.startswith("Fragmentation index: 0.50")


def test_entity_fragmentation_concentrated():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)

    files = [
        make_file("sha1", "core/main.py", changes=90, pr_number=1),
        make_file("sha2", "docs/readme.md", changes=10, pr_number=1),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = EntityFragmentation()
    res = metric.run(context)

    # shares 0.9/0.1 => HHI 0.82 => fragmentation 0.18
    assert res.details["entity_count"] == 2
    assert res.details["total_changes"] == 100
    assert res.details["hhi"] == 0.82
    assert res.details["fragmentation_index"] == 0.18
    assert res.summary.startswith("Fragmentation index: 0.18")


def test_entity_fragmentation_excludes_generated_files():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    files = [
        make_file("sha1", "src/main.py", changes=10, pr_number=1),
        make_file("sha2", "package-lock.json", changes=5000, pr_number=1),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = EntityFragmentation()
    res = metric.run(context)

    assert res.details["entity_count"] == 1
    assert res.details["total_changes"] == 10
    assert res.details["fragmentation_index"] == 0.0


def test_entity_fragmentation_no_data():
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[], files=[])
    context = make_context(bundle, user_login="alice")

    metric = EntityFragmentation()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["total_changes"] == 0
    assert res.details["fragmentation_index"] == 0.0
