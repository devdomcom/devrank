from datetime import timedelta

from impact.metrics.plugins.authored.commit_message_mining import CommitMessageMining
from impact.metrics.utils import classify_defect_commit
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_repo,
    make_user,
)


def test_commit_message_mining_basic():
    """Defect-related commit messages should be detected and counted."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    commits = [
        make_commit("sha1", user, start, 1, message="fix: handle null pointer"),
        make_commit("sha2", user, start + timedelta(hours=1), 1, message="bugfix: resolve crash on startup"),
        make_commit("sha3", user, start + timedelta(hours=2), 1, message="feat: add new dashboard"),
        make_commit("sha4", user, start + timedelta(hours=3), 1, message="refactor: clean up API"),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        commits=commits,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CommitMessageMining()
    res = metric.run(context)

    assert res.metric_slug == "commit_message_mining"
    assert res.details["defect_commit_count"] == 2
    assert res.details["total_commits"] == 4
    assert res.details["defect_commit_rate"] == 50.0


def test_commit_message_mining_varied_terms():
    """Regex should match a variety of defect terms."""
    messages = [
        "hotfix: rollback release",
        "Resolve outage in auth service",
        "Mitigate incident with error handler",
        "Patch regression in billing",
        "Fix flaky test in pipeline",
        "Handle exception in worker",
    ]

    for msg in messages:
        assert classify_defect_commit(msg) is True


def test_commit_message_mining_filters_merge_commits():
    """Merge commits should be excluded from sampling."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    commits = [
        make_commit("sha1", user, start, 1, message="merge branch 'main'", parent_count=2),
        make_commit("sha2", user, start + timedelta(hours=1), 1, message="fix: crash on start"),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        commits=commits,
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = CommitMessageMining()
    res = metric.run(context)

    assert res.details["total_commits"] == 1
    assert res.details["defect_commit_count"] == 1


def test_commit_message_mining_no_data():
    """No commits should return no_data flag."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], commits=[])
    context = make_context(bundle, user_login="alice")

    metric = CommitMessageMining()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["total_commits"] == 0


def test_classify_defect_commit_negative_terms():
    """Non-defect messages should not be classified as defects."""
    messages = [
        "feat: add auth endpoint",
        "docs: update README",
        "refactor: reorganize modules",
        "chore: bump deps",
    ]

    for msg in messages:
        assert classify_defect_commit(msg) is False
