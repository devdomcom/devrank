"""Tests for TimeToRestore (DORA MTTR proxy) metric."""

from datetime import timedelta

from impact.metrics.plugins.authored.time_to_restore import TimeToRestore
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_time_to_restore_basic():
    """Revert followed by a fix commit on overlapping files."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=1))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=5),
                  merged_at=start + timedelta(hours=6))

    # Revert commit on PR1
    revert = make_commit(
        "rev1", user, start + timedelta(hours=2), 1,
        message='Revert "Add feature X"',
    )
    # Fix commit on PR2, touches same file 4h later
    fix = make_commit(
        "fix1", user, start + timedelta(hours=6), 2,
        message="fix: restore feature X properly",
    )

    # Files: revert touches src/x.py, fix also touches src/x.py
    f1 = make_file("sha1", "src/x.py", pr_number=1)
    f2 = make_file("sha2", "src/x.py", pr_number=2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[revert, fix],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    assert result.metric_slug == "time_to_restore"
    assert result.details["revert_count"] == 1
    assert result.details["incidents_with_fix"] == 1
    # 4 hours from revert to fix
    assert result.details["median_restore_hours"] == 4.0
    assert len(result.details["incidents"]) == 1
    incident = result.details["incidents"][0]
    assert incident["revert_sha"] == "rev1"[:8]
    assert incident["fix_sha"] == "fix1"[:8]


def test_time_to_restore_no_overlapping_files():
    """Revert and subsequent commit on different files → no incident matched."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=1))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=5),
                  merged_at=start + timedelta(hours=6))

    revert = make_commit(
        "rev1", user, start + timedelta(hours=2), 1,
        message='Revert "Add feature"',
    )
    other = make_commit(
        "other1", user, start + timedelta(hours=6), 2,
        message="feat: unrelated change",
    )

    # Different files
    f1 = make_file("sha1", "src/x.py", pr_number=1)
    f2 = make_file("sha2", "src/y.py", pr_number=2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[revert, other],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    assert result.details["revert_count"] == 1
    assert result.details["incidents_with_fix"] == 0
    assert result.details["no_data"] is True


def test_time_to_restore_skips_subsequent_reverts():
    """The fix commit should not be another revert."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=1))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=5),
                  merged_at=start + timedelta(hours=6))

    revert1 = make_commit(
        "rev1", user, start + timedelta(hours=2), 1,
        message='Revert "Feature A"',
    )
    revert2 = make_commit(
        "rev2", user, start + timedelta(hours=4), 1,
        message='Revert "Feature B"',
    )
    fix = make_commit(
        "fix1", user, start + timedelta(hours=8), 2,
        message="fix: actual fix",
    )

    f1 = make_file("sha1", "src/x.py", pr_number=1)
    f2 = make_file("sha2", "src/x.py", pr_number=2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[revert1, revert2, fix],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    # First revert should find fix (not revert2), second revert should also find fix
    assert result.details["incidents_with_fix"] >= 1
    for incident in result.details["incidents"]:
        assert not incident["fix_sha"].startswith("rev")


def test_time_to_restore_no_reverts():
    """No revert commits → no_data."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=1))
    c1 = make_commit("sha1", user, start + timedelta(hours=0.5), 1, message="feat: normal commit")

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[c1],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    assert result.details["no_data"] is True
    assert result.details["revert_count"] == 0


def test_time_to_restore_revert_without_pr():
    """Revert commit without a pull_request_number should be skipped."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Revert commit with no PR number
    revert = make_commit(
        "rev1", user, start + timedelta(hours=2), None,
        message='Revert "Something"',
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[],
        commits=[revert],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    assert result.details["revert_count"] == 1
    assert result.details["incidents_with_fix"] == 0
    assert result.details["no_data"] is True


def test_time_to_restore_multiple_incidents():
    """Multiple revert-fix cycles should all be measured."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=1))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=3),
                  merged_at=start + timedelta(hours=4))
    pr3 = make_pr(3, user, repo, created_at=start + timedelta(days=1),
                  merged_at=start + timedelta(days=1, hours=1))
    pr4 = make_pr(4, user, repo, created_at=start + timedelta(days=1, hours=3),
                  merged_at=start + timedelta(days=1, hours=4))

    revert1 = make_commit("rev1", user, start + timedelta(hours=2), 1, message='Revert "A"')
    fix1 = make_commit("fix1", user, start + timedelta(hours=4), 2, message="fix A")
    revert2 = make_commit("rev2", user, start + timedelta(days=1, hours=2), 3, message='Revert "B"')
    fix2 = make_commit("fix2", user, start + timedelta(days=1, hours=4), 4, message="fix B")

    f1 = make_file("s1", "src/a.py", pr_number=1)
    f2 = make_file("s2", "src/a.py", pr_number=2)
    f3 = make_file("s3", "src/b.py", pr_number=3)
    f4 = make_file("s4", "src/b.py", pr_number=4)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4],
        commits=[revert1, fix1, revert2, fix2],
        files=[f1, f2, f3, f4],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TimeToRestore().run(ctx)

    assert result.details["revert_count"] == 2
    assert result.details["incidents_with_fix"] == 2
    assert result.details["median_restore_hours"] == 2.0  # both 2h
