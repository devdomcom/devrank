"""Tests for the Change Proximity metric."""

from datetime import timedelta
from impact.metrics.plugins.authored.change_proximity import ChangeProximity
from impact.metrics.utils import compute_change_proximity
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_change_proximity_concentrated():
    """Test with concentrated changes (low proximity = good)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with concentrated changes: lines 10, 11, 12 (proximity = 1+1 = 2)
    # Hunk header @@ -X,Y +start,count @@ means new lines start at 'start'
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch_concentrated = (
        "@@ -5,3 +10,3 @@\n"
        " context\n"
        "+new line 10\n"
        "+new line 11\n"
        "+new line 12\n"
        " context\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch_concentrated)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    assert res.metric_slug == "change_proximity"
    # Lines 10, 11, 12 -> proximity = (11-10) + (12-11) = 2
    # avg_proximity_per_change = 2 / 3 = 0.67
    assert res.details["total_proximity"] == 2
    assert res.details["total_changes"] == 3
    assert res.details["avg_proximity_per_change"] < 1.0  # Low = concentrated
    assert res.details.get("no_data") is not True


def test_change_proximity_scattered():
    """Test with scattered changes (high proximity = risky)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with scattered changes: lines 10, 50, 100 (proximity = 40+50 = 90)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch_scattered = (
        "@@ -5,1 +10,1 @@\n"
        "+line 10\n"
        "@@ -45,1 +50,1 @@\n"
        "+line 50\n"
        "@@ -95,1 +100,1 @@\n"
        "+line 100\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch_scattered)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    assert res.metric_slug == "change_proximity"
    # Lines 10, 50, 100 -> proximity = (50-10) + (100-50) = 90
    # avg_proximity_per_change = 90 / 3 = 30
    assert res.details["total_proximity"] == 90
    assert res.details["total_changes"] == 3
    assert res.details["avg_proximity_per_change"] == 30.0
    assert res.details.get("no_data") is not True


def test_change_proximity_single_line():
    """Test with single line change (proximity = 0, concentrated)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch_single = (
        "@@ -5,1 +10,1 @@\n"
        "+single line\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=1, changes=1, pr_number=1, patch=patch_single)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Single line has proximity 0 (perfectly concentrated)
    assert res.details["total_proximity"] == 0
    assert res.details["total_changes"] == 1
    assert res.details["avg_proximity_per_change"] == 0.0


def test_change_proximity_multiple_files():
    """Test with multiple files having different proximity patterns."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)

    # File A: concentrated (lines 10, 11, 12 -> proximity = 2)
    patch_a = (
        "@@ -5,3 +10,3 @@\n"
        "+line 10\n"
        "+line 11\n"
        "+line 12\n"
    )
    f1 = make_file("sha1", "src/a.py", additions=3, changes=3, pr_number=1, patch=patch_a)

    # File B: scattered (lines 10, 50 -> proximity = 40)
    patch_b = (
        "@@ -5,1 +10,1 @@\n"
        "+line 10\n"
        "@@ -45,1 +50,1 @@\n"
        "+line 50\n"
    )
    f2 = make_file("sha2", "src/b.py", additions=2, changes=2, pr_number=1, patch=patch_b)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Total proximity = 2 + 40 = 42
    # Total changes = 3 + 2 = 5
    # avg_proximity_per_change = 42 / 5 = 8.4
    assert res.details["total_proximity"] == 42
    assert res.details["total_changes"] == 5
    assert res.details["total_files"] == 2
    assert res.details["avg_proximity_per_change"] == 8.4

    # Per-file details should be sorted by proximity (scattered first)
    per_file = res.details["per_file"]
    assert len(per_file) == 2
    assert per_file[0]["filename"] == "src/b.py"  # Higher proximity first
    assert per_file[0]["proximity"] == 40
    assert per_file[1]["filename"] == "src/a.py"
    assert per_file[1]["proximity"] == 2


def test_change_proximity_excludes_generated():
    """Generated files should be excluded from proximity calculation."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)

    # Regular file with concentrated changes
    patch_regular = (
        "@@ -5,3 +10,3 @@\n"
        "+line 10\n"
        "+line 11\n"
        "+line 12\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch_regular)

    # Lockfile (generated) with scattered changes - should be excluded
    patch_lock = (
        "@@ -5,1 +10,1 @@\n"
        "+line 10\n"
        "@@ -100,1 +110,1 @@\n"
        "+line 110\n"
    )
    f2 = make_file("sha2", "package-lock.json", additions=2, changes=2, pr_number=1, patch=patch_lock)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Only the regular file should be counted
    assert res.details["total_files"] == 1
    assert res.details["total_changes"] == 3
    assert res.details["total_proximity"] == 2  # Only from src/main.py


def test_change_proximity_multiple_prs():
    """Test with multiple PRs to verify per-PR tracking."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: concentrated
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch1 = "@@ -5,3 +10,3 @@\n+line 10\n+line 11\n+line 12\n"
    f1 = make_file("sha1", "src/a.py", additions=3, changes=3, pr_number=1, patch=patch1)

    # PR2: scattered
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
    patch2 = "@@ -5,1 +10,1 @@\n+line 10\n@@ -55,1 +60,1 @@\n+line 60\n"
    f2 = make_file("sha2", "src/b.py", additions=2, changes=2, pr_number=2, patch=patch2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    assert res.details["pr_count"] == 2
    per_pr = res.details["per_pr"]
    assert len(per_pr) == 2

    # Check per-PR details
    pr1_detail = next(p for p in per_pr if p["number"] == 1)
    assert pr1_detail["proximity"] == 2
    assert pr1_detail["change_count"] == 3

    pr2_detail = next(p for p in per_pr if p["number"] == 2)
    assert pr2_detail["proximity"] == 50  # 60 - 10 = 50
    assert pr2_detail["change_count"] == 2


def test_change_proximity_no_data():
    """Edge case: no PRs -> no data."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")

    metric = ChangeProximity()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["total_proximity"] == 0
    assert res.details["total_changes"] == 0


def test_change_proximity_no_patch():
    """Files without patch data should be handled gracefully."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    # File without patch
    f1 = make_file("sha1", "src/main.py", additions=5, deletions=2, changes=7, pr_number=1, patch=None)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Without patch, we can't compute line-level proximity
    assert res.details["total_changes"] == 0
    assert res.details["no_data"] is True


def test_change_proximity_additions_and_deletions():
    """Only additions are counted for proximity (new file positions)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    # Patch with both additions and deletions
    # New lines (added): lines 11, 12
    # Proximity = (12-11) = 1
    patch_mixed = (
        "@@ -8,3 +10,3 @@\n"
        " context\n"
        "-old line 10\n"
        "+new line 11\n"
        "+new line 12\n"
        " context\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=2, deletions=1, changes=3, pr_number=1, patch=patch_mixed)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Only additions count for proximity (new file positions)
    assert res.details["total_changes"] == 2  # Only 2 additions
    # Lines 11, 12 -> proximity = 1
    assert res.details["total_proximity"] == 1


def test_compute_change_proximity_function():
    """Direct test of the compute_change_proximity helper function."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch = "@@ -5,3 +10,3 @@\n+line 10\n+line 11\n+line 12\n"
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))
    ledger = ctx.ledger

    result = compute_change_proximity(ledger, [pr1], start_date=start, end_date=start + timedelta(days=30))

    assert result["total_proximity"] == 2
    assert result["total_changes"] == 3
    assert result["avg_proximity_per_change"] < 1.0
    assert result["pr_count"] == 1
    assert result["total_files"] == 1


def test_change_proximity_hunk_count():
    """Verify hunk_count is tracked in per-file details."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    # Patch with 2 hunks
    patch_two_hunks = (
        "@@ -5,1 +10,1 @@\n"
        "+line 10\n"
        "@@ -55,1 +60,1 @@\n"
        "+line 60\n"
    )
    f1 = make_file("sha1", "src/main.py", additions=2, changes=2, pr_number=1, patch=patch_two_hunks)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Should have hunk_count in per_file details
    assert len(res.details["per_file"]) == 1
    assert res.details["per_file"][0]["hunk_count"] == 2


def test_change_proximity_line_range():
    """Verify line_range is tracked in per-file details."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch = "@@ -5,1 +10,1 @@\n+line 10\n@@ -55,1 +60,1 @@\n+line 60\n@@ -105,1 +110,1 @@\n+line 110\n"
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Line range should be (min, max) of all changed lines
    assert len(res.details["per_file"]) == 1
    assert res.details["per_file"][0]["line_range"] == (10, 110)


def test_change_proximity_short_period():
    """Short period with few PRs should set no_data flag."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch = "@@ -5,3 +10,3 @@\n+line 10\n+line 11\n+line 12\n"
    f1 = make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=patch)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    # Short period (3 days) with only 1 PR
    end = start + timedelta(days=3)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ChangeProximity()
    res = metric.run(context)

    # Should have no_data flag due to short period with few PRs
    assert res.details["no_data"] is True
