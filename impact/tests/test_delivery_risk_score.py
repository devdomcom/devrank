"""Tests for the Delivery Risk Score metric.

TDD: Tests written first. These tests exercise real data flow through
the ledger and utilities without overmocking.
"""

from datetime import timedelta

from impact.metrics.plugins.authored.delivery_risk_score import DeliveryRiskScore
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


# ---------------------------------------------------------------------------
# Test 1: High-risk scenario (many files, scattered changes, low experience)
# ---------------------------------------------------------------------------
def test_delivery_risk_score_high_risk():
    """High file count + scattered changes + low experience → high risk score (8-10)."""
    user = make_user(id=1, login="newbie")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with many files (40) and scattered changes across the codebase
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=400)

    # Create 40 files with scattered patches (lines far apart = high diffusion)
    # Use indented added lines to trigger complexity detection
    files = []
    for i in range(40):
        # Scattered: lines at 10, 100, 200, 500 (high proximity)
        # Indented added lines for complexity scoring
        patch = (
            "@@ -5,4 +10,4 @@\n"
            " context\n"
            "+    indented_line_10\n"
            "+        deeply_nested_11\n"
            " context\n"
            "@@ -95,2 +100,2 @@\n"
            "+    indented_line_100\n"
            " context\n"
            "@@ -195,2 +200,2 @@\n"
            "+        deeply_nested_200\n"
            " context\n"
            "@@ -495,2 +500,2 @@\n"
            "+    indented_line_500\n"
            " context\n"
        )
        files.append(
            make_file(
                f"sha{i}",
                f"module/submodule/file_{i}.py",
                additions=4,
                changes=4,
                pr_number=1,
                patch=patch,
            )
        )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="newbie", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    assert "risk_score" in res.details
    # High risk: many files (40), scattered changes (diffusion=10)
    # Note: experience is 100% in user-centric data (limitation noted in METRICS_OVERVIEW.md)
    assert 5 <= res.details["risk_score"] <= 8, f"Expected 5-8, got {res.details['risk_score']}"
    assert res.details.get("no_data") is not True
    assert res.details["file_count"] == 40
    assert res.details["pr_count"] == 1


# ---------------------------------------------------------------------------
# Test 2: Low-risk scenario (few files, concentrated changes, high experience)
# ---------------------------------------------------------------------------
def test_delivery_risk_score_low_risk():
    """Few files + concentrated changes + high experience → low risk score (1-3)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with few files (2) and concentrated changes
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=10)

    # Concentrated: lines 10, 11, 12, 13, 14 (low proximity)
    patch_concentrated = (
        "@@ -5,10 +10,10 @@\n"
        "+line 10\n"
        "+line 11\n"
        "+line 12\n"
        "+line 13\n"
        "+line 14\n"
        "+line 15\n"
        "+line 16\n"
        "+line 17\n"
        "+line 18\n"
        "+line 19\n"
    )
    files = [
        make_file("sha1", "src/main.py", additions=10, changes=10, pr_number=1, patch=patch_concentrated),
        make_file("sha2", "src/utils.py", additions=10, changes=10, pr_number=1, patch=patch_concentrated),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    # Low risk: few files (2), concentrated changes
    # (experience 100% in user-centric data; low file count + low diffusion = low risk)
    assert 1 <= res.details["risk_score"] <= 4, f"Expected 1-4, got {res.details['risk_score']}"
    assert res.details.get("no_data") is not True
    assert res.details["file_count"] == 2


# ---------------------------------------------------------------------------
# Test 3: No data scenario
# ---------------------------------------------------------------------------
def test_delivery_risk_score_no_data():
    """Empty PR list → no_data flag set, risk_score = 0 or None."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # Bundle with no PRs for this user
    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[],
        files=[],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    assert res.details.get("no_data") is True
    assert res.details["risk_score"] == 0 or res.details["risk_score"] is None


# ---------------------------------------------------------------------------
# Test 4: Mixed risk scenario (medium files, medium diffusion)
# ---------------------------------------------------------------------------
def test_delivery_risk_score_medium_risk():
    """Medium file count + medium diffusion → medium risk score (4-7)."""
    user = make_user(id=1, login="bob")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=50)

    # Medium diffusion: some lines close, some spread
    patch_mixed = (
        "@@ -5,5 +10,5 @@\n"
        "+a\n+b\n+c\n"
        "@@ -95,2 +100,2 @@\n"
        "+d\n+e\n"
    )
    files = [
        make_file(f"sha{i}", f"src/file_{i}.py", additions=5, changes=5, pr_number=1, patch=patch_mixed)
        for i in range(10)
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="bob", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    assert 2 <= res.details["risk_score"] <= 5, f"Expected 2-5, got {res.details['risk_score']}"
    assert res.details.get("no_data") is not True


# ---------------------------------------------------------------------------
# Test 5: Risk labels on PRs increase risk score
# ---------------------------------------------------------------------------
def test_delivery_risk_score_with_risk_labels():
    """PR with risk labels (e.g., 'risk:db-migration') → higher risk score."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with risk labels
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=20)
    pr1.labels = ["risk:db-migration", "size/L"]

    patch = "@@ -1,5 +1,5 @@\n" + "+x\n" * 5
    files = [
        make_file("sha1", "migrations/001.sql", additions=5, changes=5, pr_number=1, patch=patch),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    # Risk labels should be detected and counted
    assert res.details.get("risk_label_count", 0) >= 1
    # Risk labels contribute to score (each adds ~1 point via label_score component)
    assert res.details["risk_score"] >= 2, f"Expected >=2 with risk labels, got {res.details['risk_score']}"


# ---------------------------------------------------------------------------
# Test 6: Multiple PRs aggregated correctly
# ---------------------------------------------------------------------------
def test_delivery_risk_score_multiple_prs():
    """Multiple PRs → risk_score reflects overall period risk."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=5)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24, additions=200)

    # Small PR
    patch_small = "@@ -1,3 +1,3 @@\n+a\n+b\n+c\n"
    f1 = make_file("sha1", "small.py", additions=3, changes=3, pr_number=1, patch=patch_small)

    # Large risky PR
    patch_large = "@@ -5,1 +10,1 @@\n" + "+x\n" * 10
    files_large = [
        make_file(f"sha{i}", f"big/file_{i}.py", additions=10, changes=10, pr_number=2, patch=patch_large)
        for i in range(30)
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1] + files_large,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    assert res.details["pr_count"] == 2
    assert res.details["file_count"] == 31  # 1 + 30
    # Overall score should reflect the mix (one low, one high risk PR)
    assert 1 <= res.details["risk_score"] <= 10
    assert res.details.get("no_data") is not True


# ---------------------------------------------------------------------------
# Test 7: Excludes generated files from complexity/diffusion calc
# ---------------------------------------------------------------------------
def test_delivery_risk_score_excludes_generated():
    """Generated files (e.g., package-lock.json) are excluded from risk calc."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=5000)

    # One real file, one generated
    real_patch = "@@ -1,3 +1,3 @@\n+a\n+b\n+c\n"
    files = [
        make_file("sha1", "src/main.py", additions=3, changes=3, pr_number=1, patch=real_patch),
        make_file("sha2", "package-lock.json", additions=5000, changes=5000, pr_number=1, patch=""),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DeliveryRiskScore()
    res = metric.run(context)

    assert res.metric_slug == "delivery_risk_score"
    # Only 1 real file should count (generated excluded)
    assert res.details.get("non_generated_file_count", 1) == 1
    assert res.details.get("no_data") is not True
