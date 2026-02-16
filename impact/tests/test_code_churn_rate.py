from datetime import timedelta

from impact.metrics.plugins.authored.code_churn_rate import CodeChurnRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_code_churn_rate():
    """Test churn rate with file history (reuse DRY util; 30d window)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: introduces fileA (no prior)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/fileA.py", changes=100, pr_number=1)
    # PR2: modifies same fileA within 20d -> churn; also new fileB
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=168)  # +1wk
    f2 = make_file("sha2", "src/fileA.py", changes=50, pr_number=2)  # churn
    f3 = make_file("sha3", "src/fileB.py", changes=30, pr_number=2)  # no prior
    # PR3: modifies fileA but >30d later (out of window, no churn)
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=336)  # +2wk
    f4 = make_file("sha4", "src/fileA.py", changes=20, pr_number=3)
    # PR4: no files
    pr4 = make_pr(4, user, repo, base_time=start, created_delta_hours=504)  # +3wk

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4],
        files=[f1, f2, f3, f4],
    )
    end = start + timedelta(days=30)  # covers up to pr3; pr4 for completeness
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeChurnRate()
    res = metric.run(context)

    assert res.metric_slug == "code_churn_rate"
    # PR2:50/80 from PR1; PR3:20 from PR2 (<=30d); total 70/200=35%; max_weekly=100 (spike)
    assert res.details["churn_rate"] == 35.0
    assert res.details["churned_lines"] == 70
    assert res.details["total_lines"] == 200
    assert res.details["pr_count"] == 4
    assert res.details["window_days"] == 30
    assert res.details["max_weekly_churn"] == 100.0
    assert res.details["churn_per_week"] > 0
    # per_pr details
    assert len(res.details["per_pr"]) == 4
    assert any(p["number"] == 2 and p["churn_rate"] == 62.5 for p in res.details["per_pr"])
    assert "Code churn:" in res.summary


def test_code_churn_rate_no_prs():
    """Edge: no PRs -> 0 rate (incl weekly)."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    metric = CodeChurnRate()
    res = metric.run(context)
    assert res.details["churn_rate"] == 0.0
    assert res.details["pr_count"] == 0
    assert res.details["max_weekly_churn"] == 0.0
    assert res.details["churn_per_week"] == 0.0


def test_code_churn_rate_no_churn():
    """All unique files or out-of-window -> 0 churn."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "unique.py", changes=100, pr_number=1)
    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))
    metric = CodeChurnRate()
    res = metric.run(context)
    assert res.details["churn_rate"] == 0.0
    assert res.details["max_weekly_churn"] == 0.0
    assert res.details["churn_per_week"] == 0.0


def test_code_churn_rate_line_level_with_patches():
    """Line-level churn: only overlapping lines count, not entire file."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: adds lines 1-5 in fileA
    pr1_patch = "@@ -0,0 +1,5 @@\n+line1\n+line2\n+line3\n+line4\n+line5\n"
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/fileA.py", additions=5, deletions=0, changes=5,
                    pr_number=1, patch=pr1_patch)

    # PR2: adds lines 3-7 in same file (overlap on lines 3,4,5)
    pr2_patch = "@@ -0,0 +3,5 @@\n+line3\n+line4\n+line5\n+line6\n+line7\n"
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=168)  # +1wk
    f2 = make_file("sha2", "src/fileA.py", additions=5, deletions=0, changes=5,
                    pr_number=2, patch=pr2_patch)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeChurnRate()
    res = metric.run(context)

    # PR1: no prior → churn=0, total=5
    # PR2: lines 3,4,5 overlap with PR1's lines 1,2,3,4,5 → overlap = lines {3,4,5} = 3
    # Total: churn=3, total=10
    assert res.details["churned_lines"] == 3
    assert res.details["total_lines"] == 10
    assert res.details["churn_rate"] == 30.0  # 3/10*100


def test_code_churn_rate_excludes_generated_files():
    """Generated files (lockfiles etc.) should not inflate churn."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: real file + lockfile
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/main.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "package-lock.json", changes=5000, pr_number=1)

    # PR2: same files modified
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=168)
    f3 = make_file("sha3", "src/main.py", changes=10, pr_number=2)
    f4 = make_file("sha4", "package-lock.json", changes=5000, pr_number=2)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2, f3, f4],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeChurnRate()
    res = metric.run(context)

    # Only real files counted: total_lines should be 20 (10+10), not 10020
    assert res.details["total_lines"] == 20
    # Churn from PR2's main.py (all lines file-level since no patches): 10
    assert res.details["churned_lines"] == 10


def test_code_churn_rate_uses_context_period():
    """Period should be from context dates, not PR dates."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # All PRs in the same day
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "a.py", changes=10, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    # Context: 90-day window
    end = start + timedelta(days=90)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeChurnRate()
    res = metric.run(context)

    # Should use 90 days, not 1 day (from PR dates)
    assert res.details["period_days"] == 90.0
