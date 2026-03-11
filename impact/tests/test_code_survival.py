from datetime import timedelta

from impact.metrics.plugins.authored.code_survival import CodeSurvival
from impact.metrics.utils import compute_code_survival
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_code_survival_basic():
    """Code survival should track how many lines remain untouched."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # Cohort PR (Alice adds 10 lines to a.py)
    # Set merged_delta_hours to ensure it is considered merged
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=1)
    patch1 = "@@ -0,0 +1,10 @@\n" + "".join([f"+line {i}\n" for i in range(1, 11)])
    f1 = make_file("sha1", "src/a.py", additions=10, changes=10, pr_number=1, patch=patch1)

    # Subsequent PR (Someone removes 3 lines from a.py)
    other = make_user(id=3, login="bob")
    pr2 = make_pr(2, other, repo, base_time=start, created_delta_hours=24, merged_delta_hours=25)
    # Remove lines 2, 5, 8
    patch2 = "@@ -1,10 +1,7 @@\n line 1\n-line 2\n line 3\n line 4\n-line 5\n line 6\n line 7\n-line 8\n line 9\n line 10\n"
    f2 = make_file("sha2", "src/a.py", deletions=3, changes=3, pr_number=2, patch=patch2)

    bundle = make_bundle(
        users=[user, owner, other],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeSurvival()
    res = metric.run(context)

    assert res.metric_slug == "code_survival"
    assert res.details["total_contributed"] == 10
    assert res.details["total_survived"] == 7
    assert res.details["survival_rate"] == 70.0
    assert len(res.details["per_cohort"]) == 1
    assert res.details["per_cohort"][0]["survived_lines"] == 7


def test_code_survival_multiple_cohorts():
    """Multiple cohort PRs should be tracked independently."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # Alice PR1: 5 lines in a.py
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=1)
    patch1 = "@@ -0,0 +1,5 @@\n+line 1\n+line 2\n+line 3\n+line 4\n+line 5\n"
    f1 = make_file("sha1", "src/a.py", additions=5, changes=5, pr_number=1, patch=patch1)

    # Alice PR2: 5 lines in b.py
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=48, merged_delta_hours=49)
    patch2 = "@@ -0,0 +1,5 @@\n+line 1\n+line 2\n+line 3\n+line 4\n+line 5\n"
    f2 = make_file("sha2", "src/b.py", additions=5, changes=5, pr_number=2, patch=patch2)

    # Bob PR3: removes 2 lines from a.py (happens AFTER pr1, pr2)
    other = make_user(id=3, login="bob")
    pr3 = make_pr(3, other, repo, base_time=start, created_delta_hours=72, merged_delta_hours=73)
    patch3 = "@@ -1,5 +1,3 @@\n line 1\n-line 2\n line 3\n-line 4\n line 5\n"
    f3 = make_file("sha3", "src/a.py", deletions=2, changes=2, pr_number=3, patch=patch3)

    bundle = make_bundle(
        users=[user, owner, other],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        files=[f1, f2, f3],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = CodeSurvival()
    res = metric.run(context)

    # a.py (5 contributed, 3 survived)
    # b.py (5 contributed, 5 survived)
    # total: 10 contributed, 8 survived (80%)
    assert res.details["total_contributed"] == 10
    assert res.details["total_survived"] == 8
    assert res.details["survival_rate"] == 80.0


def test_code_survival_excludes_generated():
    """Generated files should be ignored."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=1)
    f1 = make_file("sha1", "package-lock.json", additions=1000, changes=1000, pr_number=1, patch="@@ -0,0 +1,1000 @@\n+...")

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    context = make_context(bundle, user_login="alice")

    metric = CodeSurvival()
    res = metric.run(context)

    assert res.details["total_contributed"] == 0
    assert res.details["no_data"] is True


def test_compute_code_survival_helper():
    """Direct test of the survival helper."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=1)
    patch1 = "@@ -0,0 +1,2 @@\n+line 1\n+line 2\n"
    f1 = make_file("sha1", "src/a.py", additions=2, changes=2, pr_number=1, patch=patch1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    result = compute_code_survival(context.ledger, "alice", [pr1], start_date=start, end_date=start + timedelta(days=30))

    assert result["survival_rate"] == 100.0
    assert result["total_contributed"] == 2
    assert result["total_survived"] == 2


def test_code_survival_short_period():
    """Short period with few PRs should flag no_data."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0, merged_delta_hours=1)
    f1 = make_file("sha1", "src/a.py", additions=5, changes=5, pr_number=1, patch="@@ -0,0 +1,5 @@\n+line 1\n+line 2\n+line 3\n+line 4\n+line 5\n")

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1],
    )
    # 3 days, 1 PR -> insufficient data
    end = start + timedelta(days=3)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CodeSurvival()
    res = metric.run(context)

    assert res.details["no_data"] is True
