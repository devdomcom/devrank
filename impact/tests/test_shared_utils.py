"""
Tests for shared utility functions promoted to impact/metrics/utils.py.

These functions were originally private helpers in individual metric files but are
now shared across multiple metrics, so they belong in utils.py.
"""

from datetime import timedelta
from collections import defaultdict

from impact.domain.models import (
    CanonicalBundle,
    Commit,
    FileRecord,
    MetricContext,
    PullRequest,
    PullRequestState,
    Repository,
    User,
)
from impact.ledger.ledger import Ledger
from impact.metrics.utils import (
    build_file_contributors,
    complexity_from_patch,
)
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


# ---------------------------------------------------------------------------
# Tests for complexity_from_patch (from complexity_trend.py)
# ---------------------------------------------------------------------------

class TestComplexityFromPatch:
    """Test the complexity_from_patch utility."""

    def test_none_patch_returns_none(self):
        assert complexity_from_patch(None) is None

    def test_empty_patch_returns_none(self):
        assert complexity_from_patch("") is None

    def test_patch_with_no_added_lines_returns_none(self):
        # Only context lines, no + lines
        patch = "@@ -1,3 +1,3 @@\n line1\n line2\n line3\n"
        assert complexity_from_patch(patch) is None

    def test_single_added_line_zero_indent(self):
        patch = "@@ -1,1 +1,1 @@\n+code\n"
        # 0 spaces / 4 = 0.0
        assert complexity_from_patch(patch) == 0.0

    def test_single_added_line_with_indent(self):
        patch = "@@ -1,1 +1,1 @@\n+    indented_code\n"
        # 4 spaces / 4 = 1.0
        assert complexity_from_patch(patch) == 1.0

    def test_multiple_added_lines_average(self):
        patch = (
            "@@ -1,3 +1,3 @@\n"
            "+    level1\n"
            "+        level2\n"
            "+            level3\n"
        )
        # (1 + 2 + 3) / 3 = 2.0
        assert complexity_from_patch(patch) == 2.0

    def test_ignores_non_added_lines(self):
        patch = "@@ -1,2 +1,2 @@\n-removed\n+added\n context\n"
        # Only "+added" counts, indent = 0
        assert complexity_from_patch(patch) == 0.0

    def test_ignores_hunk_headers(self):
        # +++ is a file header, not an added line
        patch = "+++ b/file.py\n@@ -1,1 +1,1 @@\n+code\n"
        assert complexity_from_patch(patch) == 0.0

    def test_tabs_converted_to_spaces(self):
        patch = "@@ -1,1 +1,1 @@\n+\t\tcode\n"  # 2 tabs = 8 spaces
        # 8 / 4 = 2.0
        assert complexity_from_patch(patch) == 2.0


# ---------------------------------------------------------------------------
# Tests for build_file_contributors (from main_developer.py)
# ---------------------------------------------------------------------------

class TestBuildFileContributors:
    """Test the build_file_contributors utility."""

    def test_empty_inputs(self):
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo", owner=user)
        bundle = make_bundle(users=[user], repositories=[repo], pull_requests=[], files=[])
        ledger = Ledger(bundle)
        files_in_scope: set[str] = set()

        result = build_file_contributors(files_in_scope, bundle, ledger, by="lines")
        assert result == {}

    def test_lines_mode_single_file_single_author(self):
        user = make_user(id=1, login="alice")
        owner = make_user(id=99, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, user, repo, base_time=start, created_delta_hours=0, additions=10)
        f = make_file("sha1", "src/main.py", additions=10, changes=10, pr_number=1)

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr],
            files=[f],
        )
        ledger = Ledger(bundle)
        files_in_scope = {"src/main.py"}

        result = build_file_contributors(files_in_scope, bundle, ledger, by="lines")
        assert "src/main.py" in result
        assert result["src/main.py"]["alice"] == 10.0  # changes=10

    def test_lines_mode_multiple_files(self):
        user = make_user(id=1, login="alice")
        owner = make_user(id=99, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        f1 = make_file("sha1", "src/a.py", additions=5, changes=5, pr_number=1)
        f2 = make_file("sha2", "src/b.py", additions=3, changes=3, pr_number=1)

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr],
            files=[f1, f2],
        )
        ledger = Ledger(bundle)
        files_in_scope = {"src/a.py", "src/b.py"}

        result = build_file_contributors(files_in_scope, bundle, ledger, by="lines")
        assert result["src/a.py"]["alice"] == 5.0
        assert result["src/b.py"]["alice"] == 3.0

    def test_excludes_generated_files(self):
        user = make_user(id=1, login="alice")
        owner = make_user(id=99, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        real = make_file("sha1", "src/main.py", additions=5, changes=5, pr_number=1)
        gen = make_file("sha2", "package-lock.json", additions=1000, changes=1000, pr_number=1)

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr],
            files=[real, gen],
        )
        ledger = Ledger(bundle)
        files_in_scope = {"src/main.py", "package-lock.json"}

        result = build_file_contributors(files_in_scope, bundle, ledger, by="lines")
        # Generated file should be excluded entirely
        assert "package-lock.json" not in result
        assert "src/main.py" in result

    def test_revisions_mode_counts_commits(self):
        user = make_user(id=1, login="alice")
        owner = make_user(id=99, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        f = make_file("sha1", "src/main.py", additions=1, changes=1, pr_number=1)

        # Two commits on the same PR
        c1 = make_commit("sha_c1", user, start, pr_number=1, message="first")
        c2 = make_commit("sha_c2", user, start + timedelta(hours=1), pr_number=1, message="second")

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2],
            files=[f],
        )
        ledger = Ledger(bundle)
        files_in_scope = {"src/main.py"}

        result = build_file_contributors(files_in_scope, bundle, ledger, by="revisions")
        # Two commits => score 2.0
        assert result["src/main.py"]["alice"] == 2.0
