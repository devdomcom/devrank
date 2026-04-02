"""Tests for Main Developer metrics (by revisions and by lines)."""

import pytest
from datetime import datetime, timezone

from impact.metrics.plugins.authored.main_developer import (
    MainDeveloperByRevisions,
    MainDeveloperByLines,
)

from impact.tests.conftest import (
    make_user,
    make_repo,
    make_pr,
    make_commit,
    make_file,
    make_bundle,
    make_context,
)


class TestMainDeveloperByRevisions:
    """Test the MainDeveloperByRevisions metric class."""

    @pytest.fixture
    def metric(self):
        return MainDeveloperByRevisions()

    def test_metric_properties(self, metric):
        assert metric.slug == "main_developer_by_revisions"
        assert metric.name == "Main Developer (by revisions)"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks

    def test_no_files_in_scope_returns_no_data(self, metric):
        """When the target user has no PRs, there are no files to analyze."""
        alice = make_user(id=1, login="alice")
        repo = make_repo()

        bundle = make_bundle()
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.metric_slug == "main_developer_by_revisions"
        assert result.details["total_files"] == 0
        assert result.details["files_with_main_developer"] == 0
        assert result.details.get("no_data") is True
        assert "No files found" in result.summary

    def test_identifies_main_developer_by_commit_count(self, metric):
        """
        Main developer is the author with the most commits touching a file.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice has a PR touching src/main.py
        pr_alice = make_pr(
            number=10,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Bob has 3 commits on this file; Alice has 1
        commit_alice = make_commit(
            sha="a1",
            author=alice,
            date=datetime(2024, 1, 11, tzinfo=timezone.utc),
            pr_number=10,
            message="feat: initial",
        )
        commit_bob_1 = make_commit(
            sha="b1",
            author=bob,
            date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            pr_number=10,
            message="fix: bug 1",
        )
        commit_bob_2 = make_commit(
            sha="b2",
            author=bob,
            date=datetime(2024, 1, 16, tzinfo=timezone.utc),
            pr_number=10,
            message="fix: bug 2",
        )
        commit_bob_3 = make_commit(
            sha="b3",
            author=bob,
            date=datetime(2024, 1, 17, tzinfo=timezone.utc),
            pr_number=10,
            message="fix: bug 3",
        )

        file_main = make_file(
            sha="sha_main",
            filename="src/main.py",
            pr_number=10,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice],
            commits=[commit_alice, commit_bob_1, commit_bob_2, commit_bob_3],
            files=[file_main],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 1
        assert result.details["files_with_main_developer"] == 1

        main_devs = result.details["main_developers"]
        assert len(main_devs) == 1
        assert main_devs[0]["file"] == "src/main.py"
        assert main_devs[0]["main_author"] == "bob"
        assert main_devs[0]["revision_count"] == 3
        assert main_devs[0]["total_revisions"] == 4
        assert main_devs[0]["ownership_pct"] == 75.0

    def test_tie_breaking_by_alphabetical_order(self, metric):
        """
        When two authors have equal commit counts, the first alphabetically wins.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        pr_alice = make_pr(
            number=20,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Both have 1 commit each
        commit_alice = make_commit(
            sha="a1",
            author=alice,
            date=datetime(2024, 1, 11, tzinfo=timezone.utc),
            pr_number=20,
        )
        commit_bob = make_commit(
            sha="b1",
            author=bob,
            date=datetime(2024, 1, 12, tzinfo=timezone.utc),
            pr_number=20,
        )

        file_x = make_file(sha="sha_x", filename="src/x.py", pr_number=20)

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice],
            commits=[commit_alice, commit_bob],
            files=[file_x],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        main_devs = result.details["main_developers"]
        assert len(main_devs) == 1
        # alice < bob alphabetically, so alice wins the tie
        assert main_devs[0]["main_author"] == "alice"
        assert main_devs[0]["revision_count"] == 1

    def test_multiple_files_with_different_main_developers(self, metric):
        """
        Each file independently reports its own main developer.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice has two PRs, each touching a different file
        pr_alice_1 = make_pr(
            number=30,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        pr_alice_2 = make_pr(
            number=31,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

        # Alice touches file_a via PR 30
        commit_alice = make_commit(
            sha="a1", author=alice, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=30
        )
        file_a = make_file(sha="sha_a", filename="src/a.py", pr_number=30)

        # Bob also touches file_b via PR 31 (but alice owns the PR)
        commit_bob = make_commit(
            sha="b1", author=bob, date=datetime(2024, 1, 16, tzinfo=timezone.utc), pr_number=31
        )
        file_b = make_file(sha="sha_b", filename="src/b.py", pr_number=31)

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice_1, pr_alice_2],
            commits=[commit_alice, commit_bob],
            files=[file_a, file_b],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 2
        assert result.details["files_with_main_developer"] == 2

        main_devs = {md["file"]: md for md in result.details["main_developers"]}
        assert main_devs["src/a.py"]["main_author"] == "alice"
        assert main_devs["src/b.py"]["main_author"] == "bob"


class TestMainDeveloperByLines:
    """Test the MainDeveloperByLines metric class."""

    @pytest.fixture
    def metric(self):
        return MainDeveloperByLines()

    def test_metric_properties(self, metric):
        assert metric.slug == "main_developer_by_lines"
        assert metric.name == "Main Developer (by lines)"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks

    def test_no_files_in_scope_returns_no_data(self, metric):
        """When the target user has no PRs, there are no files to analyze."""
        alice = make_user(id=1, login="alice")
        repo = make_repo()

        bundle = make_bundle()
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.metric_slug == "main_developer_by_lines"
        assert result.details["total_files"] == 0
        assert result.details["files_with_main_developer"] == 0
        assert result.details.get("no_data") is True
        assert "No files found" in result.summary

    def test_identifies_main_developer_by_line_contribution(self, metric):
        """
        Main developer is the author with the most line changes on a file.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        pr_alice = make_pr(
            number=40,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Alice's PR added 200 lines to main.py
        file_main = make_file(
            sha="sha_main",
            filename="src/main.py",
            additions=200,
            deletions=10,
            changes=210,
            pr_number=40,
        )

        # Bob's PR added 50 lines to main.py
        pr_bob = make_pr(
            number=41,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        file_main_bob = make_file(
            sha="sha_main2",
            filename="src/main.py",
            additions=50,
            deletions=0,
            changes=50,
            pr_number=41,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice, pr_bob],
            commits=[],
            files=[file_main, file_main_bob],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 1
        assert result.details["files_with_main_developer"] == 1

        main_devs = result.details["main_developers"]
        assert len(main_devs) == 1
        assert main_devs[0]["file"] == "src/main.py"
        # Alice contributed 210 changes, Bob contributed 50
        assert main_devs[0]["main_author"] == "alice"
        assert main_devs[0]["line_contribution"] == 210
        assert main_devs[0]["total_lines"] == 260
        assert main_devs[0]["ownership_pct"] == pytest.approx(80.77, rel=0.01)

    def test_multiple_files_with_different_main_developers_by_lines(self, metric):
        """
        Each file independently reports its main developer based on line changes.

        Note: In "by lines" mode, changes are attributed to the PR author (since
        FileRecords are at the PR level). This test shows that when different
        PR owners touch different files, each gets credited with their own lines.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice owns PR 50, Bob owns PR 51 — each touches a different file
        pr_alice = make_pr(
            number=50,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        pr_bob = make_pr(
            number=51,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

        # Alice dominates file_a with 100 changes via her PR
        file_a = make_file(
            sha="sha_a",
            filename="src/a.py",
            additions=100,
            deletions=0,
            changes=100,
            pr_number=50,
        )
        # Bob dominates file_b with 80 changes via his PR
        file_b = make_file(
            sha="sha_b",
            filename="src/b.py",
            additions=80,
            deletions=0,
            changes=80,
            pr_number=51,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice, pr_bob],
            commits=[],
            files=[file_a, file_b],
        )
        # Use bob as the target user so file_b (on his PR) is in scope
        context = make_context(
            bundle=bundle,
            user_login="bob",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 1  # Only file_b is in scope for bob
        assert result.details["files_with_main_developer"] == 1

        main_devs = result.details["main_developers"]
        assert len(main_devs) == 1
        assert main_devs[0]["file"] == "src/b.py"
        assert main_devs[0]["main_author"] == "bob"
