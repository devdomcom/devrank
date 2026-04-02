"""Tests for the Entity Ownership metric."""

import pytest
from datetime import datetime, timezone

from impact.metrics.plugins.authored.entity_ownership import EntityOwnership

from impact.tests.conftest import (
    make_user,
    make_repo,
    make_pr,
    make_commit,
    make_file,
    make_bundle,
    make_context,
)


class TestEntityOwnership:
    """Test the EntityOwnership metric class."""

    @pytest.fixture
    def metric(self):
        return EntityOwnership()

    def test_metric_properties(self, metric):
        assert metric.slug == "entity_ownership"
        assert metric.name == "Entity Ownership"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks
        assert metric.description.startswith("Per-author contribution percentages per file")

    def test_init_validates_by_mode(self):
        """Only 'revisions' and 'lines' are valid weighting modes."""
        m_rev = EntityOwnership(by="revisions")
        assert m_rev._by == "revisions"

        m_lines = EntityOwnership(by="lines")
        assert m_lines._by == "lines"

        with pytest.raises(ValueError):
            EntityOwnership(by="invalid")

    def test_no_files_in_scope_returns_no_data(self, metric):
        """When the target user has no PRs, there are no files to analyze."""
        alice = make_user(id=1, login="alice")
        bundle = make_bundle()
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.metric_slug == "entity_ownership"
        assert result.details["total_files"] == 0
        assert result.details["files"] == []
        assert result.details.get("no_data") is True
        assert result.details["avg_top_owner_pct"] == 0.0
        assert "No files found" in result.summary

    def test_reports_full_ownership_breakdown_per_file(self, metric):
        """
        For each file, reports all contributors with their percentages.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        pr_alice = make_pr(
            number=10,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Bob has 3 commits, Alice has 1 — Bob owns 75% of the file
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
        assert result.details["by"] == "revisions"
        assert result.details["avg_top_owner_pct"] == 75.0

        files = result.details["files"]
        assert len(files) == 1

        fo = files[0]
        assert fo["file"] == "src/main.py"
        assert fo["total_contributions"] == 4.0
        assert fo["author_count"] == 2

        # Authors are sorted by contribution descending
        ownership = fo["ownership"]
        assert len(ownership) == 2
        assert ownership[0]["author"] == "bob"
        assert ownership[0]["contribution"] == 3.0
        assert ownership[0]["ownership_pct"] == 75.0
        assert ownership[1]["author"] == "alice"
        assert ownership[1]["contribution"] == 1.0
        assert ownership[1]["ownership_pct"] == 25.0

    def test_multiple_files_with_different_ownership(self, metric):
        """
        Each file independently reports its own ownership distribution.
        Only files the target user touched are analyzed.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice has one PR touching two files
        pr_alice = make_pr(
            number=20,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # File a: Alice only
        commit_alice_a = make_commit(
            sha="a1", author=alice, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=20
        )
        file_a = make_file(sha="sha_a", filename="src/a.py", pr_number=20)

        # File b: Bob only (but Alice's PR also touched it, so it's in scope)
        commit_bob_b = make_commit(
            sha="b1", author=bob, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=20
        )
        file_b = make_file(sha="sha_b", filename="src/b.py", pr_number=20)

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice],
            commits=[commit_alice_a, commit_bob_b],
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
        # Both commits are in the same PR, so both authors are credited for both files
        assert result.details["avg_top_owner_pct"] == 50.0

        files_by_name = {f["file"]: f for f in result.details["files"]}
        # Each file has 50/50 ownership between alice and bob
        for fname in ("src/a.py", "src/b.py"):
            fo = files_by_name[fname]
            assert fo["author_count"] == 2
            ownership = fo["ownership"]
            assert ownership[0]["ownership_pct"] == 50.0
            assert ownership[1]["ownership_pct"] == 50.0

    def test_lines_weighting_mode(self):
        """
        When by='lines', contribution is weighted by additions + deletions,
        attributed to the PR author (not commit authors).
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Two separate PRs so each author gets credited for their own changes
        pr_alice = make_pr(
            number=30,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        pr_bob = make_pr(
            number=31,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

        # Alice's PR touches src/x.py with 100 lines
        commit_alice = make_commit(
            sha="a1", author=alice, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=30
        )
        file_x_alice = make_file(
            sha="sha_x1",
            filename="src/x.py",
            pr_number=30,
            additions=100,
            deletions=0,
            changes=100,
        )

        # Bob's PR touches src/x.py with 300 lines
        commit_bob = make_commit(
            sha="b1", author=bob, date=datetime(2024, 1, 16, tzinfo=timezone.utc), pr_number=31
        )
        file_x_bob = make_file(
            sha="sha_x2",
            filename="src/x.py",
            pr_number=31,
            additions=300,
            deletions=0,
            changes=300,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice, pr_bob],
            commits=[commit_alice, commit_bob],
            files=[file_x_alice, file_x_bob],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        metric = EntityOwnership(by="lines")
        result = metric.run(context)

        assert result.details["by"] == "lines"
        files = result.details["files"]
        assert len(files) == 1

        fo = files[0]
        # Total lines = 100 + 300 = 400
        assert fo["total_contributions"] == 400.0
        ownership = fo["ownership"]
        # Bob contributed 300/400 = 75%, Alice 100/400 = 25%
        assert ownership[0]["author"] == "bob"
        assert ownership[0]["ownership_pct"] == 75.0
        assert ownership[1]["author"] == "alice"
        assert ownership[1]["ownership_pct"] == 25.0

    def test_alphabetical_tie_breaking(self, metric):
        """
        When two authors have equal contributions, sort alphabetically.
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

        commit_alice = make_commit(
            sha="a1", author=alice, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=40
        )
        commit_bob = make_commit(
            sha="b1", author=bob, date=datetime(2024, 1, 12, tzinfo=timezone.utc), pr_number=40
        )

        file_y = make_file(sha="sha_y", filename="src/y.py", pr_number=40)

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice],
            commits=[commit_alice, commit_bob],
            files=[file_y],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        ownership = result.details["files"][0]["ownership"]
        # Equal contributions (1 each), so alice < bob alphabetically comes first
        assert ownership[0]["author"] == "alice"
        assert ownership[1]["author"] == "bob"
