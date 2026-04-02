"""Tests for the Contributor Experience metric."""

import pytest
from datetime import datetime, timezone

from impact.metrics.plugins.authored.contributor_experience import ContributorExperience

from impact.tests.conftest import (
    make_user,
    make_repo,
    make_pr,
    make_commit,
    make_file,
    make_bundle,
    make_context,
)


class TestContributorExperience:
    """Test the ContributorExperience metric class."""

    @pytest.fixture
    def metric(self):
        return ContributorExperience()

    def test_metric_properties(self, metric):
        assert metric.slug == "contributor_experience"
        assert metric.name == "Contributor Experience"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks
        assert "relative share" in metric.description.lower()

    def test_no_files_in_scope_returns_no_data(self, metric):
        """When there are no files, experience_pct is 0 with no_data flag."""
        alice = make_user(id=1, login="alice")
        bundle = make_bundle()
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.metric_slug == "contributor_experience"
        assert result.details["experience_pct"] == 0.0
        assert result.details["user_lines"] == 0.0
        assert result.details["total_lines"] == 0.0
        assert result.details.get("no_data") is True

    def test_zero_activity_for_target_user_sets_no_data(self, metric):
        """When the target user has no contributions, experience_pct is 0."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Bob has a PR, but Alice (target) has none
        pr_bob = make_pr(
            number=10,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        commit_bob = make_commit(
            sha="b1",
            author=bob,
            date=datetime(2024, 1, 11, tzinfo=timezone.utc),
            pr_number=10,
        )
        file_bob = make_file(
            sha="sha_b",
            filename="src/b.py",
            pr_number=10,
            additions=100,
            deletions=0,
            changes=100,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_bob],
            commits=[commit_bob],
            files=[file_bob],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["experience_pct"] == 0.0
        assert result.details["user_lines"] == 0.0
        assert result.details["total_lines"] == 100.0
        assert result.details.get("no_data") is True

    def test_experience_pct_computed_correctly(self, metric):
        """
        experience_pct = user_lines / total_lines * 100 across all bundle files.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice has a PR with 100 lines
        pr_alice = make_pr(
            number=10,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        commit_alice = make_commit(
            sha="a1",
            author=alice,
            date=datetime(2024, 1, 11, tzinfo=timezone.utc),
            pr_number=10,
        )
        file_alice = make_file(
            sha="sha_a",
            filename="src/a.py",
            pr_number=10,
            additions=100,
            deletions=0,
            changes=100,
        )

        # Bob has a PR with 300 lines
        pr_bob = make_pr(
            number=20,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        commit_bob = make_commit(
            sha="b1",
            author=bob,
            date=datetime(2024, 1, 16, tzinfo=timezone.utc),
            pr_number=20,
        )
        file_bob = make_file(
            sha="sha_b",
            filename="src/b.py",
            pr_number=20,
            additions=300,
            deletions=0,
            changes=300,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice, pr_bob],
            commits=[commit_alice, commit_bob],
            files=[file_alice, file_bob],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        # Alice: 100 lines, Bob: 300 lines, total: 400
        # Alice's experience: 100/400 = 25%
        assert result.details["experience_pct"] == 25.0
        assert result.details["user_lines"] == 100.0
        assert result.details["total_lines"] == 400.0
        assert result.details.get("no_data") is None  # not set when there's data

    def test_period_fallback_when_dates_missing(self, metric):
        """When start_date/end_date are None, make_context fills defaults."""
        alice = make_user(id=1, login="alice")
        bundle = make_bundle()
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=None,
            end_date=None,
        )

        result = metric.run(context)

        # make_context provides default dates spanning 10 days
        assert result.details["period_days"] == 10.0

    def test_summary_includes_experience_percentage(self, metric):
        """Summary string includes the computed experience percentage."""
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
        commit_alice = make_commit(
            sha="a1", author=alice, date=datetime(2024, 1, 11, tzinfo=timezone.utc), pr_number=10
        )
        file_alice = make_file(
            sha="sha_a", filename="src/a.py", pr_number=10, additions=50, changes=50
        )

        pr_bob = make_pr(
            number=20,
            user=bob,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        commit_bob = make_commit(
            sha="b1", author=bob, date=datetime(2024, 1, 16, tzinfo=timezone.utc), pr_number=20
        )
        file_bob = make_file(
            sha="sha_b", filename="src/b.py", pr_number=20, additions=50, changes=50
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice, pr_bob],
            commits=[commit_alice, commit_bob],
            files=[file_alice, file_bob],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert "50.0%" in result.summary
        assert "50" in result.summary  # alice's 50 lines
        assert "100" in result.summary  # total 100 lines
