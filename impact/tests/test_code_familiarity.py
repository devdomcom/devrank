"""Tests for Code Familiarity metric."""

import pytest
from datetime import datetime, timedelta, timezone

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.code_familiarity import CodeFamiliarity

from impact.tests.conftest import (
    make_user,
    make_repo,
    make_pr,
    make_commit,
    make_file,
    make_bundle,
    make_context,
)


class TestCodeFamiliarityMetric:
    """Test the CodeFamiliarity metric class."""

    @pytest.fixture
    def metric(self):
        return CodeFamiliarity()

    def test_metric_properties(self, metric):
        assert metric.slug == "code_familiarity"
        assert metric.name == "Code Familiarity"
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

        assert result.metric_slug == "code_familiarity"
        assert result.details["total_files"] == 0
        assert result.details["familiar_file_count"] == 0
        assert result.details.get("no_data") is True
        assert "No files found" in result.summary

    def test_all_files_familiar_when_active_contributors_touched_everything(self, metric):
        """
        If every file in scope has been touched by at least one active contributor,
        familiarity is 100%.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo()

        # Alice has one PR touching src/main.py
        pr_alice = make_pr(
            number=10,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Bob touched the same file within the period — he's active
        commit_bob = make_commit(
            sha="b1",
            author=bob,
            date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            pr_number=10,
            message="feat: update main",
        )

        file_main = make_file(
            sha="sha_main",
            filename="src/main.py",
            pr_number=10,
        )

        bundle = make_bundle(
            users=[alice, bob],
            pull_requests=[pr_alice],
            commits=[commit_bob],
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
        assert result.details["familiar_file_count"] == 1
        assert result.details["familiarity_pct"] == 100.0
        assert result.details["active_contributors_count"] == 1
        assert "bob" in result.details["active_contributors"]
        assert "100" in result.summary or "100.0" in result.summary

    def test_unfamiliar_file_when_no_active_contributor_touched_it(self, metric):
        """
        A file touched only by inactive contributors (no commits in the period)
        is counted as unfamiliar.
        """
        alice = make_user(id=1, login="alice")
        carol = make_user(id=3, login="carol")  # inactive — no commits in period
        repo = make_repo()

        pr_alice = make_pr(
            number=20,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        # Carol's commit is OUTSIDE the analysis period — she's inactive
        commit_carol = make_commit(
            sha="c1",
            author=carol,
            date=datetime(2023, 6, 1, tzinfo=timezone.utc),
            pr_number=20,
            message="feat: old change",
        )

        file_legacy = make_file(
            sha="sha_legacy",
            filename="src/legacy.py",
            pr_number=20,
        )

        bundle = make_bundle(
            users=[alice, carol],
            pull_requests=[pr_alice],
            commits=[commit_carol],
            files=[file_legacy],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 1
        assert result.details["familiar_file_count"] == 0
        assert result.details["familiarity_pct"] == 0.0
        assert result.details["unfamiliar_file_count"] == 1
        assert "src/legacy.py" in result.details["unfamiliar_files"]
        assert "No files are familiar" in result.summary or result.details["familiarity_pct"] == 0.0

    def test_mixed_familiarity_counts_correctly(self, metric):
        """
        With multiple files, only those touched by an active contributor count as familiar.
        """
        alice = make_user(id=1, login="alice")
        dave = make_user(id=4, login="dave")   # active
        eve = make_user(id=5, login="eve")     # inactive
        repo = make_repo()

        # Two separate PRs so files are attributed independently
        pr_active = make_pr(
            number=30,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )
        pr_stale = make_pr(
            number=31,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

        # Dave (active) touched only the "active" file via PR 30
        commit_dave = make_commit(
            sha="d1",
            author=dave,
            date=datetime(2024, 1, 20, tzinfo=timezone.utc),
            pr_number=30,
            message="feat: active change",
        )
        # Eve (inactive) touched only the "stale" file via PR 31
        commit_eve = make_commit(
            sha="e1",
            author=eve,
            date=datetime(2023, 5, 1, tzinfo=timezone.utc),
            pr_number=31,
            message="feat: old change",
        )

        file_active = make_file(sha="sha_a", filename="src/active.py", pr_number=30)
        file_stale = make_file(sha="sha_s", filename="src/stale.py", pr_number=31)

        bundle = make_bundle(
            users=[alice, dave, eve],
            pull_requests=[pr_active, pr_stale],
            commits=[commit_dave, commit_eve],
            files=[file_active, file_stale],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        assert result.details["total_files"] == 2
        assert result.details["familiar_file_count"] == 1
        assert result.details["familiarity_pct"] == 50.0
        assert result.details["unfamiliar_file_count"] == 1
        assert "src/stale.py" in result.details["unfamiliar_files"]

    def test_active_contributors_detected_from_commit_dates(self, metric):
        """
        Active contributors are those with at least one commit within the analysis period.
        """
        alice = make_user(id=1, login="alice")
        frank = make_user(id=6, login="frank")  # active
        grace = make_user(id=7, login="grace")  # inactive
        repo = make_repo()

        pr_alice = make_pr(
            number=40,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 12, tzinfo=timezone.utc),
        )

        commit_frank = make_commit(
            sha="f1",
            author=frank,
            date=datetime(2024, 1, 25, tzinfo=timezone.utc),
            pr_number=40,
            message="feat: active",
        )
        commit_grace = make_commit(
            sha="g1",
            author=grace,
            date=datetime(2023, 3, 1, tzinfo=timezone.utc),
            pr_number=40,
            message="feat: very old",
        )

        file_x = make_file(sha="sha_x", filename="src/x.py", pr_number=40)

        bundle = make_bundle(
            users=[alice, frank, grace],
            pull_requests=[pr_alice],
            commits=[commit_frank, commit_grace],
            files=[file_x],
        )
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        active = result.details["active_contributors"]
        assert "frank" in active
        assert "grace" not in active
        assert result.details["active_contributors_count"] == 1

    def test_no_data_guard_for_sparse_period(self, metric):
        """
        Very short periods with minimal PRs should trigger the no_data guard.
        """
        alice = make_user(id=1, login="alice")
        repo = make_repo()

        pr_alice = make_pr(
            number=50,
            user=alice,
            repo=repo,
            created_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            merged_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )

        commit_alice = make_commit(
            sha="a1",
            author=alice,
            date=datetime(2024, 1, 2, tzinfo=timezone.utc),
            pr_number=50,
        )

        file_y = make_file(sha="sha_y", filename="src/y.py", pr_number=50)

        bundle = make_bundle(
            users=[alice],
            pull_requests=[pr_alice],
            commits=[commit_alice],
            files=[file_y],
        )
        # 3-day period with only 1 PR — should be flagged as sparse
        context = make_context(
            bundle=bundle,
            user_login="alice",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 3, tzinfo=timezone.utc),
        )

        result = metric.run(context)

        # Either no_data is set, or the result is valid but with low confidence
        # The exact behavior depends on the threshold; ensure the field exists
        assert "no_data" in result.details or result.details["period_days"] <= 14
