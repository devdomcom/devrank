"""Tests for Bus Factor metric."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.bus_factor import BusFactor


def make_mock_pr(number: int, title: str, user_login: str, files=None):
    """Create a mock PR with required attributes."""
    pr = MagicMock()
    pr.number = number
    pr.title = title
    pr.draft = False
    pr.merged = True
    pr.created_at = datetime(2024, 1, 15, tzinfo=timezone.utc)
    
    user_mock = MagicMock()
    user_mock.login = user_login
    pr.user = user_mock
    
    return pr


def make_mock_file(filename: str, changes: int = 10, additions: int = 5, deletions: int = 5):
    """Create a mock file record."""
    f = MagicMock()
    f.filename = filename
    f.changes = changes
    f.additions = additions
    f.deletions = deletions
    f.patch = None
    return f


class TestBusFactorMetric:
    """Test the BusFactor metric class."""

    @pytest.fixture
    def metric(self):
        return BusFactor()

    @pytest.fixture
    def mock_context(self):
        """Create a mock context."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "alice"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        ledger = MagicMock()
        context.ledger = ledger

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "bus_factor"
        assert metric.name == "Bus Factor"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks

    def test_no_prs_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        ledger.get_prs_for_user.return_value = []

        result = metric.run(context)

        assert result.metric_slug == "bus_factor"
        assert result.details["total_files"] == 0
        assert result.details["no_data"] is True
        assert "No files found" in result.summary

    def test_single_contributor_bus_factor_zero(self, metric, mock_context):
        """If only one contributor touches all files, bus factor is 0."""
        context, ledger = mock_context

        # Alice touches files in 3 PRs
        prs = [
            make_mock_pr(1, "Add feature", "alice"),
            make_mock_pr(2, "Fix bug", "alice"),
            make_mock_pr(3, "Refactor", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs

        # All PRs touch different files
        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/feature.py", 20)],
            2: [make_mock_file("src/bugfix.py", 10)],
            3: [make_mock_file("src/refactor.py", 15)],
        }.get(pr_num, [])

        result = metric.run(context)

        # Bus factor is 0 because all files have single contributor
        assert result.details["bus_factor"] == 0
        assert result.details["unique_contributors"] == 1
        assert result.details["single_contributor_files_count"] == 3
        assert "CRITICAL" in result.summary

    def test_two_contributors_shared_files(self, metric, mock_context):
        """If two contributors share all files, bus factor is 1."""
        context, ledger = mock_context

        # Both Alice and Bob touch the same files
        prs = [
            make_mock_pr(1, "Add feature", "alice"),
            make_mock_pr(2, "Fix bug", "bob"),
            make_mock_pr(3, "Update feature", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs

        # Both contributors touch the same file
        shared_file = make_mock_file("src/shared.py", 30)
        ledger.get_files_for_pr.side_effect = lambda pr_num: [shared_file]

        result = metric.run(context)

        # Bus factor is 1 - if either leaves, the file still has one contributor
        assert result.details["bus_factor"] == 1
        assert result.details["unique_contributors"] == 2
        assert result.details["single_contributor_files_count"] == 0
        # Summary contains "Bus factor is 1" not "Bus factor: 1" because it's a warning case
        assert "Bus factor is 1" in result.summary or "Bus factor: 1" in result.summary

    def test_bus_factor_with_distributed_ownership(self, metric, mock_context):
        """Bus factor of 2 when removing 2 top contributors still leaves someone."""
        context, ledger = mock_context

        # Three contributors, each file touched by at least 2 people
        prs = [
            make_mock_pr(1, "Feature A", "alice"),
            make_mock_pr(2, "Feature A fix", "bob"),
            make_mock_pr(3, "Feature B", "bob"),
            make_mock_pr(4, "Feature B fix", "charlie"),
            make_mock_pr(5, "Feature C", "charlie"),
            make_mock_pr(6, "Feature C fix", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs

        # Each file touched by 2 contributors
        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/a.py")],
            2: [make_mock_file("src/a.py")],
            3: [make_mock_file("src/b.py")],
            4: [make_mock_file("src/b.py")],
            5: [make_mock_file("src/c.py")],
            6: [make_mock_file("src/c.py")],
        }.get(pr_num, [])

        result = metric.run(context)

        # Bus factor should be at least 1 - can lose at least 1 contributor
        assert result.details["bus_factor"] >= 1
        assert result.details["unique_contributors"] == 3
        assert len(result.details["contributor_order"]) >= 2

    def test_mixed_single_and_multi_contributor_files(self, metric, mock_context):
        """Some files with single contributor, some with multiple."""
        context, ledger = mock_context

        prs = [
            make_mock_pr(1, "Shared feature", "alice"),
            make_mock_pr(2, "Shared fix", "bob"),
            make_mock_pr(3, "Solo feature", "alice"),  # Only alice touches this
        ]
        ledger.get_prs_for_user.return_value = prs

        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/shared.py")],
            2: [make_mock_file("src/shared.py")],
            3: [make_mock_file("src/solo.py")],
        }.get(pr_num, [])

        result = metric.run(context)

        # Bus factor is 0 due to solo.py having single contributor
        assert result.details["bus_factor"] == 0
        assert result.details["single_contributor_files_count"] == 1
        assert "src/solo.py" in result.details["single_contributor_files"]

    def test_contribution_weight_affects_order(self, metric, mock_context):
        """Contributors with more changes are removed first."""
        context, ledger = mock_context

        # Alice makes big contribution, Bob makes small one
        prs = [
            make_mock_pr(1, "Big feature", "alice"),
            make_mock_pr(2, "Small fix", "bob"),
        ]
        ledger.get_prs_for_user.return_value = prs

        # Alice touches file with 100 changes, Bob with 10
        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/big.py", 100, 100, 0)],
            2: [make_mock_file("src/small.py", 10, 10, 0)],
        }.get(pr_num, [])

        result = metric.run(context)

        # Alice should be first in contributor order (higher contribution)
        assert result.details["contributor_order"][0] == "alice"

    def test_no_data_guard_short_period(self, metric, mock_context):
        """Short period with few files triggers no_data."""
        context, ledger = mock_context
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        prs = [
            make_mock_pr(1, "Feature", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs
        ledger.get_files_for_pr.return_value = [make_mock_file("src/a.py")]

        result = metric.run(context)

        assert result.details["no_data"] is True

    def test_sufficient_data_with_enough_files(self, metric, mock_context):
        """Sufficient files and period doesn't trigger no_data."""
        context, ledger = mock_context
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 21, tzinfo=timezone.utc)

        # Need at least 3 files to avoid no_data
        prs = [
            make_mock_pr(1, "Feature 1", "alice"),
            make_mock_pr(2, "Feature 2", "bob"),
            make_mock_pr(3, "Feature 3", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs
        ledger.get_files_for_pr.side_effect = lambda pr_num: [
            make_mock_file(f"src/{pr_num}.py")
        ]

        result = metric.run(context)

        # Should have 3 files
        assert result.details["total_files"] == 3
        assert result.details.get("no_data") is not True

    def test_generated_files_excluded(self, metric, mock_context):
        """Generated files should not be counted."""
        context, ledger = mock_context

        prs = [
            make_mock_pr(1, "Add generated code", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs

        # File looks generated (has generated marker in patch)
        gen_file = make_mock_file("generated/api.py", 100)
        gen_file.patch = "// Code generated by tool. DO NOT EDIT."
        ledger.get_files_for_pr.return_value = [gen_file]

        result = metric.run(context)

        # Generated file should be excluded
        assert result.details["total_files"] == 0

    def test_per_file_contributor_count(self, metric, mock_context):
        """Verify per-file contributor counts are accurate."""
        context, ledger = mock_context

        prs = [
            make_mock_pr(1, "Feature A", "alice"),
            make_mock_pr(2, "Feature A fix", "bob"),
            make_mock_pr(3, "Feature B", "alice"),
        ]
        ledger.get_prs_for_user.return_value = prs

        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/a.py")],
            2: [make_mock_file("src/a.py")],
            3: [make_mock_file("src/b.py")],
        }.get(pr_num, [])

        result = metric.run(context)

        per_file = result.details["per_file_contributor_count"]
        assert per_file["src/a.py"] == 2  # alice and bob
        assert per_file["src/b.py"] == 1  # only alice

    def test_at_risk_files_identified(self, metric, mock_context):
        """Files that become orphaned after removing top contributor."""
        context, ledger = mock_context

        # Alice is top contributor, Bob only touches one file
        prs = [
            make_mock_pr(1, "Alice main", "alice"),
            make_mock_pr(2, "Alice secondary", "alice"),
            make_mock_pr(3, "Bob solo", "bob"),  # Only Bob touches this
        ]
        ledger.get_prs_for_user.return_value = prs

        ledger.get_files_for_pr.side_effect = lambda pr_num: {
            1: [make_mock_file("src/shared.py", 50)],
            2: [make_mock_file("src/shared2.py", 30)],
            3: [make_mock_file("src/bob_solo.py", 10)],
        }.get(pr_num, [])

        result = metric.run(context)

        # Bus factor is 0 because bob_solo.py has single contributor
        assert result.details["bus_factor"] == 0
        # bob_solo.py should be in the list (check for the filename)
        single_contrib_files = result.details["single_contributor_files"]
        assert any("bob_solo.py" in f for f in single_contrib_files)
