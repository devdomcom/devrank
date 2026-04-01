"""Tests for Knowledge Loss metric."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.knowledge_loss import KnowledgeLoss


class TestKnowledgeLossMetric:
    """Test the KnowledgeLoss metric class."""

    @pytest.fixture
    def metric(self):
        return KnowledgeLoss()

    def _make_mock_pr(self, number: int, author_login: str, created_at=None):
        """Create a mock PR."""
        pr = MagicMock()
        pr.number = number
        pr.draft = False
        pr.merged = True
        user = MagicMock()
        user.login = author_login
        pr.user = user
        pr.created_at = created_at or datetime(2024, 1, 15, tzinfo=timezone.utc)
        return pr

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with bundle data for commit-based ownership."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "alice"
        context.start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        ledger = MagicMock()
        context.ledger = ledger

        # Bundle with files and commits (simulated)
        bundle = MagicMock()
        bundle.files = []
        bundle.commits = []
        bundle.pull_requests = []
        ledger.bundle = bundle

        # Default: no user PRs (individual tests override)
        ledger.get_prs_for_user.return_value = []
        ledger.get_files_for_pr.return_value = []

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "knowledge_loss"
        assert metric.name == "Knowledge Loss"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks
        assert metric.LOSS_THRESHOLD == 0.50

    def _make_commit(self, sha: str, author_login: str, pr_num: int, date=None):
        """Create a mock commit with author and PR linkage."""
        c = MagicMock()
        c.sha = sha
        c.author = MagicMock(login=author_login)
        c.pull_request_number = pr_num
        c.date = date or datetime(2024, 1, 20, tzinfo=timezone.utc)
        return c

    def _make_file_record(self, filename: str, pr_num: int):
        """Create a mock FileRecord with filename and PR linkage."""
        f = MagicMock()
        f.filename = filename
        f.pull_request_number = pr_num
        f.patch = None
        return f

    def test_no_prs_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        # Empty bundle
        result = metric.run(context)
        assert result.metric_slug == "knowledge_loss"
        assert result.details["total_files"] == 0
        assert result.details["loss_count"] == 0
        assert result.details.get("no_data") is True

    def test_no_loss_when_all_contributors_active(self, metric, mock_context):
        """Files with all contributors active in period have no knowledge loss."""
        context, ledger = mock_context

        # Both alice and bob have commits within the analysis period
        file1_pr1 = self._make_file_record("src/main.py", 1)
        file1_pr2 = self._make_file_record("src/main.py", 2)

        # Both commits are within the analysis period (Jan 15-31)
        commit_alice = self._make_commit("a1", "alice", 1, datetime(2024, 1, 20, tzinfo=timezone.utc))
        commit_bob = self._make_commit("b1", "bob", 2, datetime(2024, 1, 22, tzinfo=timezone.utc))

        pr1 = self._make_mock_pr(1, "alice")
        pr2 = self._make_mock_pr(2, "bob")
        ledger.get_prs_for_user.return_value = [pr1]
        ledger.get_files_for_pr.side_effect = lambda n: {
            1: [file1_pr1],
            2: [file1_pr2],
        }.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2]
        bundle.commits = [commit_alice, commit_bob]
        bundle.pull_requests = [pr1, pr2]

        result = metric.run(context)

        assert result.details["loss_count"] == 0
        assert result.details["total_files"] == 1
        assert result.details["active_contributors_count"] == 2
        assert "No knowledge loss detected" in result.summary

    def test_detects_knowledge_loss_from_inactive_contributor(self, metric, mock_context):
        """File with 50%+ commits from inactive contributors shows knowledge loss."""
        context, ledger = mock_context

        # bob's commit is BEFORE the analysis period (inactive)
        # alice's commit is within the analysis period (active)
        file1_pr1 = self._make_file_record("src/legacy.py", 1)
        file1_pr2 = self._make_file_record("src/legacy.py", 2)

        commit_bob_old = self._make_commit("b1", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc))  # Before period
        commit_alice_new = self._make_commit("a1", "alice", 2, datetime(2024, 1, 20, tzinfo=timezone.utc))  # In period

        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "alice")
        ledger.get_prs_for_user.return_value = [pr2]
        ledger.get_files_for_pr.side_effect = lambda n: {
            1: [file1_pr1],
            2: [file1_pr2],
        }.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2]
        bundle.commits = [commit_bob_old, commit_alice_new]
        bundle.pull_requests = [pr1, pr2]

        result = metric.run(context)

        assert result.details["loss_count"] == 1
        assert result.details["total_files"] == 1
        at_risk = result.details["at_risk_files"]
        assert len(at_risk) == 1
        assert at_risk[0]["file"] == "src/legacy.py"
        assert at_risk[0]["inactive_pct"] == 50.0
        assert "bob" in at_risk[0]["inactive_contributors"]
        assert "Found 1 file(s) with knowledge loss" in result.summary

    def test_detects_multiple_knowledge_loss_files(self, metric, mock_context):
        """Multiple files with inactive contributors are all flagged."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/legacy1.py", 1)
        file2 = self._make_file_record("src/legacy2.py", 2)
        file3 = self._make_file_record("src/active.py", 3)

        # bob and charlie are inactive (commits before period)
        # alice is active (commit in period)
        commit_bob_old = self._make_commit("b1", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc))
        commit_charlie_old = self._make_commit("c1", "charlie", 2, datetime(2024, 1, 8, tzinfo=timezone.utc))
        commit_alice_new = self._make_commit("a1", "alice", 3, datetime(2024, 1, 20, tzinfo=timezone.utc))

        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "charlie")
        pr3 = self._make_mock_pr(3, "alice")
        # alice touches all three files in her PR
        ledger.get_prs_for_user.return_value = [pr3]
        ledger.get_files_for_pr.side_effect = lambda n: {
            1: [file1],
            2: [file2],
            3: [file1, file2, file3],  # alice touches all files
        }.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1, file2, file3]
        bundle.commits = [commit_bob_old, commit_charlie_old, commit_alice_new]
        bundle.pull_requests = [pr1, pr2, pr3]

        result = metric.run(context)

        assert result.details["loss_count"] == 2
        assert result.details["total_files"] == 3
        at_risk_files = {f["file"] for f in result.details["at_risk_files"]}
        assert "src/legacy1.py" in at_risk_files
        assert "src/legacy2.py" in at_risk_files
        assert "src/active.py" not in at_risk_files

    def test_loss_threshold_at_50_percent(self, metric, mock_context):
        """Ownership at exactly 50% triggers knowledge loss; below does not."""
        context, ledger = mock_context

        file1_pr1 = self._make_file_record("src/edge.py", 1)
        file1_pr2 = self._make_file_record("src/edge.py", 2)

        # 10 commits from bob (inactive) + 10 commits from alice (active) = 50% each
        bob_commits = [self._make_commit(f"b{i}", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc)) for i in range(10)]
        alice_commits = [self._make_commit(f"a{i}", "alice", 2, datetime(2024, 1, 20, tzinfo=timezone.utc)) for i in range(10)]

        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "alice")
        ledger.get_prs_for_user.return_value = [pr2]
        ledger.get_files_for_pr.side_effect = lambda n: {1: [file1_pr1], 2: [file1_pr2]}.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2]
        bundle.commits = bob_commits + alice_commits
        bundle.pull_requests = [pr1, pr2]

        result = metric.run(context)

        assert result.details["loss_count"] == 1
        assert result.details["at_risk_files"][0]["inactive_pct"] == 50.0

    def test_below_threshold_not_flagged(self, metric, mock_context):
        """49% from inactive contributors does NOT trigger knowledge loss."""
        context, ledger = mock_context

        file1_pr1 = self._make_file_record("src/safe.py", 1)
        file1_pr2 = self._make_file_record("src/safe.py", 2)

        # 10 commits from bob (inactive) + 11 commits from alice (active) = ~47.6% inactive
        bob_commits = [self._make_commit(f"b{i}", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc)) for i in range(10)]
        alice_commits = [self._make_commit(f"a{i}", "alice", 2, datetime(2024, 1, 20, tzinfo=timezone.utc)) for i in range(11)]

        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "alice")
        ledger.get_prs_for_user.return_value = [pr2]
        ledger.get_files_for_pr.side_effect = lambda n: {1: [file1_pr1], 2: [file1_pr2]}.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2]
        bundle.commits = bob_commits + alice_commits
        bundle.pull_requests = [pr1, pr2]

        result = metric.run(context)

        assert result.details["loss_count"] == 0

    def test_departed_contributor_detected(self, metric, mock_context):
        """A contributor with no commits in period is marked as departed/inactive."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/departed_code.py", 1)

        # Only charlie's old commit - he's departed
        commit_charlie_old = self._make_commit("c1", "charlie", 1, datetime(2023, 6, 1, tzinfo=timezone.utc))

        # alice touches the file now but charlie wrote it all before
        pr1 = self._make_mock_pr(1, "charlie")
        ledger.get_prs_for_user.return_value = [pr1]
        ledger.get_files_for_pr.side_effect = lambda n: {1: [file1]}.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1]
        bundle.commits = [commit_charlie_old]
        bundle.pull_requests = [pr1]

        result = metric.run(context)

        assert result.details["loss_count"] == 1
        assert result.details["at_risk_files"][0]["inactive_pct"] == 100.0
        assert "charlie" in result.details["at_risk_files"][0]["inactive_contributors"]

    def test_files_sorted_by_inactive_pct(self, metric, mock_context):
        """At-risk files are sorted by inactive percentage (highest first)."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/fully_inactive.py", 1)
        file2 = self._make_file_record("src/partially_inactive.py", 2)
        file3 = self._make_file_record("src/partially_inactive.py", 3)

        # file1: 100% bob (inactive)
        commit_bob = self._make_commit("b1", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc))

        # file2/file3: 60% charlie (inactive), 40% alice (active)
        charlie_commits = [self._make_commit(f"c{i}", "charlie", 2, datetime(2024, 1, 5, tzinfo=timezone.utc)) for i in range(6)]
        alice_commits = [self._make_commit(f"a{i}", "alice", 3, datetime(2024, 1, 20, tzinfo=timezone.utc)) for i in range(4)]

        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "charlie")
        pr3 = self._make_mock_pr(3, "alice")
        ledger.get_prs_for_user.return_value = [pr1, pr3]
        ledger.get_files_for_pr.side_effect = lambda n: {
            1: [file1], 2: [file2], 3: [file3],
        }.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1, file2, file3]
        bundle.commits = [commit_bob] + charlie_commits + alice_commits
        bundle.pull_requests = [pr1, pr2, pr3]

        result = metric.run(context)

        at_risk = result.details["at_risk_files"]
        assert len(at_risk) == 2
        # Sorted by inactive_pct: 100% first, 60% second
        assert at_risk[0]["inactive_pct"] == 100.0
        assert at_risk[1]["inactive_pct"] == 60.0

    def test_overall_loss_percentage_calculated(self, metric, mock_context):
        """Overall loss percentage is calculated across all files."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/file1.py", 1)
        file2 = self._make_file_record("src/file2.py", 2)

        # file1: 10 commits total (5 bob inactive + 5 alice active) = 50% loss
        bob_commits_1 = [self._make_commit(f"b{i}", "bob", 1, datetime(2024, 1, 5, tzinfo=timezone.utc)) for i in range(5)]
        alice_commits_1 = [self._make_commit(f"a{i}", "alice", 1, datetime(2024, 1, 20, tzinfo=timezone.utc)) for i in range(5)]

        # file2: 10 commits total (3 charlie inactive + 7 alice active) = 30% loss (below threshold)
        charlie_commits = [self._make_commit(f"c{i}", "charlie", 2, datetime(2024, 1, 5, tzinfo=timezone.utc)) for i in range(3)]
        alice_commits_2 = [self._make_commit(f"a2{i}", "alice", 2, datetime(2024, 1, 20, tzinfo=timezone.utc)) for i in range(7)]

        pr1 = self._make_mock_pr(1, "alice")
        pr2 = self._make_mock_pr(2, "alice")
        ledger.get_prs_for_user.return_value = [pr1, pr2]
        ledger.get_files_for_pr.side_effect = lambda n: {1: [file1], 2: [file2]}.get(n, [])

        bundle = ledger.bundle
        bundle.files = [file1, file2]
        bundle.commits = bob_commits_1 + alice_commits_1 + charlie_commits + alice_commits_2
        bundle.pull_requests = [pr1, pr2]

        result = metric.run(context)

        # Overall: (5 + 3) inactive out of 20 total = 40%
        assert result.details["overall_loss_pct"] == 40.0
        # Only file1 is at risk (50% >= threshold)
        assert result.details["loss_count"] == 1
