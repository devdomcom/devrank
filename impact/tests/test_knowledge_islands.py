"""Tests for Knowledge Islands metric."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.knowledge_islands import KnowledgeIslands


class TestKnowledgeIslandsMetric:
    """Test the KnowledgeIslands metric class."""

    @pytest.fixture
    def metric(self):
        return KnowledgeIslands()

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with bundle data for commit-based ownership."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "alice"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        ledger = MagicMock()
        context.ledger = ledger

        # Bundle with files and commits (simulated)
        bundle = MagicMock()
        bundle.files = []
        bundle.commits = []
        ledger.bundle = bundle

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "knowledge_islands"
        assert metric.name == "Knowledge Islands"
        assert metric.category == "code_quality"
        assert "CodeScene" in metric.frameworks
        assert metric.ISLAND_THRESHOLD == 0.95

    def _make_commit(self, sha: str, author_login: str, pr_num: int):
        """Create a mock commit with author and PR linkage."""
        c = MagicMock()
        c.sha = sha
        c.author = MagicMock(login=author_login)
        c.pull_request_number = pr_num
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
        assert result.metric_slug == "knowledge_islands"
        assert result.details["total_files"] == 0
        assert result.details["island_count"] == 0
        assert result.details.get("no_data") is True

    def test_no_islands_when_shared_ownership(self, metric, mock_context):
        """Files with multiple contributors < 95% each are not islands."""
        context, ledger = mock_context

        # File1 and File2 touched by both PR1 (alice) and PR2 (bob)
        file1_pr1 = self._make_file_record("src/main.py", 1)
        file1_pr2 = self._make_file_record("src/main.py", 2)
        file2_pr1 = self._make_file_record("src/utils.py", 1)
        file2_pr2 = self._make_file_record("src/utils.py", 2)

        commit_alice_pr1 = self._make_commit("a1", "alice", 1)
        commit_bob_pr2 = self._make_commit("b1", "bob", 2)

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2, file2_pr1, file2_pr2]
        bundle.commits = [commit_alice_pr1, commit_bob_pr2]

        result = metric.run(context)

        assert result.details["island_count"] == 0
        assert result.details["total_files"] == 2
        assert "No knowledge islands" in result.summary

    def test_detects_knowledge_island(self, metric, mock_context):
        """File with one author >= 95% is flagged as island."""
        context, ledger = mock_context

        # file1: only alice commits on PR1 -> 100% alice ISLAND
        # file2: alice and bob both commit on PR2 -> 50/50 NOT island
        file1_pr1 = self._make_file_record("src/secret.py", 1)
        file2_pr2 = self._make_file_record("src/shared.py", 2)

        commit_alice_pr1 = self._make_commit("a1", "alice", 1)
        commit_alice_pr2 = self._make_commit("a2", "alice", 2)
        commit_bob_pr2 = self._make_commit("b1", "bob", 2)

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file2_pr2]
        bundle.commits = [commit_alice_pr1, commit_alice_pr2, commit_bob_pr2]

        result = metric.run(context)

        assert result.details["island_count"] == 1
        assert result.details["total_files"] == 2
        islands = result.details["islands"]
        assert len(islands) == 1
        assert islands[0]["file"] == "src/secret.py"
        assert islands[0]["owner"] == "alice"
        assert islands[0]["ownership_pct"] == 100.0
        assert "Found 1 knowledge island" in result.summary

    def test_detects_multiple_islands(self, metric, mock_context):
        """Multiple files with single owners are all flagged."""
        context, ledger = mock_context

        # file1: alice only on PR1
        # file2: bob only on PR2
        # shared: both on PR3
        file1 = self._make_file_record("src/alice_code.py", 1)
        file2 = self._make_file_record("src/bob_code.py", 2)
        shared_pr3 = self._make_file_record("src/shared.py", 3)

        commit_alice_pr1 = self._make_commit("a1", "alice", 1)
        commit_bob_pr2 = self._make_commit("b1", "bob", 2)
        commit_alice_pr3 = self._make_commit("a3", "alice", 3)
        commit_bob_pr3 = self._make_commit("b3", "bob", 3)

        bundle = ledger.bundle
        bundle.files = [file1, file2, shared_pr3]
        bundle.commits = [commit_alice_pr1, commit_bob_pr2, commit_alice_pr3, commit_bob_pr3]

        result = metric.run(context)

        assert result.details["island_count"] == 2
        assert result.details["total_files"] == 3
        islands = result.details["islands"]
        island_files = {i["file"] for i in islands}
        assert "src/alice_code.py" in island_files
        assert "src/bob_code.py" in island_files
        assert "src/shared.py" not in island_files

    def test_island_threshold_at_95_percent(self, metric, mock_context):
        """Ownership at exactly 95% is an island; below is not."""
        context, ledger = mock_context

        file1_pr1 = self._make_file_record("src/edge.py", 1)
        file1_pr2 = self._make_file_record("src/edge.py", 2)

        # 19 commits by alice on PR1, 1 by bob on PR2 = 95% alice
        commits = [self._make_commit(f"a{i}", "alice", 1) for i in range(19)]
        commits.append(self._make_commit("b1", "bob", 2))

        bundle = ledger.bundle
        bundle.files = [file1_pr1, file1_pr2]
        bundle.commits = commits

        result = metric.run(context)

        assert result.details["island_count"] == 1
        assert result.details["islands"][0]["ownership_pct"] == 95.0

    def test_ai_author_detected_as_island(self, metric, mock_context):
        """AI authors (Claude, Copilot) are treated like any author."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/ai_generated.py", 1)
        commit_claude = self._make_commit("c1", "claude", 1)

        bundle = ledger.bundle
        bundle.files = [file1]
        bundle.commits = [commit_claude]

        result = metric.run(context)

        assert result.details["island_count"] == 1
        assert result.details["islands"][0]["owner"] == "claude"
        assert result.details["islands"][0]["ownership_pct"] == 100.0

    def test_islands_sorted_by_ownership_pct(self, metric, mock_context):
        """Islands are sorted by ownership percentage (highest first)."""
        context, ledger = mock_context

        file1 = self._make_file_record("src/alice_100.py", 1)
        file2_pr2 = self._make_file_record("src/alice_95.py", 2)
        file2_pr3 = self._make_file_record("src/alice_95.py", 3)

        # file1: alice 1 commit = 100% island
        commit_alice_100 = self._make_commit("a1", "alice", 1)
        # file2: 19 alice commits, 1 bob commit = 95% alice island
        commits_alice_95 = [self._make_commit(f"a{i}", "alice", 2) for i in range(19)]
        commit_bob_5 = self._make_commit("b1", "bob", 3)

        bundle = ledger.bundle
        bundle.files = [file1, file2_pr2, file2_pr3]
        bundle.commits = [commit_alice_100] + commits_alice_95 + [commit_bob_5]

        result = metric.run(context)

        islands = result.details["islands"]
        assert len(islands) == 2
        # Sorted by ownership: 100% first, 95% second
        assert islands[0]["ownership_pct"] == 100.0
        assert islands[1]["ownership_pct"] == 95.0
