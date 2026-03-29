"""Tests for AI Adoption Rate metric (per-engineer)."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.ai_adoption_rate import AIAdoptionRate


def make_mock_pr(number: int, title: str, body: str = "", user_login: str = "testuser"):
    """Create a mock PR with all required attributes."""
    pr = MagicMock()
    pr.number = number
    pr.title = title
    pr.body = body
    pr.draft = False
    pr.merged = True
    pr.created_at = datetime(2024, 1, 15, tzinfo=timezone.utc)
    pr.additions = 10
    pr.deletions = 5

    # Nested user mock
    user_mock = MagicMock()
    user_mock.login = user_login
    pr.user = user_mock

    return pr


class TestAIAdoptionRateMetric:
    """Test the AIAdoptionRate metric class (per-engineer focus)."""

    @pytest.fixture
    def metric(self):
        return AIAdoptionRate()

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with test PRs."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "testuser"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        # Mock ledger
        ledger = MagicMock()
        context.ledger = ledger

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "ai_adoption_rate"
        assert metric.name == "AI Adoption Rate"
        assert metric.category == "contextual"
        assert "DevRank" in metric.frameworks

    def test_no_prs_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        ledger.get_prs_for_user.return_value = []

        result = metric.run(context)

        assert result.metric_slug == "ai_adoption_rate"
        assert result.details["has_adopted_ai"] is False
        assert result.details["total_pr_count"] == 0
        assert result.details.get("no_data") is True

    def test_human_prs_only_no_adoption(self, metric, mock_context):
        """When no PRs have AI signatures, has_adopted_ai is False."""
        context, ledger = mock_context
        
        pr = make_mock_pr(1, "Fix bug", "Normal description", "testuser")
        ledger.get_prs_for_user.return_value = [pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert result.details["has_adopted_ai"] is False
        assert result.details["ai_pr_count"] == 0
        assert result.details["total_pr_count"] == 1
        assert result.details["tools_used"] == []

    def test_copilot_pr_shows_adoption(self, metric, mock_context):
        """PR with Copilot signature shows has_adopted_ai = True."""
        context, ledger = mock_context
        
        pr = make_mock_pr(
            1,
            "Add feature (Copilot)",
            "Co-authored-by: GitHub Copilot",
            "testuser"
        )
        ledger.get_prs_for_user.return_value = [pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert result.details["has_adopted_ai"] is True
        assert result.details["ai_pr_count"] == 1
        assert result.details["total_pr_count"] == 1
        assert "copilot" in result.details["tools_used"]

    def test_claude_pr_shows_adoption(self, metric, mock_context):
        """PR with Claude Code signature shows has_adopted_ai = True."""
        context, ledger = mock_context
        
        pr = make_mock_pr(
            1,
            "Refactor",
            "🤖 Generated with [Claude Code](https://claude.com/claude-code)",
            "testuser"
        )
        ledger.get_prs_for_user.return_value = [pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert result.details["has_adopted_ai"] is True
        assert "claude" in result.details["tools_used"]

    def test_multiple_prs_some_ai(self, metric, mock_context):
        """Engineer with mix of AI and human PRs shows adoption."""
        context, ledger = mock_context
        
        ai_pr = make_mock_pr(1, "Fix (Copilot)", "", "testuser")
        human_pr = make_mock_pr(2, "Add feature", "Normal", "testuser")
        
        ledger.get_prs_for_user.return_value = [ai_pr, human_pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert result.details["has_adopted_ai"] is True
        assert result.details["ai_pr_count"] == 1
        assert result.details["total_pr_count"] == 2
        assert "copilot" in result.details["tools_used"]

    def test_multiple_ai_tools_detected(self, metric, mock_context):
        """Multiple AI tools used are all reported."""
        context, ledger = mock_context
        
        copilot_pr = make_mock_pr(1, "Fix (Copilot)", "", "testuser")
        claude_pr = make_mock_pr(2, "Refactor (Claude)", "Generated with Claude", "testuser")
        
        ledger.get_prs_for_user.return_value = [copilot_pr, claude_pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert result.details["has_adopted_ai"] is True
        assert "copilot" in result.details["tools_used"]
        assert "claude" in result.details["tools_used"]
        assert len(result.details["tools_used"]) == 2

    def test_summary_reflects_adoption(self, metric, mock_context):
        """Summary clearly states adoption status."""
        context, ledger = mock_context
        
        pr = make_mock_pr(1, "Fix (Copilot)", "", "testuser")
        ledger.get_prs_for_user.return_value = [pr]
        ledger.get_commits_for_pr.return_value = []
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []

        result = metric.run(context)

        assert "AI adopted" in result.summary
        assert "copilot" in result.summary.lower()
