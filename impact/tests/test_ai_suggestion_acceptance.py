"""Tests for AI Suggestion Acceptance metric."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext
from impact.metrics.plugins.authored.ai_suggestion_acceptance import (
    AISuggestionAcceptance,
    _is_ai_bot,
    _extract_suggestion_blocks,
    _suggestion_accepted,
)


class TestIsAIBot:
    """Test AI bot detection helper."""

    def test_detects_copilot(self):
        assert _is_ai_bot("Copilot") is True

    def test_detects_codeant_bot(self):
        assert _is_ai_bot("codeant-ai-for-open-source[bot]") is True

    def test_detects_bito_bot(self):
        assert _is_ai_bot("bito-code-review[bot]") is True

    def test_detects_korbit_bot(self):
        assert _is_ai_bot("korbit-ai[bot]") is True

    def test_detects_case_insensitive(self):
        assert _is_ai_bot("COPILOT") is True
        assert _is_ai_bot("copilot") is True

    def test_human_user_not_detected(self):
        assert _is_ai_bot("johndoe") is False

    def test_empty_login(self):
        assert _is_ai_bot("") is False
        assert _is_ai_bot(None) is False


class TestExtractSuggestionBlocks:
    """Test suggestion block extraction."""

    def test_extracts_single_suggestion(self):
        body = "Consider this change:\n```suggestion\nx = 1\ny = 2\n```\nThanks!"
        blocks = _extract_suggestion_blocks(body)
        assert len(blocks) == 1
        assert "x = 1" in blocks[0]
        assert "y = 2" in blocks[0]

    def test_extracts_multiple_suggestions(self):
        body = "```suggestion\na = 1\n```\n\n```suggestion\nb = 2\n```"
        blocks = _extract_suggestion_blocks(body)
        assert len(blocks) == 2

    def test_handles_case_insensitive(self):
        body = "```SUGGESTION\ncode\n```"
        blocks = _extract_suggestion_blocks(body)
        assert len(blocks) == 1
        assert "code" in blocks[0]

    def test_empty_body_returns_empty(self):
        assert _extract_suggestion_blocks("") == []
        assert _extract_suggestion_blocks(None) == []

    def test_no_suggestion_blocks_returns_empty(self):
        body = "Just a regular comment without code blocks."
        assert _extract_suggestion_blocks(body) == []


class TestSuggestionAccepted:
    """Test suggestion acceptance heuristic."""

    def test_detects_code_in_patch(self):
        """Suggestion code found in patch (after stripping diff markers)."""
        suggested = "x = 1"
        patch = "+x = 1"
        assert _suggestion_accepted(suggested, patch) is True

    def test_detects_multiline_in_clean_patch(self):
        """Multiple lines of suggestion found in clean patch."""
        suggested = "def foo():\n    return 1"
        patch = "def foo():\n    return 1\n    # extra"
        assert _suggestion_accepted(suggested, patch) is True

    def test_no_match_returns_false(self):
        suggested = "completely different code"
        patch = "some other code here"
        assert _suggestion_accepted(suggested, patch) is False

    def test_no_patch_returns_false(self):
        assert _suggestion_accepted("code", None) is False

    def test_empty_suggestion_returns_false(self):
        assert _suggestion_accepted("", "patch") is False

    def test_handles_diff_markers(self):
        """Suggestion code should match even with +/- markers in patch."""
        suggested = "y = 2"
        patch = "--- a/file.py\n+++ b/file.py\n@@\n+y = 2"
        assert _suggestion_accepted(suggested, patch) is True


class TestAISuggestionAcceptanceMetric:
    """Test the AISuggestionAcceptance metric class."""

    @pytest.fixture
    def metric(self):
        return AISuggestionAcceptance()

    @pytest.fixture
    def mock_context(self):
        context = MagicMock(spec=MetricContext)
        context.user_login = "testuser"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        ledger = MagicMock()
        context.ledger = ledger
        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "ai_suggestion_acceptance"
        assert metric.name == "AI Suggestion Acceptance"
        assert metric.category == "contextual"
        assert "DevRank" in metric.frameworks

    def test_no_prs_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        ledger.get_prs_for_user.return_value = []

        result = metric.run(context)

        assert result.details["no_data"] is True
        assert result.details["suggestions_made"] == 0

    def test_no_ai_suggestions_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context

        pr = MagicMock()
        pr.number = 1
        pr.title = "Test PR"
        pr.draft = False
        pr.merged = True

        ledger.get_prs_for_user.return_value = [pr]
        ledger.get_comments_for_pr.return_value = []  # No comments

        result = metric.run(context)

        assert result.details["no_data"] is True
        assert result.details["suggestions_made"] == 0

    def test_counts_suggestions_from_ai_bots(self, metric, mock_context):
        context, ledger = mock_context

        pr = MagicMock()
        pr.number = 1
        pr.title = "Test PR"
        pr.draft = False
        pr.merged = True

        ledger.get_prs_for_user.return_value = [pr]

        # Create AI bot comment with suggestion
        comment = MagicMock()
        comment.id = 1
        comment.user.login = "Copilot"
        comment.body = "Suggestion:\n```suggestion\nx = 1\n```"
        comment.path = "file.py"
        comment.created_at = datetime(2024, 1, 10, tzinfo=timezone.utc)
        ledger.get_comments_for_pr.return_value = [comment]

        # File with patch that includes the suggestion
        file_rec = MagicMock()
        file_rec.filename = "file.py"
        file_rec.patch = "x = 1"
        ledger.get_files_for_pr.return_value = [file_rec]

        result = metric.run(context)

        assert result.details["suggestions_made"] == 1
        assert result.details["suggestions_accepted"] == 1
        assert result.details["acceptance_rate"] == 100.0

    def test_detects_dismissed_suggestions(self, metric, mock_context):
        context, ledger = mock_context

        pr = MagicMock()
        pr.number = 1
        pr.title = "Test PR"
        pr.draft = False
        pr.merged = True

        ledger.get_prs_for_user.return_value = [pr]

        # AI bot suggestion that is NOT in the patch
        comment = MagicMock()
        comment.id = 1
        comment.user.login = "bito-code-review[bot]"
        comment.body = "```suggestion\ncompletely different code\n```"
        comment.path = "file.py"
        comment.created_at = datetime(2024, 1, 10, tzinfo=timezone.utc)
        ledger.get_comments_for_pr.return_value = [comment]

        # File patch doesn't contain the suggestion
        file_rec = MagicMock()
        file_rec.filename = "file.py"
        file_rec.patch = "some other code"
        ledger.get_files_for_pr.return_value = [file_rec]

        result = metric.run(context)

        assert result.details["suggestions_made"] == 1
        assert result.details["suggestions_accepted"] == 0
        assert result.details["suggestions_dismissed"] == 1
        assert result.details["acceptance_rate"] == 0.0

    def test_mixed_accepted_and_dismissed(self, metric, mock_context):
        context, ledger = mock_context

        pr = MagicMock()
        pr.number = 1
        pr.title = "Test PR"
        pr.draft = False
        pr.merged = True

        ledger.get_prs_for_user.return_value = [pr]

        # Two suggestions: one accepted, one dismissed
        comments = []
        for i, (bot, code, accepted) in enumerate([
            ("Copilot", "accepted_code", True),
            ("codeant-ai-for-open-source[bot]", "dismissed_code", False),
        ]):
            c = MagicMock()
            c.id = i
            c.user.login = bot
            c.body = f"```suggestion\n{code}\n```"
            c.path = "file.py"
            c.created_at = datetime(2024, 1, 10, tzinfo=timezone.utc)
            comments.append(c)

        ledger.get_comments_for_pr.return_value = comments

        file_rec = MagicMock()
        file_rec.filename = "file.py"
        file_rec.patch = "accepted_code"  # Only first suggestion present
        ledger.get_files_for_pr.return_value = [file_rec]

        result = metric.run(context)

        assert result.details["suggestions_made"] == 2
        assert result.details["suggestions_accepted"] == 1
        assert result.details["suggestions_dismissed"] == 1
        assert result.details["acceptance_rate"] == 50.0
