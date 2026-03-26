"""Tests for AI Code Quality metric."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext, ReviewState
from impact.metrics.plugins.authored.ai_code_quality import (
    AICodeQuality,
    _is_ai_assisted_pr,
    _compute_pr_rework_indicator,
)


def make_mock_pr(number: int, title: str, body: str = "", user_login: str = "testuser"):
    """Create a mock PR."""
    pr = MagicMock()
    pr.number = number
    pr.title = title
    pr.body = body
    pr.draft = False
    pr.merged = True
    pr.created_at = datetime(2024, 1, 15, tzinfo=timezone.utc)
    pr.additions = 100
    pr.deletions = 20
    
    user_mock = MagicMock()
    user_mock.login = user_login
    pr.user = user_mock
    
    return pr


def make_mock_review(review_id: int, state: str, submitted_at: datetime, pr_number: int):
    """Create a mock review."""
    review = MagicMock()
    review.id = review_id
    review.state = MagicMock()
    review.state.value = state
    review.submitted_at = submitted_at
    review.pull_request_number = pr_number
    return review


class TestIsAIAssistedPR:
    """Test the AI assistance detection helper."""

    def test_detects_copilot_in_title(self):
        """Test that (Copilot) in title is detected as AI-assisted."""
        pr = make_mock_pr(1, "Feature (Copilot)", "Description")
        ledger = MagicMock()
        
        # Mock _analyze_pr_for_ai in the ai_assisted_pr_rate module
        from impact.metrics.plugins.authored import ai_assisted_pr_rate
        original_analyze = ai_assisted_pr_rate._analyze_pr_for_ai
        ai_assisted_pr_rate._analyze_pr_for_ai = lambda pr, ledger: (True, "copilot", [])
        
        try:
            result = _is_ai_assisted_pr(pr, ledger)
            assert result is True
        finally:
            ai_assisted_pr_rate._analyze_pr_for_ai = original_analyze

    def test_human_pr_not_detected(self):
        """Test that normal PRs are not flagged as AI-assisted."""
        pr = make_mock_pr(1, "Manual fix", "Description")
        ledger = MagicMock()
        
        from impact.metrics.plugins.authored import ai_assisted_pr_rate
        original_analyze = ai_assisted_pr_rate._analyze_pr_for_ai
        ai_assisted_pr_rate._analyze_pr_for_ai = lambda pr, ledger: (False, None, [])
        
        try:
            result = _is_ai_assisted_pr(pr, ledger)
            assert result is False
        finally:
            ai_assisted_pr_rate._analyze_pr_for_ai = original_analyze


class TestComputeReworkIndicator:
    """Test rework indicator computation."""

    def test_no_reviews_zero_iterations(self):
        """Test that PR with no reviews has 0 iterations."""
        pr = make_mock_pr(1, "Feature", "Description")
        ledger = MagicMock()
        ledger.get_reviews_for_pr.return_value = []
        ledger.get_files_for_pr.return_value = []
        
        result = _compute_pr_rework_indicator(pr, ledger)
        assert result["review_iterations"] == 0
        assert result["files_changed"] == 0

    def test_changes_requested_counts_as_iteration(self):
        """Test that changes_requested reviews count as iterations."""
        pr = make_mock_pr(1, "Feature", "Description")
        ledger = MagicMock()
        
        review = make_mock_review(
            1, "changes_requested", 
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            1
        )
        ledger.get_reviews_for_pr.return_value = [review]
        ledger.get_review_comments_for_review.return_value = []
        ledger.get_files_for_pr.return_value = []
        
        result = _compute_pr_rework_indicator(pr, ledger)
        assert result["review_iterations"] == 1


class TestAICodeQualityMetric:
    """Test the AICodeQuality metric class."""

    @pytest.fixture
    def metric(self):
        return AICodeQuality()

    @pytest.fixture
    def mock_context(self):
        """Create a mock context."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "testuser"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        ledger = MagicMock()
        context.ledger = ledger

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "ai_code_quality"
        assert metric.name == "AI Code Quality"
        assert metric.category == "code_quality"
        assert "DevRank" in metric.frameworks

    def test_no_prs_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        ledger.get_prs_for_user.return_value = []

        result = metric.run(context)

        assert result.details["no_data"] is True
        assert result.details["total_pr_count"] == 0

    def test_computes_quality_ratio(self, metric, mock_context):
        context, ledger = mock_context

        # Create mock PRs
        ai_pr = make_mock_pr(1, "AI Feature (Copilot)", "Description")
        human_pr = make_mock_pr(2, "Human Fix", "Description")
        
        ledger.get_prs_for_user.return_value = [ai_pr, human_pr]
        
        # Mock AI detection in the ai_assisted_pr_rate module
        from impact.metrics.plugins.authored import ai_assisted_pr_rate
        original_analyze = ai_assisted_pr_rate._analyze_pr_for_ai
        
        def mock_analyze(pr, ledger):
            if "Copilot" in pr.title:
                return (True, "copilot", [])
            return (False, None, [])
        
        ai_assisted_pr_rate._analyze_pr_for_ai = mock_analyze
        
        try:
            ledger.get_reviews_for_pr.return_value = []
            ledger.get_files_for_pr.return_value = []
            
            result = metric.run(context)

            assert result.details["ai_pr_count"] == 1
            assert result.details["human_pr_count"] == 1
            assert "quality_ratio" in result.details
            assert result.details["quality_ratio"] >= 0
            assert "ai_avg_review_iterations" in result.details
            assert "human_avg_review_iterations" in result.details
        finally:
            ai_assisted_pr_rate._analyze_pr_for_ai = original_analyze
