"""Tests for Knowledge Sharing Index metric."""

import pytest
import math
from datetime import datetime, timezone
from unittest.mock import MagicMock

from impact.domain.models import MetricContext, User, UserType
from impact.metrics.plugins.influence.knowledge_sharing_index import KnowledgeSharingIndex


class TestKnowledgeSharingIndexMetric:
    """Test the KnowledgeSharingIndex metric class."""

    @pytest.fixture
    def metric(self):
        return KnowledgeSharingIndex()

    def _make_user(self, login: str, is_bot: bool = False):
        """Create a mock user."""
        user = MagicMock(spec=User)
        user.login = login
        user.is_bot = is_bot
        user.type = UserType.BOT if is_bot else UserType.USER
        return user

    def _make_review(self, reviewer_login: str, pr_number: int, is_bot: bool = False, submitted_at: datetime | None = None):
        """Create a mock review."""
        review = MagicMock()
        review.user = self._make_user(reviewer_login, is_bot)
        review.pull_request_number = pr_number
        review.submitted_at = submitted_at or datetime(2024, 1, 15, tzinfo=timezone.utc)
        return review

    def _make_mock_pr(self, number: int, author_login: str):
        """Create a mock PR."""
        pr = MagicMock()
        pr.number = number
        pr.user = self._make_user(author_login)
        return pr

    @pytest.fixture
    def mock_context(self):
        """Create a mock context with bundle data."""
        context = MagicMock(spec=MetricContext)
        context.user_login = "alice"
        context.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        context.end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        ledger = MagicMock()
        context.ledger = ledger

        bundle = MagicMock()
        bundle.reviews = []
        bundle.pull_requests = []
        ledger.bundle = bundle

        ledger.get_pr.return_value = None

        return context, ledger

    def test_metric_properties(self, metric):
        assert metric.slug == "knowledge_sharing_index"
        assert metric.name == "Knowledge Sharing Index"
        assert metric.category == "review_impact"
        assert "Network" in metric.frameworks
        assert metric.MIN_REVIEWERS == 3
        assert metric.MIN_REVIEWS == 5

    def test_no_reviews_returns_no_data(self, metric, mock_context):
        context, ledger = mock_context
        result = metric.run(context)
        assert result.metric_slug == "knowledge_sharing_index"
        assert result.details["sharing_index"] == 0.0
        assert result.details["reviewer_count"] == 0
        assert result.details["total_reviews"] == 0
        assert result.details.get("no_data") is True

    def test_single_reviewer_zero_index(self, metric, mock_context):
        """Single reviewer has sharing index of 0 (no sharing possible)."""
        context, ledger = mock_context

        reviews = [
            self._make_review("alice", 1),
            self._make_review("alice", 2),
            self._make_review("alice", 3),
        ]
        ledger.bundle.reviews = reviews

        result = metric.run(context)

        assert result.details["sharing_index"] == 0.0
        assert result.details["reviewer_count"] == 1
        assert result.details["total_reviews"] == 3
        assert result.details.get("no_data") is True  # Insufficient reviewers

    def test_perfect_sharing_two_reviewers(self, metric, mock_context):
        """Two reviewers with equal reviews = high sharing index."""
        context, ledger = mock_context

        reviews = [
            self._make_review("alice", 1),
            self._make_review("bob", 2),
            self._make_review("alice", 3),
            self._make_review("bob", 4),
        ]
        ledger.bundle.reviews = reviews

        result = metric.run(context)

        assert result.details["reviewer_count"] == 2
        assert result.details["total_reviews"] == 4
        # With 2 reviewers and equal distribution, entropy = log(2), max = log(2)
        # So index should be 1.0
        assert result.details["sharing_index"] == 1.0

    def test_bot_reviews_excluded(self, metric, mock_context):
        """Bot reviews should not count toward sharing index."""
        context, ledger = mock_context

        reviews = [
            self._make_review("alice", 1),
            self._make_review("dependabot[bot]", 2, is_bot=True),
            self._make_review("bob", 3),
            self._make_review("codecov[bot]", 4, is_bot=True),
        ]
        ledger.bundle.reviews = reviews

        result = metric.run(context)

        # Only alice and bob count (2 human reviewers)
        assert result.details["reviewer_count"] == 2
        assert result.details["total_reviews"] == 2  # Only human reviews
        assert result.details["bot_review_count"] == 2
        assert result.details["sharing_index"] == 1.0  # Equal distribution

    def test_self_reviews_excluded(self, metric, mock_context):
        """Self-reviews (reviewer is PR author) should be excluded."""
        context, ledger = mock_context

        reviews = [
            self._make_review("alice", 1),  # alice reviews PR1 (author=bob) - not self
            self._make_review("bob", 1),  # bob reviews PR1 (author=bob) - self-review
            self._make_review("alice", 2),  # alice reviews PR2 (author=charlie) - not self
            self._make_review("charlie", 2),  # charlie reviews PR2 (author=charlie) - self-review
        ]
        ledger.bundle.reviews = reviews

        # Set up PR lookups
        pr1 = self._make_mock_pr(1, "bob")
        pr2 = self._make_mock_pr(2, "charlie")
        ledger.get_pr.side_effect = lambda n: {1: pr1, 2: pr2}.get(n)

        ledger.bundle.reviews = reviews

        result = metric.run(context)

        # bob's review on PR1 and charlie's review on PR2 are self-reviews
        assert result.details["self_review_count"] == 2
        assert result.details["total_reviews"] == 2  # Excluding self-reviews (alice's 2 reviews)
        assert result.details["reviewer_count"] == 1  # Only alice (bob and charlie only had self-reviews)

    def test_uneven_distribution_lower_index(self, metric, mock_context):
        """Uneven review distribution results in lower sharing index."""
        context, ledger = mock_context

        # 3 reviewers: alice does 8, bob does 1, charlie does 1
        reviews = []
        for i in range(8):
            reviews.append(self._make_review("alice", i))
        reviews.append(self._make_review("bob", 8))
        reviews.append(self._make_review("charlie", 9))

        ledger.bundle.reviews = reviews

        result = metric.run(context)

        assert result.details["reviewer_count"] == 3
        assert result.details["total_reviews"] == 10
        # With uneven distribution, index should be < 1.0
        assert result.details["sharing_index"] < 1.0
        # But still > 0 since there are multiple reviewers
        assert result.details["sharing_index"] > 0.0

    def test_minimum_thresholds_for_data(self, metric, mock_context):
        """Metric should mark no_data when below minimum thresholds."""
        context, ledger = mock_context

        # Only 2 reviewers with 4 reviews (below MIN_REVIEWERS=3, MIN_REVIEWS=5)
        reviews = [
            self._make_review("alice", 1),
            self._make_review("bob", 2),
            self._make_review("alice", 3),
            self._make_review("bob", 4),
        ]
        ledger.bundle.reviews = reviews

        result = metric.run(context)

        assert result.details["reviewer_count"] == 2
        assert result.details.get("no_data") is True

    def test_entropy_calculation(self, metric):
        """Test the entropy calculation directly."""
        # Equal distribution: 3 reviewers, 3 reviews each
        reviewer_counts = {"alice": 3, "bob": 3, "charlie": 3}
        total = 9

        index, details = metric._calculate_sharing_index(reviewer_counts, total)

        # Max entropy for 3 reviewers is log(3)
        expected_max_entropy = math.log(3)
        assert details["max_entropy"] == expected_max_entropy

        # Equal distribution gives full entropy
        expected_entropy = -sum((3/9) * math.log(3/9) for _ in range(3))
        assert abs(details["entropy"] - expected_entropy) < 0.001
        assert abs(index - 1.0) < 0.0001

    def test_reviewer_distribution_in_details(self, metric, mock_context):
        """Top reviewers should be listed in details."""
        context, ledger = mock_context

        reviews = []
        for i in range(5):
            reviews.append(self._make_review("alice", i))
        for i in range(5, 10):
            reviews.append(self._make_review("bob", i))

        ledger.bundle.reviews = reviews

        result = metric.run(context)

        distribution = result.details["reviewer_distribution"]
        assert "alice" in distribution
        assert "bob" in distribution
        assert distribution["alice"] == 5
        assert distribution["bob"] == 5

    def test_summary_includes_top_and_bottom_reviewers(self, metric, mock_context):
        """Summary should mention top and bottom reviewers."""
        context, ledger = mock_context

        reviews = []
        for i in range(8):
            reviews.append(self._make_review("alice", i))
        reviews.append(self._make_review("bob", 8))
        reviews.append(self._make_review("charlie", 9))

        ledger.bundle.reviews = reviews

        result = metric.run(context)

        assert "alice" in result.summary
        assert "charlie" in result.summary
        assert "8" in result.summary  # alice's count
        assert "1" in result.summary  # charlie's count

    def test_reviews_outside_window_excluded(self, metric, mock_context):
        """Reviews outside the analysis window should be excluded."""
        context, ledger = mock_context

        # Window is Jan 1-31, 2024
        reviews_in_window = [
            self._make_review("alice", 1, submitted_at=datetime(2024, 1, 10, tzinfo=timezone.utc)),
            self._make_review("bob", 2, submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc)),
            self._make_review("charlie", 3, submitted_at=datetime(2024, 1, 20, tzinfo=timezone.utc)),
        ]
        # These should be excluded
        reviews_outside_window = [
            self._make_review("alice", 4, submitted_at=datetime(2023, 12, 15, tzinfo=timezone.utc)),  # Before window
            self._make_review("bob", 5, submitted_at=datetime(2024, 2, 15, tzinfo=timezone.utc)),  # After window
            self._make_review("david", 6, submitted_at=datetime(2023, 6, 1, tzinfo=timezone.utc)),  # Way before
        ]

        ledger.bundle.reviews = reviews_in_window + reviews_outside_window

        result = metric.run(context)

        # Only 3 reviewers from within the window
        assert result.details["reviewer_count"] == 3
        assert result.details["total_reviews"] == 3
        # david should not appear since all his reviews are outside window
        assert "david" not in result.details["reviewer_distribution"]
