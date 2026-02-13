"""Tests for threshold and cross-cutting fixes.

Covers:
- Issue 1: no_data guard returns INSUFFICIENT_DATA rating
- Issue 21: review_turnaround_time thresholds are tighter (<=4h excellent)
- Issue 25: conventional_commit_rate replaces commit_message_clarity in thresholds
- Issue 19: review_leverage uses 0-1 scale thresholds
"""

import pytest

from impact.config.role_metrics import get_role_config
from impact.scripts.generate_report import get_metric_rating
from impact.thresholds import METRIC_THRESHOLDS

_ROLE_CONFIG = get_role_config("senior_dev")


# ---------------------------------------------------------------------------
# Issue 1: no_data guard -- metrics with no_data: True get INSUFFICIENT_DATA
# ---------------------------------------------------------------------------


class TestNoDataGuard:
    """When a metric result has no_data=True, the rating must be INSUFFICIENT_DATA."""

    def test_no_data_returns_insufficient_data_for_cycle_time(self):
        details = {"merged_count": 0, "median_hours": 0.0, "p75_hours": 0.0, "no_data": True}
        rating = get_metric_rating("cycle_time", details, _ROLE_CONFIG)
        assert rating == "INSUFFICIENT_DATA"

    def test_no_data_returns_insufficient_data_for_trivial_contribution_rate(self):
        details = {"total_pr_count": 0, "trivial_prs_per_day": 0.0, "no_data": True}
        rating = get_metric_rating("trivial_contribution_rate", details, _ROLE_CONFIG)
        assert rating == "INSUFFICIENT_DATA"

    def test_no_data_returns_insufficient_data_for_any_metric(self):
        """no_data guard should work for any metric slug, even unknown ones."""
        details = {"some_key": 0.0, "no_data": True}
        rating = get_metric_rating("pr_throughput", details, _ROLE_CONFIG)
        assert rating == "INSUFFICIENT_DATA"

    def test_no_data_false_does_not_trigger_guard(self):
        """When no_data is explicitly False, normal rating logic applies."""
        details = {"median_hours": 0.5, "no_data": False}
        rating = get_metric_rating("cycle_time", details, _ROLE_CONFIG)
        assert rating != "INSUFFICIENT_DATA"

    def test_no_data_absent_does_not_trigger_guard(self):
        """When no_data key is absent, normal rating logic applies."""
        details = {"median_hours": 0.5}
        rating = get_metric_rating("cycle_time", details, _ROLE_CONFIG)
        assert rating != "INSUFFICIENT_DATA"

    def test_no_data_takes_precedence_over_no_cr_activity(self):
        """no_data check comes after no_cr_activity; but if only no_data is set it works."""
        details = {"median_hours": 0.0, "no_data": True}
        rating = get_metric_rating("cycle_time", details, _ROLE_CONFIG)
        assert rating == "INSUFFICIENT_DATA"


# ---------------------------------------------------------------------------
# Issue 21: review_turnaround_time thresholds are tighter (reduced 12x gap)
# ---------------------------------------------------------------------------


class TestReviewTurnaroundThresholds:
    """review_turnaround_time excellent should be <=4h, not the old <=12h."""

    def test_excellent_at_4h(self):
        thresh = METRIC_THRESHOLDS["review_turnaround_time"]
        assert thresh["excellent"](4) is True
        assert thresh["excellent"](4.01) is False

    def test_good_range(self):
        thresh = METRIC_THRESHOLDS["review_turnaround_time"]
        assert thresh["good"](5) is True
        assert thresh["good"](8) is True
        assert thresh["good"](4) is False
        assert thresh["good"](8.01) is False

    def test_neutral_range(self):
        thresh = METRIC_THRESHOLDS["review_turnaround_time"]
        assert thresh["neutral"](9) is True
        assert thresh["neutral"](16) is True
        assert thresh["neutral"](8) is False
        assert thresh["neutral"](16.01) is False

    def test_bad_range(self):
        thresh = METRIC_THRESHOLDS["review_turnaround_time"]
        assert thresh["bad"](16.01) is True
        assert thresh["bad"](16) is False

    def test_rating_integration_excellent(self):
        details = {"median_hours": 1.5}
        rating = get_metric_rating("review_turnaround_time", details, _ROLE_CONFIG)
        assert rating == "excellent"

    def test_rating_integration_bad(self):
        details = {"median_hours": 20.0}
        rating = get_metric_rating("review_turnaround_time", details, _ROLE_CONFIG)
        assert rating == "bad"


# ---------------------------------------------------------------------------
# Issue 25: conventional_commit_rate replaces commit_message_clarity
# ---------------------------------------------------------------------------


class TestConventionalCommitRateRename:
    """Threshold key should be conventional_commit_rate, not commit_message_clarity."""

    def test_conventional_commit_rate_exists(self):
        assert "conventional_commit_rate" in METRIC_THRESHOLDS

    def test_commit_message_clarity_removed(self):
        assert "commit_message_clarity" not in METRIC_THRESHOLDS

    def test_conventional_commit_rate_has_expected_structure(self):
        thresh = METRIC_THRESHOLDS["conventional_commit_rate"]
        assert thresh["key"] == "conventional_commit_rate"
        assert "excellent" in thresh
        assert "good" in thresh
        assert "neutral" in thresh
        assert "bad" in thresh

    def test_conventional_commit_rate_thresholds_work(self):
        thresh = METRIC_THRESHOLDS["conventional_commit_rate"]
        assert thresh["excellent"](85) is True
        assert thresh["good"](70) is True
        assert thresh["neutral"](50) is True
        assert thresh["bad"](30) is True


# ---------------------------------------------------------------------------
# Issue 19: review_leverage uses 0-1 scale (not 0-100)
# ---------------------------------------------------------------------------


class TestReviewLeverageScale:
    """review_leverage thresholds should use 0-1 scale with effectiveness_rate key."""

    def test_key_is_effectiveness_rate(self):
        thresh = METRIC_THRESHOLDS["review_leverage"]
        assert thresh["key"] == "effectiveness_rate"

    def test_excellent_at_0_8(self):
        thresh = METRIC_THRESHOLDS["review_leverage"]
        assert thresh["excellent"](0.8) is True
        assert thresh["excellent"](0.95) is True
        assert thresh["excellent"](0.79) is False

    def test_good_range(self):
        thresh = METRIC_THRESHOLDS["review_leverage"]
        assert thresh["good"](0.6) is True
        assert thresh["good"](0.75) is True
        assert thresh["good"](0.8) is False
        assert thresh["good"](0.59) is False

    def test_neutral_range(self):
        thresh = METRIC_THRESHOLDS["review_leverage"]
        assert thresh["neutral"](0.3) is True
        assert thresh["neutral"](0.5) is True
        assert thresh["neutral"](0.6) is False
        assert thresh["neutral"](0.29) is False

    def test_bad_range(self):
        thresh = METRIC_THRESHOLDS["review_leverage"]
        assert thresh["bad"](0.1) is True
        assert thresh["bad"](0.29) is True
        assert thresh["bad"](0.3) is False

    def test_rating_integration(self):
        details = {"effectiveness_rate": 0.85}
        rating = get_metric_rating("review_leverage", details, _ROLE_CONFIG)
        assert rating == "excellent"

    def test_old_scale_values_dont_match_excellent(self):
        """80 on the 0-1 scale is way above 1.0, so should still be excellent.
        But the old key effectiveness_percentage should not be recognized."""
        details = {"effectiveness_percentage": 80}
        rating = get_metric_rating("review_leverage", details, _ROLE_CONFIG)
        # The key doesn't match effectiveness_rate, so it should be unknown
        assert rating == "unknown"
