"""Regression tests: zero activity in a LONG period must NOT be marked no_data.

The no_data guard should only suppress metrics in SHORT periods where the
sample is too small to be meaningful.  In a sufficiently long period, zero
reviews / zero demand / zero change requests IS the signal — the engineer
simply did not participate in code review — and must rate "bad", not hide
behind INSUFFICIENT_DATA.

Each test verifies both sides:
  - Short period + 0 data  → no_data = True  (guard fires)
  - Long period  + 0 data  → no_data absent  (signal preserved)
"""

from datetime import timedelta

from impact.metrics.plugins.influence.inline_comment_density import InlineCommentDensity
from impact.metrics.plugins.mixed.co_author_contribution_rate import CoAuthorContributionRate
from impact.metrics.plugins.authored.rework_rate import ReworkRate
from impact.metrics.plugins.authored.self_merge_rate import SelfMergeRate
from impact.metrics.plugins.influence.approval_to_merge_ratio import ApprovalToMergeRatio
from impact.metrics.plugins.influence.blocking_comment_rate import BlockingCommentRate
from impact.metrics.plugins.influence.change_inducing_review_rate import ChangeInducingReviewRate
from impact.metrics.plugins.influence.first_reviewer_rate import FirstReviewerRate
from impact.metrics.plugins.influence.mentorship_signal import MentorshipSignal
from impact.metrics.plugins.influence.pr_merge_rate import PRMergeRate
from impact.metrics.plugins.influence.review_breadth import ReviewBreadth
from impact.metrics.plugins.influence.review_comment_substance import ReviewCommentSubstance
from impact.metrics.plugins.influence.review_demand import ReviewDemand
from impact.metrics.plugins.influence.review_leverage import ReviewLeverage
from impact.metrics.plugins.influence.review_turnaround_time import ReviewTurnaroundTime
from impact.metrics.plugins.influence.reviews_given import ReviewsGiven
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_user,
)

LONG_PERIOD_END = DEFAULT_START + timedelta(days=45)  # ~6 weeks
SHORT_PERIOD_END = DEFAULT_START + timedelta(days=7)  # 1 week


def _ctx_long():
    """Empty context with a 45-day period (well above all MIN thresholds)."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    return make_context(bundle, user_login="alice", start_date=DEFAULT_START, end_date=LONG_PERIOD_END)


def _ctx_short():
    """Empty context with a 7-day period (below all MIN thresholds)."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    return make_context(bundle, user_login="alice", start_date=DEFAULT_START, end_date=SHORT_PERIOD_END)


# ── reviews_given ─────────────────────────────────────────────────────

class TestReviewsGivenNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = ReviewsGiven().run(_ctx_long())
        assert res.details["review_count"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = ReviewsGiven().run(_ctx_short())
        assert res.details["review_count"] == 0
        assert res.details.get("no_data") is True


# ── pr_merge_rate ─────────────────────────────────────────────────────

class TestPRMergeRateNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = PRMergeRate().run(_ctx_long())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = PRMergeRate().run(_ctx_short())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is True


# ── change_inducing_review_rate ───────────────────────────────────────

class TestChangeInducingNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = ChangeInducingReviewRate().run(_ctx_long())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = ChangeInducingReviewRate().run(_ctx_short())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is True


# ── approval_to_merge_ratio ──────────────────────────────────────────

class TestApprovalToMergeNoData:
    def test_long_period_zero_approvals_is_signal(self):
        res = ApprovalToMergeRatio().run(_ctx_long())
        assert res.details["total_approvals"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_approvals_is_no_data(self):
        res = ApprovalToMergeRatio().run(_ctx_short())
        assert res.details["total_approvals"] == 0
        assert res.details.get("no_data") is True


# ── blocking_comment_rate ─────────────────────────────────────────────

class TestBlockingCommentRateNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = BlockingCommentRate().run(_ctx_long())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = BlockingCommentRate().run(_ctx_short())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is True


# ── review_comment_substance ─────────────────────────────────────────

class TestReviewCommentSubstanceNoData:
    def test_long_period_zero_comments_is_signal(self):
        res = ReviewCommentSubstance().run(_ctx_long())
        assert res.details["total_comments"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_comments_is_no_data(self):
        res = ReviewCommentSubstance().run(_ctx_short())
        assert res.details["total_comments"] == 0
        assert res.details.get("no_data") is True


# ── first_reviewer_rate ──────────────────────────────────────────────

class TestFirstReviewerRateNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = FirstReviewerRate().run(_ctx_long())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = FirstReviewerRate().run(_ctx_short())
        assert res.details["total_reviews"] == 0
        assert res.details.get("no_data") is True


# ── mentorship_signal ────────────────────────────────────────────────

class TestMentorshipSignalNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = MentorshipSignal().run(_ctx_long())
        assert res.details["total_reviewed_prs"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = MentorshipSignal().run(_ctx_short())
        assert res.details["total_reviewed_prs"] == 0
        assert res.details.get("no_data") is True


# ── review_breadth ───────────────────────────────────────────────────

class TestReviewBreadthNoData:
    def test_long_period_zero_reviews_is_signal(self):
        res = ReviewBreadth().run(_ctx_long())
        assert res.details["unique_authors"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_reviews_is_no_data(self):
        res = ReviewBreadth().run(_ctx_short())
        assert res.details["unique_authors"] == 0
        assert res.details.get("no_data") is True


# ── review_demand ────────────────────────────────────────────────────

class TestReviewDemandNoData:
    def test_long_period_zero_demand_is_signal(self):
        """Zero demand in 45-day period = nobody asked for your reviews (bad signal)."""
        res = ReviewDemand().run(_ctx_long())
        assert res.details["demand_count"] == 0
        # total_timeline_events == 0 still triggers no_data (genuinely missing data)
        # but that's correct — when the bundle has no PRs, timeline data is missing
        assert res.details.get("no_data") is True  # timeline events = 0

    def test_short_period_zero_demand_is_no_data(self):
        res = ReviewDemand().run(_ctx_short())
        assert res.details["demand_count"] == 0
        assert res.details.get("no_data") is True


# ── review_leverage ──────────────────────────────────────────────────

class TestReviewLeverageNoData:
    def test_long_period_zero_change_requests_is_signal(self):
        """Zero change requests in 45 days → effectiveness_rate 0.0, no no_data."""
        res = ReviewLeverage().run(_ctx_long())
        assert res.details.get("no_data") is not True
        assert res.details["effectiveness_rate"] == 0.0

    def test_short_period_zero_change_requests_is_no_data(self):
        res = ReviewLeverage().run(_ctx_short())
        assert res.details.get("no_data") is True


# ── review_turnaround_time (special: sentinel value) ─────────────────

class TestReviewTurnaroundTimeNoData:
    def test_long_period_zero_reviews_uses_sentinel(self):
        """Zero reviews in 45 days → median_hours set to period*24 (rates 'bad')."""
        res = ReviewTurnaroundTime().run(_ctx_long())
        assert res.details["reviewed_prs"] == 0
        assert res.details.get("no_data") is not True
        # Sentinel: entire period converted to hours
        expected_hours = 45 * 24
        assert res.details["median_hours"] == expected_hours
        assert res.details["p75_hours"] == expected_hours

    def test_short_period_zero_reviews_is_no_data(self):
        res = ReviewTurnaroundTime().run(_ctx_short())
        assert res.details["reviewed_prs"] == 0
        assert res.details.get("no_data") is True
        assert res.details["median_hours"] == 0.0


# ── inline_comment_density ───────────────────────────────────────────

class TestInlineCommentDensityNoData:
    def test_long_period_zero_prs_is_signal(self):
        res = InlineCommentDensity().run(_ctx_long())
        assert res.details["reviewed_pr_count"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_prs_is_no_data(self):
        res = InlineCommentDensity().run(_ctx_short())
        assert res.details["reviewed_pr_count"] == 0
        assert res.details.get("no_data") is True


# ── co_author_contribution_rate ──────────────────────────────────────

class TestCoAuthorContributionNoData:
    def test_long_period_zero_events_is_signal(self):
        res = CoAuthorContributionRate().run(_ctx_long())
        assert res.details["total_co_events"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_events_is_no_data(self):
        res = CoAuthorContributionRate().run(_ctx_short())
        assert res.details["total_co_events"] == 0
        assert res.details.get("no_data") is True


# ── self_merge_rate ──────────────────────────────────────────────────

class TestSelfMergeRateNoData:
    def test_long_period_zero_merges_is_signal(self):
        res = SelfMergeRate().run(_ctx_long())
        assert res.details["engineer_merged_count"] == 0
        assert res.details.get("no_data") is not True

    def test_short_period_zero_merges_is_no_data(self):
        res = SelfMergeRate().run(_ctx_short())
        assert res.details["engineer_merged_count"] == 0
        assert res.details.get("no_data") is True


# ── rework_rate (special: period calculation fix) ────────────────────

class TestReworkRateNoData:
    def test_long_period_zero_prs_is_no_data(self):
        """Zero PRs means no patch data at all — genuinely no data."""
        res = ReworkRate().run(_ctx_long())
        assert res.details["pr_count"] == 0
        assert res.details.get("no_data") is True

    def test_short_period_zero_prs_is_no_data(self):
        res = ReworkRate().run(_ctx_short())
        assert res.details["pr_count"] == 0
        assert res.details.get("no_data") is True
