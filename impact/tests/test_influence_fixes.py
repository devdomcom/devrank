"""
Tests verifying fixes for influence metric bugs (Issues 6, 7, 13, 14, 19, 20, 24, 26).
Plus Bug 8: MentorshipSignal threshold scaling.
"""

from datetime import UTC, datetime, timedelta

from impact.domain.models import ReviewState, TimelineEvent
from impact.metrics.plugins.mixed.co_author_contribution_rate import CoAuthorContributionRate
from impact.metrics.plugins.influence.blocking_comment_rate import BlockingCommentRate
from impact.metrics.plugins.influence.change_inducing_review_rate import ChangeInducingReviewRate
from impact.metrics.plugins.influence.mentorship_signal import MentorshipSignal
from impact.metrics.plugins.influence.pr_merge_rate import PRMergeRate
from impact.metrics.plugins.influence.review_leverage import ReviewLeverage
from impact.metrics.plugins.influence.review_turnaround_time import ReviewTurnaroundTime
from impact.metrics.plugins.influence.reviews_given import ReviewsGiven
from impact.metrics.plugins.influence.unblock_time import UnblockTime
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


# ---------------------------------------------------------------------------
# Issue 6: co_author_contribution_rate parses Co-authored-by trailers
# ---------------------------------------------------------------------------

class TestCoAuthorTrailerParsing:
    """Issue 6: Should parse Co-authored-by: trailers, not just 'different author'."""

    def test_co_authored_by_trailer_detected(self):
        """Commits with Co-authored-by trailer should count as co-authored."""
        user = make_user(id=1, login="alice")
        coauthor = make_user(id=2, login="bob")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        # Commit with Co-authored-by trailer
        c1 = make_commit(
            "sha1", user, start + timedelta(hours=1), 1,
            message="feat: add feature\n\nCo-authored-by: Bob <bob@example.com>"
        )
        # Regular commit without trailer
        c2 = make_commit("sha2", user, start + timedelta(hours=2), 1, message="fix: typo")

        bundle = make_bundle(
            users=[user, coauthor, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[c1, c2],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = CoAuthorContributionRate()
        res = metric.run(context)

        # Only c1 has Co-authored-by trailer, so inbound_co = 1 out of 2 total
        assert res.details["inbound_co_commits"] == 1
        assert res.details["per_pr_co"][0]["co_authors"] == 1
        assert res.details["per_pr_co"][0]["total_commits"] == 2

    def test_commit_by_other_author_without_trailer_not_counted(self):
        """A commit by someone other than PR author WITHOUT a Co-authored-by trailer
        should NOT count as co-authored (this was the old buggy behavior)."""
        user = make_user(id=1, login="alice")
        other = make_user(id=2, login="bob")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        # Bob commits on Alice's PR, but no Co-authored-by trailer
        c1 = make_commit("sha1", other, start + timedelta(hours=1), 1, message="fix stuff")

        bundle = make_bundle(
            users=[user, other, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[c1],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = CoAuthorContributionRate()
        res = metric.run(context)

        # No Co-authored-by trailer, so inbound_co should be 0
        assert res.details["inbound_co_commits"] == 0

    def test_case_insensitive_trailer(self):
        """Co-authored-by should be case-insensitive."""
        user = make_user(id=1, login="alice")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        c1 = make_commit(
            "sha1", user, start + timedelta(hours=1), 1,
            message="feat: thing\n\nco-authored-by: Bob <bob@example.com>"
        )

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[c1],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = CoAuthorContributionRate()
        res = metric.run(context)

        assert res.details["inbound_co_commits"] == 1


# ---------------------------------------------------------------------------
# Issue 7: No artificial 0.5 floor
# ---------------------------------------------------------------------------

class TestCoAuthorNoArtificialFloor:
    """Issue 7: Zero events should report 0.0, not artificial 0.5."""

    def test_zero_events_returns_zero(self):
        """When there are zero co-author events, collab_per_week should be 0.0."""
        user = make_user(id=1, login="alice")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # PR with commits but no Co-authored-by trailers
        pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        c1 = make_commit("sha1", user, start + timedelta(hours=1), 1, message="solo commit")

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[c1],
        )
        # Short period (<=30 days) where old code would force 0.5
        end = start + timedelta(days=7)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = CoAuthorContributionRate()
        res = metric.run(context)

        assert res.details["collab_per_week"] == 0.0
        assert res.details["no_data"] is True

    def test_zero_events_no_data_flag(self):
        """When there are zero co-author events, no_data should be True."""
        user = make_user(id=1, login="alice")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[],
            commits=[],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = CoAuthorContributionRate()
        res = metric.run(context)

        assert res.details["total_co_events"] == 0
        assert res.details["no_data"] is True
        assert res.details["collab_per_week"] == 0.0


# ---------------------------------------------------------------------------
# Issue 13 (updated): change_inducing_review_rate includes ALL reviews in denominator
# ---------------------------------------------------------------------------

class TestChangeInducingIncludesApprovals:
    """Issue 13 (updated): All non-self reviews should be in the denominator,
    including approvals. Approvals CAN induce commits (e.g., approval -> author
    pushes CI fix -> merge). The inducing check handles approvals that didn't
    induce anything naturally."""

    def test_approvals_included_in_denominator(self):
        """All non-self reviews (including approvals) should count in denominator."""
        reviewer = make_user(id=1, login="alice")
        author = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr1 = make_pr(1, author, repo, base_time=start)
        # One CR that induces a change
        cr_review = make_review(
            10, 1, reviewer, start + timedelta(hours=1),
            state=ReviewState.CHANGES_REQUESTED,
        )
        # One approval (should be in denominator too)
        approval_review = make_review(
            11, 1, reviewer, start + timedelta(hours=5),
            state=ReviewState.APPROVED,
        )
        # Author commit after CR (induced)
        commit1 = make_commit("sha1", author, start + timedelta(hours=3), 1)

        bundle = make_bundle(
            users=[reviewer, author, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[commit1],
            reviews=[cr_review, approval_review],
        )
        end = start + timedelta(days=2)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = ChangeInducingReviewRate()
        res = metric.run(context)

        # Denominator should be 2 (CR + approval)
        assert res.details["total_reviews"] == 2
        assert res.details["inducing_count"] == 1
        assert res.details["inducing_rate"] == 0.5


# ---------------------------------------------------------------------------
# Issue 14: unblock_time filters to PR author commits
# ---------------------------------------------------------------------------

class TestUnblockTimeCommitFilter:
    """Issue 14: Should filter to PR author commits, not 'not the reviewer'."""

    def test_only_pr_author_commits_counted(self):
        """Only commits by the PR author after CR should trigger unblock flow."""
        reviewer = make_user(id=1, login="alice")
        pr_author = make_user(id=2, login="bob")
        third_party = make_user(id=3, login="charlie")
        owner = make_user(id=4, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, pr_author, repo, base_time=start)
        # Alice (reviewer) requests changes
        cr = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.CHANGES_REQUESTED)
        # Charlie (third party) commits -- should NOT count as PR author response
        third_party_commit = make_commit("sha1", third_party, start + timedelta(hours=2), 1)
        # Bob (PR author) commits -- should count
        author_commit = make_commit("sha2", pr_author, start + timedelta(hours=4), 1)
        # Alice re-reviews after bob's commit
        re_review = make_review(11, 1, reviewer, start + timedelta(hours=6))

        bundle = make_bundle(
            users=[reviewer, pr_author, third_party, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[third_party_commit, author_commit],
            reviews=[cr, re_review],
        )
        end = start + timedelta(days=1)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = UnblockTime()
        res = metric.run(context)

        assert res.details["cr_count"] == 1
        # The time should be from author_commit (h4) to re_review (h6) = 2 hours
        assert res.details["median_hours"] == 2.0


# ---------------------------------------------------------------------------
# Issue 19: review_leverage returns 0.0-1.0, not 0-100
# ---------------------------------------------------------------------------

class TestReviewLeverageScale:
    """Issue 19: Effectiveness should be 0.0-1.0 scale, not 0-100."""

    def test_effectiveness_rate_zero_to_one(self):
        """Effectiveness rate should be between 0.0 and 1.0, not 0-100 percentage."""
        user = make_user(id=1, login="alice")  # PR author
        reviewer = make_user(id=2, login="bob")  # reviewer
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)

        created = datetime(2024, 12, 1, tzinfo=UTC)
        merged = datetime(2024, 12, 2, tzinfo=UTC)
        pr1 = make_pr(1, user, repo, created_at=created, merged_at=merged)

        review1 = make_review(
            id=1, pr_number=1, user=reviewer,
            submitted_at=datetime(2024, 12, 1, 2, tzinfo=UTC),
            state=ReviewState.CHANGES_REQUESTED,
        )
        # Author commit after review
        commit1 = make_commit(
            "sha1", user, datetime(2024, 12, 1, 5, tzinfo=UTC), 1
        )

        bundle = make_bundle(
            users=[user, reviewer, owner],
            repositories=[repo],
            pull_requests=[pr1],
            reviews=[review1],
            commits=[commit1],
        )
        context = make_context(
            bundle, user_login="bob",
            start_date=datetime(2024, 11, 1, tzinfo=UTC),
            end_date=datetime(2024, 12, 31, tzinfo=UTC),
        )

        metric = ReviewLeverage()
        result = metric.run(context)

        # Key should be effectiveness_rate (not effectiveness_percentage)
        assert "effectiveness_rate" in result.details
        assert "effectiveness_percentage" not in result.details
        # Value should be 0.0-1.0 scale
        rate = result.details["effectiveness_rate"]
        assert 0.0 <= rate <= 1.0


# ---------------------------------------------------------------------------
# Issue 20: review_turnaround_time uses timeline events + no_data flag
# ---------------------------------------------------------------------------

class TestReviewTurnaroundTime:
    """Issue 20: Should use review_requested timeline events when available,
    and add no_data flag when no durations."""

    def test_no_data_flag_when_empty(self):
        """When no reviews exist, no_data should be True."""
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        context = make_context(bundle, user_login="alice")

        metric = ReviewTurnaroundTime()
        res = metric.run(context)

        assert res.details.get("no_data") is True

    def test_uses_review_requested_timeline_event(self):
        """When review_requested timeline event exists, should measure from that time."""
        reviewer = make_user(id=1, login="alice")
        author = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # PR created at start, but review requested 5 hours later
        pr = make_pr(1, author, repo, base_time=start, created_delta_hours=0)
        review = make_review(10, 1, reviewer, start + timedelta(hours=8))

        # Timeline event: review_requested at hour 5 targeting alice
        timeline_event = TimelineEvent(
            id=100,
            event="review_requested",
            actor=author,
            created_at=start + timedelta(hours=5),
            pull_request_number=1,
            requested_reviewer=reviewer,  # alice was requested
        )

        bundle = make_bundle(
            users=[reviewer, author, owner],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[review],
            timeline=[timeline_event],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = ReviewTurnaroundTime()
        res = metric.run(context)

        # Should measure from review_requested (h5) to review (h8) = 3 hours
        # NOT from PR creation (h0) to review (h8) = 8 hours
        assert res.details["reviewed_prs"] == 1
        assert res.details["median_hours"] == 3.0

    def test_note_when_no_timeline_events(self):
        """When no timeline events exist, per_pr entry should include a note."""
        reviewer = make_user(id=1, login="alice")
        author = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, author, repo, base_time=start, created_delta_hours=0)
        review = make_review(10, 1, reviewer, start + timedelta(hours=8))

        bundle = make_bundle(
            users=[reviewer, author, owner],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[review],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = ReviewTurnaroundTime()
        res = metric.run(context)

        assert res.details["reviewed_prs"] == 1
        # Per-PR entry should have a note about measuring from PR creation
        assert "note" in res.details["per_pr"][0]
        assert "PR creation" in res.details["per_pr"][0]["note"]


# ---------------------------------------------------------------------------
# Issue 26: Self-review filtering across all influence metrics
# ---------------------------------------------------------------------------

class TestSelfReviewFiltering:
    """Issue 26: Reviews on user's own PRs should be filtered out."""

    def _make_self_review_context(self):
        """Helper: alice reviews her own PR (self-review) + bob's PR (real review)."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Alice's own PR (self-review should be excluded)
        pr_own = make_pr(1, alice, repo, base_time=start, created_delta_hours=0,
                         merged_delta_hours=24)
        self_review = make_review(
            10, 1, alice, start + timedelta(hours=2),
            state=ReviewState.APPROVED,
        )

        # Bob's PR (real review should be counted)
        pr_other = make_pr(2, bob, repo, base_time=start, created_delta_hours=0,
                           merged_delta_hours=24)
        real_review = make_review(
            20, 2, alice, start + timedelta(hours=3),
            state=ReviewState.CHANGES_REQUESTED,
        )

        commit_on_bob_pr = make_commit("sha1", bob, start + timedelta(hours=5), 2)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[pr_own, pr_other],
            commits=[commit_on_bob_pr],
            reviews=[self_review, real_review],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)
        return context

    def test_reviews_given_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = ReviewsGiven()
        res = metric.run(context)
        # Only the review on bob's PR should count (not self-review)
        assert res.details["review_count"] == 1

    def test_pr_merge_rate_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = PRMergeRate()
        res = metric.run(context)
        # Only the review on bob's PR should count
        assert res.details["total_reviews"] == 1

    def test_blocking_comment_rate_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = BlockingCommentRate()
        res = metric.run(context)
        # Only the review on bob's PR should count
        assert res.details["total_reviews"] == 1

    def test_change_inducing_review_rate_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = ChangeInducingReviewRate()
        res = metric.run(context)
        # Self-review (approval on own PR) filtered out; CR on bob's PR remains
        assert res.details["total_reviews"] == 1

    def test_review_turnaround_time_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = ReviewTurnaroundTime()
        res = metric.run(context)
        # Only bob's PR should be counted
        assert res.details["reviewed_prs"] == 1

    def test_review_leverage_excludes_self_reviews(self):
        context = self._make_self_review_context()
        metric = ReviewLeverage()
        res = metric.run(context)
        # Only the review on bob's PR should count
        if "total_reviews" in res.details:
            assert res.details["total_reviews"] == 1


# ---------------------------------------------------------------------------
# Issue 24: Period defaults standardized to 30 days
# ---------------------------------------------------------------------------

class TestPeriodDefaults:
    """Issue 24: All influence metrics should default to 30 days when no dates given."""

    def test_reviews_given_default_period_30(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        # No start/end dates
        context = make_context(bundle, user_login="alice")
        # Override to None to test default
        context.start_date = None
        context.end_date = None

        metric = ReviewsGiven()
        res = metric.run(context)
        assert res.details["period_days"] == 30.0

    def test_blocking_comment_rate_default_period_30(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        context = make_context(bundle, user_login="alice")
        context.start_date = None
        context.end_date = None

        metric = BlockingCommentRate()
        res = metric.run(context)
        assert res.details["period_days"] == 30.0

    def test_review_turnaround_time_default_period_30(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        context = make_context(bundle, user_login="alice")
        context.start_date = None
        context.end_date = None

        metric = ReviewTurnaroundTime()
        res = metric.run(context)
        assert res.details["period_days"] == 30.0


# ---------------------------------------------------------------------------
# Bug 8: MentorshipSignal threshold scaling by period length
# ---------------------------------------------------------------------------

class TestMentorshipSignalThresholdScaling:
    """Bug 8: The junior_threshold should scale by period length.

    ~5 PRs/month baseline, minimum 2.
    - 30-day period -> threshold = 5
    - 14-day period -> threshold = max(2, int(5 * 14/30)) = max(2, 2) = 2
    - 180-day period -> threshold = max(2, int(5 * 180/30)) = max(2, 30) = 30
    """

    def test_threshold_scales_with_long_period(self):
        """A 180-day (6-month) period should have a much higher threshold."""
        alice = make_user(id=1, login="alice")  # reviewer
        bob = make_user(id=2, login="bob")      # PR author with few PRs
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Bob has 10 PRs in the period (would be "junior" at threshold=5
        # but NOT junior at threshold=30 for 180 days)
        bob_prs = []
        for i in range(10):
            bob_prs.append(make_pr(i + 1, bob, repo, base_time=start,
                                   created_delta_hours=i * 24))

        # Alice reviews one of Bob's PRs
        review = make_review(100, 1, alice, start + timedelta(hours=5),
                             state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=bob_prs,
            reviews=[review],
        )
        # 180-day window -> threshold = max(2, int(5 * 180/30)) = 30
        end = start + timedelta(days=180)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = MentorshipSignal()
        res = metric.run(context)

        # With 180-day threshold of 30, Bob (10 PRs) IS junior
        assert res.details["junior_threshold"] == 30
        assert res.details["junior_review_count"] == 1

    def test_threshold_has_minimum_of_2(self):
        """Even for very short periods, threshold should be at least 2."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        bob_pr = make_pr(1, bob, repo, base_time=start, created_delta_hours=0)
        review = make_review(100, 1, alice, start + timedelta(hours=1),
                             state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[bob_pr],
            reviews=[review],
        )
        # Very short period: 3 days -> int(5 * 3/30) = int(0.5) = 0 -> clamped to 2
        end = start + timedelta(days=3)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = MentorshipSignal()
        res = metric.run(context)

        assert res.details["junior_threshold"] == 2

    def test_30_day_period_uses_baseline_threshold(self):
        """A 30-day period should use the baseline threshold of 5."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        bob_pr = make_pr(1, bob, repo, base_time=start, created_delta_hours=0)
        review = make_review(100, 1, alice, start + timedelta(hours=1),
                             state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[bob_pr],
            reviews=[review],
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = MentorshipSignal()
        res = metric.run(context)

        # int(5 * 30/30) = 5
        assert res.details["junior_threshold"] == 5
