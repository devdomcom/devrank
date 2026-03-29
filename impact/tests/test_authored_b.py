"""
Tests for the 6 authored metric bug fixes:
1. inline_comment_density: counts comments given (not received)
2. pr_throughput: merged ratio uses same population as opened
3. review_quality: SlowReviewResponse uses is_change_request; no_data flags
4. bug_fix_focus_rate: no double-counting of PR commits
5. revert_introduction_rate: attributes to original author
6. conventional_commit_rate: new slug name

Plus tests for additional bug fixes:
7. follow_up_commit_rate: merge commits filtered out
8. off_hours_activity_rate: returns no_data when timezone missing
9. revert_introduction_rate: ambiguous SHA prefix matching
"""

from datetime import timedelta

import pytest

from impact.domain.models import CommentType, ReviewState
from impact.metrics.plugins.authored.bug_fix_focus_rate import BugFixFocusRate
from impact.metrics.plugins.authored.conventional_commit_rate import ConventionalCommitRate
from impact.metrics.plugins.authored.follow_up_commit_rate import FollowUpCommitRate
from impact.metrics.plugins.influence.inline_comment_density import InlineCommentDensity
from impact.metrics.plugins.authored.off_hours_activity_rate import OffHoursActivityRate
from impact.metrics.plugins.authored.pr_throughput import PRThroughput
from impact.metrics.plugins.authored.revert_introduction_rate import RevertIntroductionRate
from impact.metrics.plugins.authored.review_quality import (
    ReviewIterations,
    SlowReviewResponse,
    TimeToFirstReview,
)
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


# ---------------------------------------------------------------------------
# 1. inline_comment_density: counts comments GIVEN on others' PRs
# ---------------------------------------------------------------------------

class TestInlineCommentDensityGiven:
    def test_counts_comments_given_not_received(self):
        """
        Alice reviews Bob's PRs and leaves inline comments.
        The metric should count Alice's inline comments on Bob's PRs,
        NOT comments others leave on Alice's PRs.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Bob's PR that Alice reviews
        bob_pr = make_pr(10, bob, repo, base_time=start, created_delta_hours=0)
        # Alice reviews Bob's PR
        alice_review = make_review(
            201, 10, alice, start + timedelta(hours=1), ReviewState.COMMENTED
        )
        # Alice leaves 2 inline comments on Bob's PR
        alice_inline1 = make_comment(
            301, 10, alice, start + timedelta(hours=2),
            type=CommentType.REVIEW, review_id=201, position=5, path="file.py",
        )
        alice_inline2 = make_comment(
            302, 10, alice, start + timedelta(hours=3),
            type=CommentType.REVIEW, review_id=201, position=10, path="file.py",
        )

        # Alice's own PR that Bob reviews with 3 inline comments
        # These should NOT be counted for Alice's metric
        alice_pr = make_pr(20, alice, repo, base_time=start, created_delta_hours=5)
        bob_review = make_review(
            202, 20, bob, start + timedelta(hours=6), ReviewState.COMMENTED
        )
        bob_inline1 = make_comment(
            303, 20, bob, start + timedelta(hours=7),
            type=CommentType.REVIEW, review_id=202, position=1, path="a.py",
        )
        bob_inline2 = make_comment(
            304, 20, bob, start + timedelta(hours=8),
            type=CommentType.REVIEW, review_id=202, position=2, path="a.py",
        )
        bob_inline3 = make_comment(
            305, 20, bob, start + timedelta(hours=9),
            type=CommentType.REVIEW, review_id=202, position=3, path="a.py",
        )

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[bob_pr, alice_pr],
            reviews=[alice_review, bob_review],
            comments=[alice_inline1, alice_inline2, bob_inline1, bob_inline2, bob_inline3],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = InlineCommentDensity()
        res = metric.run(context)

        assert res.metric_slug == "inline_comment_density"
        # Alice reviewed 1 PR (Bob's), gave 2 inline comments
        assert res.details["reviewed_pr_count"] == 1
        assert res.details["total_inline_comments"] == 2
        assert res.details["avg_inline_per_pr"] == 2.0

    def test_excludes_self_reviews(self):
        """Self-reviews (user reviewing their own PR) should be excluded."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Alice's own PR that she also reviews (self-review)
        alice_pr = make_pr(10, alice, repo, base_time=start)
        self_review = make_review(201, 10, alice, start + timedelta(hours=1))
        self_comment = make_comment(
            301, 10, alice, start + timedelta(hours=2),
            type=CommentType.REVIEW, review_id=201, position=5, path="file.py",
        )

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[alice_pr],
            reviews=[self_review],
            comments=[self_comment],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = InlineCommentDensity()
        res = metric.run(context)

        assert res.details["reviewed_pr_count"] == 0
        assert res.details["total_inline_comments"] == 0


# ---------------------------------------------------------------------------
# 2. pr_throughput: merged ratio only counts PRs created in the window
# ---------------------------------------------------------------------------

class TestPRThroughputSamePopulation:
    def test_merged_ratio_uses_same_population(self):
        """
        PRs merged that were created BEFORE the window should NOT inflate the
        merged count. Only PRs created in the window that were also merged count.
        """
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # PR created in window, merged in window
        pr1 = make_pr(
            1, alice, repo, base_time=start,
            created_delta_hours=0, merged_delta_hours=48,
        )
        # PR created in window, not merged
        pr2 = make_pr(
            2, alice, repo, base_time=start,
            created_delta_hours=24, merged_delta_hours=None,
        )
        # PR created BEFORE window, merged IN window (backlog clear)
        # This should NOT count as merged because it wasn't created in window
        pr3 = make_pr(
            3, alice, repo, base_time=start,
            created_delta_hours=-240, merged_delta_hours=24,
        )

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr1, pr2, pr3],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = PRThroughput()
        res = metric.run(context)

        # Only pr1 and pr2 were created in the window
        assert res.details["opened_count"] == 2
        # Only pr1 was created in window AND merged
        assert res.details["merged_count"] == 1
        # Ratio: 1/2 = 0.5
        assert res.details["merge_ratio"] == 0.5
        assert res.details["merged_pr_numbers"] == [1]

    def test_merge_ratio_cannot_exceed_one(self):
        """With the fix, merge_ratio should never exceed 1.0."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # 1 PR created in window, merged
        pr1 = make_pr(
            1, alice, repo, base_time=start,
            created_delta_hours=0, merged_delta_hours=12,
        )

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr1],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = PRThroughput()
        res = metric.run(context)

        assert res.details["merge_ratio"] <= 1.0


# ---------------------------------------------------------------------------
# 3. review_quality: SlowReviewResponse uses is_change_request; no_data flags
# ---------------------------------------------------------------------------

class TestReviewQualityConsistency:
    def test_slow_review_response_uses_is_change_request(self):
        """
        SlowReviewResponse should treat a 'commented' review that has inline
        comments as a change request (matching is_change_request behavior),
        not just reviews with state == CHANGES_REQUESTED.
        """
        author = make_user(id=1, login="alice")
        reviewer = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=10, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(
            1, author, repo, created_at=start,
            merged_at=start + timedelta(hours=24),
        )

        # A "commented" review with inline comments (is_change_request=True)
        commented_review = make_review(
            101, 1, reviewer, start + timedelta(hours=2),
            ReviewState.COMMENTED, body=None,
        )
        inline_comment = make_comment(
            201, 1, reviewer, start + timedelta(hours=2),
            type=CommentType.REVIEW, review_id=101, position=5, path="file.py",
        )

        # Author commit after the review
        fix_commit = make_commit(
            sha="fix1", author=author,
            date=start + timedelta(hours=5),
            pr_number=1, message="address feedback",
        )

        bundle = make_bundle(
            users=[author, reviewer, owner],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[commented_review],
            commits=[fix_commit],
            comments=[inline_comment],
        )
        ctx = make_context(
            bundle, user_login="alice",
            start_date=start, end_date=start + timedelta(days=5),
        )

        metric = SlowReviewResponse()
        res = metric.run(ctx)

        # The "commented" review with inline comments should be treated as a CR
        assert res.details["samples"] == 1
        assert res.details["per_review"][0]["pr"] == 1
        # Response time: 5h - 2h = 3h
        assert abs(res.details["median_hours"] - 3.0) < 1e-6

    def test_no_data_flags_review_iterations(self):
        """ReviewIterations should set no_data=True when no merged PRs."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # No PRs at all
        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
        )
        ctx = make_context(
            bundle, user_login="alice",
            start_date=start, end_date=start + timedelta(days=5),
        )

        res = ReviewIterations().run(ctx)
        assert res.details.get("no_data") is True

    def test_no_data_flags_time_to_first_review(self):
        """TimeToFirstReview should set no_data=True when no reviews."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # PR with no reviews
        pr = make_pr(1, alice, repo, base_time=start)
        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
        )
        ctx = make_context(
            bundle, user_login="alice",
            start_date=start, end_date=start + timedelta(days=5),
        )

        res = TimeToFirstReview().run(ctx)
        assert res.details.get("no_data") is True

    def test_no_data_flags_slow_review_response(self):
        """SlowReviewResponse should set no_data=True when no response data."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # No PRs
        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
        )
        ctx = make_context(
            bundle, user_login="alice",
            start_date=start, end_date=start + timedelta(days=5),
        )

        res = SlowReviewResponse().run(ctx)
        assert res.details.get("no_data") is True


# ---------------------------------------------------------------------------
# 4. bug_fix_focus_rate: no double-counting
# ---------------------------------------------------------------------------

class TestBugFixFocusRateDedup:
    def test_bug_pr_with_bug_commits_counts_as_one(self):
        """
        A bug-fix PR with 3 bug-fix commits should count as 1 fix total,
        not 4 (1 PR + 3 commits).
        """
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Bug-fix PR (title has "fix:")
        bug_pr = make_pr(
            1, alice, repo, base_time=start, created_delta_hours=0,
        )
        bug_pr.title = "fix: resolve crash on login"

        # 3 bug-fix commits inside the PR
        c1 = make_commit("sha1", alice, start + timedelta(hours=1), 1, message="fix: part 1")
        c2 = make_commit("sha2", alice, start + timedelta(hours=2), 1, message="fix: part 2")
        c3 = make_commit("sha3", alice, start + timedelta(hours=3), 1, message="fix: part 3")

        # A non-bug PR for denominator
        normal_pr = make_pr(
            2, alice, repo, base_time=start, created_delta_hours=10,
        )
        normal_pr.title = "feat: add dashboard"
        c4 = make_commit("sha4", alice, start + timedelta(hours=11), 2, message="feat: dashboard")

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[bug_pr, normal_pr],
            commits=[c1, c2, c3, c4],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = BugFixFocusRate()
        res = metric.run(context)

        # 1 bug PR out of 2 PRs. All commits belong to PRs, so 0 standalone.
        # total_bug = 1 (bug PR) + 0 (standalone bug commits)
        assert res.details["bug_pr_count"] == 1
        assert res.details["total_bug"] == 1
        # total_items = 2 PRs + 0 standalone commits = 2
        assert res.details["total_items"] == 2
        assert res.details["overall_rate"] == pytest.approx(50.0)

    def test_standalone_bug_commits_still_counted(self):
        """Bug commits NOT part of any PR should still be counted."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # A normal PR
        pr = make_pr(1, alice, repo, base_time=start)
        pr.title = "feat: new feature"
        pr_commit = make_commit("sha1", alice, start + timedelta(hours=1), 1, message="feat: impl")

        # Standalone bug commit (no PR)
        standalone = make_commit(
            "sha_standalone", alice, start + timedelta(hours=5),
            pr_number=0,  # not linked to any PR
            message="fix: hotfix for production issue",
        )

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[pr_commit, standalone],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = BugFixFocusRate()
        res = metric.run(context)

        # 1 PR (not bug) + 1 standalone commit (bug) = 2 items, 1 bug
        assert res.details["bug_pr_count"] == 0
        assert res.details["bug_standalone_commit_count"] == 1
        assert res.details["total_bug"] == 1
        assert res.details["total_items"] == 2
        assert res.details["overall_rate"] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# 5. revert_introduction_rate: attributes to original commit author
# ---------------------------------------------------------------------------

class TestRevertIntroductionRateAttribution:
    def test_attributes_revert_to_original_author(self):
        """
        When Bob reverts Alice's commit, the revert should be attributed
        to Alice (the original author), not Bob (the reverter).
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Alice's original commit
        alice_commit = make_commit(
            "abc123def456", alice, start + timedelta(hours=1), 1,
            message="feat: add login page",
        )

        # Bob reverts Alice's commit
        bob_revert = make_commit(
            "rev999", bob, start + timedelta(hours=5), 1,
            message='Revert "feat: add login page"\n\nThis reverts commit abc123def456.',
        )

        # Alice has another normal commit
        alice_commit2 = make_commit(
            "def456", alice, start + timedelta(hours=2), 1,
            message="docs: update readme",
        )

        pr = make_pr(1, alice, repo, base_time=start)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[alice_commit, alice_commit2, bob_revert],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = RevertIntroductionRate()
        res = metric.run(context)

        # Alice has 2 commits, 1 was reverted
        assert res.details["total_commits"] == 2
        assert res.details["revert_count"] == 1
        assert res.details["rate"] == pytest.approx(50.0)

    def test_reverter_not_penalized(self):
        """
        Bob performs the revert of Alice's code. When measured for Bob,
        the revert should NOT count against Bob.
        """
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        alice_commit = make_commit(
            "abc123def456", alice, start + timedelta(hours=1), 1,
            message="feat: add login page",
        )
        bob_revert = make_commit(
            "rev999", bob, start + timedelta(hours=5), 1,
            message='Revert "feat: add login page"\n\nThis reverts commit abc123def456.',
        )
        bob_normal = make_commit(
            "bob001", bob, start + timedelta(hours=3), 1,
            message="feat: add signup",
        )

        pr = make_pr(1, alice, repo, base_time=start)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[alice_commit, bob_revert, bob_normal],
        )
        end = start + timedelta(days=10)
        # Measure for BOB
        context = make_context(bundle, user_login="bob", start_date=start, end_date=end)

        metric = RevertIntroductionRate()
        res = metric.run(context)

        # Bob has 2 commits (revert + normal), but the revert was of Alice's code
        assert res.details["total_commits"] == 2
        assert res.details["revert_count"] == 0
        assert res.details["rate"] == 0.0

    def test_no_data_flag_when_no_commits(self):
        """no_data should be set when the user has zero commits."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = RevertIntroductionRate()
        res = metric.run(context)

        assert res.details.get("no_data") is True


# ---------------------------------------------------------------------------
# 6. conventional_commit_rate: new slug name
# ---------------------------------------------------------------------------

class TestConventionalCommitRate:
    def test_slug_is_conventional_commit_rate(self):
        """The slug should be 'conventional_commit_rate', not 'commit_message_clarity'."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        c1 = make_commit("sha1", alice, start, 1, message="feat: add new feature")
        c2 = make_commit("sha2", alice, start + timedelta(hours=1), 1, message="fix: bug fix")
        c3 = make_commit("sha3", alice, start + timedelta(hours=2), 1, message="update code")

        pr = make_pr(1, alice, repo, base_time=start)

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2, c3],
        )
        end = start + timedelta(days=10)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = ConventionalCommitRate()
        res = metric.run(context)

        # Verify slug and class name
        assert res.metric_slug == "conventional_commit_rate"
        assert metric.name == "Conventional Commit Rate"
        assert "conventional commit format" in metric.description.lower()

        # Verify the metric still works correctly
        assert res.details["conventional_commit_rate"] == pytest.approx(66.7, abs=0.1)
        assert res.details["conventional_count"] == 2
        assert res.details["total_commits"] == 3

    def test_registered_in_get_metrics(self):
        """The new slug should be registered in get_metrics()."""
        from impact.metrics import get_metrics

        metrics = get_metrics()
        assert "conventional_commit_rate" in metrics
        assert "commit_message_clarity" not in metrics


# ---------------------------------------------------------------------------
# 7. follow_up_commit_rate: merge commits filtered out
# ---------------------------------------------------------------------------

class TestFollowUpCommitRateMergeFiltering:
    def test_merge_commits_excluded_from_count(self):
        """Bug 7: Merge commits (e.g. 'Merge branch main into feature')
        should NOT count as follow-up iterations."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0)
        # 1 real commit + 1 merge commit = only 1 real commit (no follow-up)
        c1 = make_commit("sha1", alice, start + timedelta(hours=1), 1, message="feat: initial impl")
        c2 = make_commit("sha2", alice, start + timedelta(hours=5), 1,
                         message="Merge branch 'main' into feature-1")

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2],
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = FollowUpCommitRate()
        res = metric.run(context)

        # Only 1 real commit (merge excluded) -> no follow-up
        assert res.details["per_pr"][0]["has_follow_up"] is False
        assert res.details["per_pr"][0]["author_commit_count"] == 1
        assert res.details["follow_up_rate"] == 0.0

    def test_real_follow_up_still_detected(self):
        """Real follow-up commits (non-merge) should still be counted."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0)
        c1 = make_commit("sha1", alice, start + timedelta(hours=1), 1, message="feat: initial impl")
        c2 = make_commit("sha2", alice, start + timedelta(hours=5), 1, message="fix: address review feedback")

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2],
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = FollowUpCommitRate()
        res = metric.run(context)

        assert res.details["per_pr"][0]["has_follow_up"] is True
        assert res.details["per_pr"][0]["author_commit_count"] == 2
        assert res.details["follow_up_rate"] == 100.0


# ---------------------------------------------------------------------------
# 8. off_hours_activity_rate: returns no_data when timezone missing
# ---------------------------------------------------------------------------

class TestOffHoursActivityRateTimezone:
    def test_no_timezone_returns_no_data(self):
        """Bug 9: When timezone is missing from bundle, should return no_data=True
        instead of silently defaulting to UTC."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0)
        c1 = make_commit("sha1", alice, start + timedelta(hours=1), 1)

        # No user_timezone in bundle (defaults to None)
        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1],
            user_timezone=None,
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = OffHoursActivityRate()
        res = metric.run(context)

        assert res.details["no_data"] is True
        assert res.details["user_timezone"] is None
        assert "timezone" in res.summary.lower()

    def test_with_timezone_works_normally(self):
        """When timezone IS provided, the metric should work normally."""
        alice = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        c1 = make_commit("sha1", alice, start + timedelta(hours=1), 1)

        bundle = make_bundle(
            users=[alice, owner],
            repositories=[repo],
            pull_requests=[],
            commits=[c1],
            user_timezone="America/New_York",
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = OffHoursActivityRate()
        res = metric.run(context)

        assert res.details["user_timezone"] == "America/New_York"
        assert res.details["total_activities"] >= 1
        # no_data should not be set (enough data with timezone)
        # (may still be set due to period/activity thresholds, but not due to timezone)


# ---------------------------------------------------------------------------
# 9. revert_introduction_rate: ambiguous SHA prefix matching
# ---------------------------------------------------------------------------

class TestRevertIntroductionRateAmbiguousSHA:
    def test_ambiguous_prefix_match_skipped(self):
        """Bug 11: When a SHA prefix matches multiple commits,
        the revert should be skipped (ambiguous) instead of picking first match."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Two commits that share a 7-char hex prefix "abc1234"
        c1 = make_commit("abc1234aaa111111", alice, start + timedelta(hours=1), 1, message="feat: A")
        c2 = make_commit("abc1234bbb222222", bob, start + timedelta(hours=2), 1, message="feat: B")

        # Revert references the ambiguous prefix "abc1234" (matches both c1 and c2)
        revert = make_commit(
            "eeeeeeeeee000000", bob, start + timedelta(hours=5), 1,
            message='Revert "feat: A"\n\nThis reverts commit abc1234.',
        )

        pr = make_pr(1, alice, repo, base_time=start)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2, revert],
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = RevertIntroductionRate()
        res = metric.run(context)

        # Ambiguous prefix -> revert should be skipped, not attributed to alice
        assert res.details["revert_count"] == 0
        assert res.details["rate"] == 0.0

    def test_unambiguous_prefix_match_works(self):
        """When a SHA prefix matches exactly one commit, it should work."""
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        owner = make_user(id=3, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        # Only c1 has the "abc1234" prefix
        c1 = make_commit("abc1234aaa111111", alice, start + timedelta(hours=1), 1, message="feat: A")
        c2 = make_commit("def5678bbb222222", bob, start + timedelta(hours=2), 1, message="feat: B")

        # Revert references "abc1234" which only matches c1
        revert = make_commit(
            "eeeeeeeeee000000", bob, start + timedelta(hours=5), 1,
            message='Revert "feat: A"\n\nThis reverts commit abc1234.',
        )

        pr = make_pr(1, alice, repo, base_time=start)

        bundle = make_bundle(
            users=[alice, bob, owner],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1, c2, revert],
        )
        end = start + timedelta(days=30)
        context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        metric = RevertIntroductionRate()
        res = metric.run(context)

        # Unambiguous prefix match -> revert attributed to alice
        assert res.details["revert_count"] == 1
        assert res.details["rate"] == pytest.approx(100.0)
