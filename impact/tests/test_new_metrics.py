"""Tests for the 5 new metrics: Follow-Up Commit Rate, PR Category Diversity,
Review Breadth, Review Comment Substance, Mentorship Signal.
"""
from datetime import timedelta

import pytest
from impact.metrics.plugins.authored.follow_up_commit_rate import FollowUpCommitRate
from impact.metrics.plugins.authored.pr_category_diversity import PRCategoryDiversity
from impact.metrics.plugins.influence.review_breadth import ReviewBreadth
from impact.metrics.plugins.influence.review_comment_substance import ReviewCommentSubstance
from impact.metrics.plugins.influence.mentorship_signal import MentorshipSignal
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
# Follow-Up Commit Rate
# ---------------------------------------------------------------------------

class TestFollowUpCommitRate:
    def test_with_follow_ups(self):
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        end = start + timedelta(days=7)

        # PR1: 2 commits by author (has follow-up)
        pr1 = make_pr(1, user, repo, base_time=start)
        c1a = make_commit("s1a", user, start + timedelta(hours=1), 1)
        c1b = make_commit("s1b", user, start + timedelta(hours=3), 1)

        # PR2: 1 commit by author (no follow-up)
        pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
        c2a = make_commit("s2a", user, start + timedelta(hours=25), 2)

        bundle = make_bundle(
            users=[user],
            repositories=[repo],
            pull_requests=[pr1, pr2],
            commits=[c1a, c1b, c2a],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        res = FollowUpCommitRate().run(ctx)
        assert res.metric_slug == "follow_up_commit_rate"
        assert res.details["follow_up_count"] == 1
        assert res.details["pr_count"] == 2
        assert res.details["follow_up_rate"] == pytest.approx(50.0)

    def test_no_prs(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = FollowUpCommitRate().run(ctx)
        assert res.details["no_data"] is True
        assert res.details["follow_up_rate"] == 0.0


# ---------------------------------------------------------------------------
# PR Category Diversity
# ---------------------------------------------------------------------------

class TestPRCategoryDiversity:
    def test_diverse_categories(self):
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        end = start + timedelta(days=7)

        pr1 = make_pr(1, user, repo, base_time=start)
        pr1.title = "feat: add login page"
        pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
        pr2.title = "fix: correct null check"
        pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=48)
        pr3.title = "docs: update README"

        bundle = make_bundle(
            users=[user], repositories=[repo], pull_requests=[pr1, pr2, pr3],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        res = PRCategoryDiversity().run(ctx)
        assert res.metric_slug == "pr_category_diversity"
        assert res.details["distinct_categories"] == 3
        assert "feat" in res.details["distribution"]
        assert "fix" in res.details["distribution"]
        assert "docs" in res.details["distribution"]

    def test_fallback_heuristics(self):
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START

        pr1 = make_pr(1, user, repo, base_time=start)
        pr1.title = "Add new authentication flow"  # should classify as "feat"
        pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
        pr2.title = "Fix broken login"  # should classify as "fix"

        bundle = make_bundle(
            users=[user], repositories=[repo], pull_requests=[pr1, pr2],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=3))

        res = PRCategoryDiversity().run(ctx)
        assert res.details["distinct_categories"] == 2

    def test_no_prs(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = PRCategoryDiversity().run(ctx)
        assert res.details["no_data"] is True


# ---------------------------------------------------------------------------
# Review Breadth
# ---------------------------------------------------------------------------

class TestReviewBreadth:
    def test_multiple_authors(self):
        reviewer = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        carol = make_user(id=3, login="carol")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        end = start + timedelta(days=7)

        pr1 = make_pr(1, bob, repo, base_time=start)
        pr2 = make_pr(2, carol, repo, base_time=start, created_delta_hours=24)

        review1 = make_review(10, 1, reviewer, start + timedelta(hours=1))
        review2 = make_review(20, 2, reviewer, start + timedelta(hours=25))

        bundle = make_bundle(
            users=[reviewer, bob, carol],
            repositories=[repo],
            pull_requests=[pr1, pr2],
            reviews=[review1, review2],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        res = ReviewBreadth().run(ctx)
        assert res.metric_slug == "review_breadth"
        assert res.details["unique_authors"] == 2
        assert res.details["total_prs_reviewed"] == 2

    def test_excludes_self_reviews(self):
        alice = make_user(id=1, login="alice")
        bob = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START

        pr_own = make_pr(1, alice, repo, base_time=start)  # own PR
        pr_bob = make_pr(2, bob, repo, base_time=start, created_delta_hours=24)

        # Alice reviews both
        review_self = make_review(10, 1, alice, start + timedelta(hours=1))
        review_bob = make_review(20, 2, alice, start + timedelta(hours=25))

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr_own, pr_bob],
            reviews=[review_self, review_bob],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=3))

        res = ReviewBreadth().run(ctx)
        assert res.details["unique_authors"] == 1  # only bob

    def test_no_reviews(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = ReviewBreadth().run(ctx)
        assert res.details["no_data"] is True


# ---------------------------------------------------------------------------
# Review Comment Substance
# ---------------------------------------------------------------------------

class TestReviewCommentSubstance:
    def test_substantive_review(self):
        reviewer = make_user(id=1, login="alice")
        author = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START

        pr1 = make_pr(1, author, repo, base_time=start)
        review1 = make_review(
            10, 1, reviewer, start + timedelta(hours=1),
            body="Consider using `Optional[str]` here instead of `str | None` for backwards compat. See https://docs.python.org/3/library/typing.html#typing.Optional",
        )

        bundle = make_bundle(
            users=[reviewer, author],
            repositories=[repo],
            pull_requests=[pr1],
            reviews=[review1],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=3))

        res = ReviewCommentSubstance().run(ctx)
        assert res.metric_slug == "review_comment_substance"
        assert res.details["total_comments"] == 1
        # Should score well: has code block, URL, length
        assert res.details["avg_substance_score"] > 40

    def test_empty_review(self):
        reviewer = make_user(id=1, login="alice")
        author = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START

        pr1 = make_pr(1, author, repo, base_time=start)
        review1 = make_review(10, 1, reviewer, start + timedelta(hours=1), body="")

        bundle = make_bundle(
            users=[reviewer, author],
            repositories=[repo],
            pull_requests=[pr1],
            reviews=[review1],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=3))

        res = ReviewCommentSubstance().run(ctx)
        # Empty body should not be scored
        assert res.details["total_comments"] == 0

    def test_no_reviews(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = ReviewCommentSubstance().run(ctx)
        assert res.details["no_data"] is True


# ---------------------------------------------------------------------------
# Mentorship Signal
# ---------------------------------------------------------------------------

class TestMentorshipSignal:
    def test_reviews_junior_authors(self):
        reviewer = make_user(id=1, login="alice")
        junior = make_user(id=2, login="newbie")  # will have <5 PRs
        senior = make_user(id=3, login="veteran")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        end = start + timedelta(days=30)

        # Junior has 1 PR
        pr_junior = make_pr(1, junior, repo, base_time=start)

        # Senior has 6 PRs (above threshold)
        senior_prs = [
            make_pr(10 + i, senior, repo, base_time=start, created_delta_hours=24 * i)
            for i in range(6)
        ]
        pr_senior = senior_prs[0]

        # Alice reviews one from each
        review_junior = make_review(100, 1, reviewer, start + timedelta(hours=2))
        review_senior = make_review(200, 10, reviewer, start + timedelta(hours=26))

        bundle = make_bundle(
            users=[reviewer, junior, senior],
            repositories=[repo],
            pull_requests=[pr_junior] + senior_prs,
            reviews=[review_junior, review_senior],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

        res = MentorshipSignal().run(ctx)
        assert res.metric_slug == "mentorship_signal"
        assert res.details["total_reviewed_prs"] == 2
        assert res.details["junior_review_count"] == 1
        assert res.details["mentorship_rate"] == pytest.approx(50.0)

    def test_no_reviews(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = MentorshipSignal().run(ctx)
        assert res.details["no_data"] is True


