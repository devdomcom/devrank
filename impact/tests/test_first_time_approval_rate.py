"""Dedicated tests for FirstTimeApprovalRate."""
from datetime import timedelta

import pytest
from impact.domain.models import CommentType, ReviewState
from impact.metrics.plugins.authored.first_time_approval_rate import FirstTimeApprovalRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


class TestFirstTimeApprovalRate:
    """Comprehensive tests for FirstTimeApprovalRate (immediate approval w/o CR/inline)."""

    def test_immediate_approval(self):
        """PR with first review=APPROVED + no inline → first-time success."""
        author = make_user(id=1, login="alice")
        reviewer = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, author, repo, base_time=start, merged_at=start + timedelta(hours=2))

        # First review: approved, no inline comments
        review_approved = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[author, reviewer],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[review_approved],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = FirstTimeApprovalRate().run(ctx)
        assert res.metric_slug == "first_time_approval_rate"
        assert res.details["rate"] == 1.0
        assert res.details["immediate_count"] == 1
        assert res.details["merged_pr_count"] == 1
        assert res.details["per_pr"][0]["immediate_approval"] is True

    def test_with_prior_cr(self):
        """Prior CR blocks first-time approval."""
        author = make_user(id=1, login="alice")
        reviewer = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, author, repo, base_time=start, merged_at=start + timedelta(hours=5))

        # CR first, then approval
        review_cr = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.CHANGES_REQUESTED)
        review_approved = make_review(20, 1, reviewer, start + timedelta(hours=3), state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[author, reviewer],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[review_cr, review_approved],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = FirstTimeApprovalRate().run(ctx)
        assert res.details["rate"] == 0.0
        assert res.details["per_pr"][0]["immediate_approval"] is False

    def test_approval_with_inline(self):
        """Approval with inline comments not first-time."""
        author = make_user(id=1, login="alice")
        reviewer = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, author, repo, base_time=start, merged_at=start + timedelta(hours=2))

        review_approved = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.APPROVED)
        inline = make_comment(100, 1, reviewer, start + timedelta(hours=1), type=CommentType.REVIEW, review_id=10, path="file.py", position=5)

        bundle = make_bundle(
            users=[author, reviewer],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[review_approved],
            comments=[inline],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = FirstTimeApprovalRate().run(ctx)
        assert res.details["rate"] == 0.0  # inline blocks

    def test_no_reviews_or_merged(self):
        """No merged PRs/reviews → no_data."""
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = FirstTimeApprovalRate().run(ctx)
        assert res.details["no_data"] is True
        assert res.details["rate"] == 0.0
