"""Dedicated tests for MergeDelay."""
from datetime import timedelta

import pytest
from impact.domain.models import ReviewState
from impact.metrics.plugins.authored.merge_delay import MergeDelay
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


class TestMergeDelay:
    """Comprehensive tests for MergeDelay (last approval → merge)."""

    def test_merge_delay(self):
        """Latest approval to merge yields hours."""
        author = make_user(id=1, login="alice")
        reviewer = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, author, repo, base_time=start, merged_at=start + timedelta(hours=10))

        # Approvals; last at t=4h
        rev1 = make_review(10, 1, reviewer, start + timedelta(hours=2), state=ReviewState.APPROVED)  # need import ReviewState? assume in full
        rev2 = make_review(20, 1, reviewer, start + timedelta(hours=4), state=ReviewState.APPROVED)

        bundle = make_bundle(
            users=[author, reviewer],
            repositories=[repo],
            pull_requests=[pr],
            reviews=[rev1, rev2],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = MergeDelay().run(ctx)
        assert res.metric_slug == "merge_delay"
        assert res.details["median_hours"] == pytest.approx(6.0)  # 10-4h
        assert res.details["per_pr_hours"][0]["hours"] == pytest.approx(6.0)

    def test_no_approval(self):
        """No approvals → no delay measured."""
        author = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, author, repo, base_time=start, merged_at=start + timedelta(hours=5))

        bundle = make_bundle(users=[author], repositories=[repo], pull_requests=[pr])
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = MergeDelay().run(ctx)
        assert res.details["merged_count"] == 1
        assert len(res.details.get("per_pr_hours", [])) == 0  # no delay

    def test_no_merged(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = MergeDelay().run(ctx)
        assert res.details["no_data"] is True
