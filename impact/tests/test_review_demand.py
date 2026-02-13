"""Dedicated tests for ReviewDemand."""
from datetime import timedelta

import pytest
from impact.metrics.plugins.influence.review_demand import ReviewDemand
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_timeline_event,  # assume or use make
    make_user,
)


# Note: add make_timeline_event to conftest if needed; for now use direct
class TestReviewDemand:
    """Comprehensive tests for ReviewDemand (review_requested events demand)."""

    def test_demand_count(self):
        """review_requested events count as demand."""
        user = make_user(id=1, login="alice")
        requester = make_user(id=2, login="bob")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, requester, repo, base_time=start)

        # Timeline: review_requested event
        timeline_evt = type('obj', (object,), {
            'event': 'review_requested',
            'actor': requester,
            'created_at': start + timedelta(hours=1),
            'pull_request_number': 1,
        })()  # mock for test

        bundle = make_bundle(
            users=[user, requester],
            repositories=[repo],
            pull_requests=[pr],
            timeline=[timeline_evt],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = ReviewDemand().run(ctx)
        assert res.metric_slug == "review_demand"
        assert res.details["demand_count"] > 0
        assert res.details["demand_per_week"] > 0

    def test_no_demand(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = ReviewDemand().run(ctx)
        assert res.details["demand_count"] == 0
