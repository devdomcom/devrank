from datetime import datetime, timezone

from impact.metrics.plugins.review_leverage import ReviewLeverage
from impact.domain.models import ReviewState
from impact.tests.conftest import (
    make_user,
    make_repo,
    make_pr,
    make_review,
    make_bundle,
    make_context,
)


def test_review_leverage_metric():
    # Mock data with change requests
    user = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    created = datetime(2024, 12, 1, tzinfo=timezone.utc)
    merged = datetime(2024, 12, 2, tzinfo=timezone.utc)
    pr1 = make_pr(1, user, repo, created_at=created, merged_at=merged)

    review1 = make_review(
        id=1,
        pr_number=1,
        user=reviewer,
        submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc),
        state=ReviewState.CHANGES_REQUESTED,
        body="Changes requested",
    )

    bundle = make_bundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1],
        reviews=[review1],
    )

    # Context for reviewer (bob) - use matching date range
    context = make_context(
        bundle,
        user_login="bob",
        start_date=datetime(2024, 11, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
    )

    metric = ReviewLeverage()
    result = metric.run(context)

    assert result.metric_slug == "review_leverage"
    assert "PRs reviewed" in result.summary
    assert isinstance(result.details, dict)
    assert "total_reviews" in result.details
    assert "effective_changes" in result.details
