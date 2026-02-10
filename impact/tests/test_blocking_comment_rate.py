from datetime import timedelta

from impact.domain.models import ReviewState
from impact.metrics.plugins.influence.blocking_comment_rate import BlockingCommentRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_blocking_rate_ownership():
    """Moderate blocking shows ownership."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    # Mix: 1 blocking CR, 1 approval on another user's PR
    pr_author = make_user(id=3, login="bob")
    pr1 = make_pr(1, pr_author, repo)
    review1 = make_review(10, 1, user, DEFAULT_START, state=ReviewState.CHANGES_REQUESTED)
    review2 = make_review(
        11, 1, user, DEFAULT_START + timedelta(hours=1), state=ReviewState.APPROVED
    )

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        reviews=[review1, review2],
    )
    context = make_context(bundle, user_login="alice")

    metric = BlockingCommentRate()
    res = metric.run(context)

    assert res.metric_slug == "blocking_comment_rate"
    assert res.details["total_reviews"] == 2
    assert res.details["blocking_count"] == 1
    assert res.details["blocking_rate"] == 0.5
    assert res.details["blocking_per_day"] > 0.0  # balanced by period


def test_blocking_rate_cosmetic():
    """All cosmetic."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = BlockingCommentRate()
    res = metric.run(context)

    assert res.details["total_reviews"] == 0
    assert res.details["blocking_rate"] == 0.0
