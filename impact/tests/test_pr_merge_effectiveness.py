from datetime import timedelta

from impact.metrics.plugins.pr_merge_effectiveness import PRMergeEffectiveness
from impact.domain.models import ReviewState, CommentType
from impact.tests.conftest import (
    DEFAULT_START,
    make_user,
    make_repo,
    make_pr,
    make_review,
    make_comment,
    make_bundle,
    make_context,
)


def test_pr_merge_effectiveness():
    user = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=10))
    pr2 = make_pr(2, user, repo, created_at=start + timedelta(hours=1), merged_at=start + timedelta(hours=20))
    pr3 = make_pr(3, user, repo, created_at=start + timedelta(hours=2), merged_at=None)  # open, ignored

    review1 = make_review(101, 1, reviewer, start + timedelta(hours=2), ReviewState.APPROVED)
    review2 = make_review(102, 1, reviewer, start + timedelta(hours=5), ReviewState.COMMENTED)

    comment1 = make_comment(201, 1, reviewer, start + timedelta(hours=3), CommentType.ISSUE)
    comment2 = make_comment(202, 1, reviewer, start + timedelta(hours=4), CommentType.REVIEW, review_id=102)

    bundle = make_bundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        reviews=[review1, review2],
        comments=[comment1, comment2],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=10))

    metric = PRMergeEffectiveness()
    res = metric.run(context)

    assert res.metric_slug == "pr_merge_effectiveness"
    assert res.details["merged_pr_count"] == 2
    assert res.details["average_merge_time_hours"] == 14.5  # (10 + 19)/2
    assert res.details["average_back_and_forth"] == 2.0  # pr1: 4 interactions, pr2: 0

    # pr1 interactions: review1, comment1, comment2? comment2 is review comment, but kind=comment_review
    # collect_interactions for pr1, author alice, cutoff merged_at = start+10
    # reviews: review1 (approved), review2 (commented) - both by bob
    # comments: comment1 (issue), comment2 (review)
    # so interactions: review1, review2, comment1, comment2 -> 4
    # pr2: no reviews, no comments -> 0
    # avg 2.0

    # But in code, back_forths.append(len(interactions))
    # Yes, and avg_back_forth = sum / len = (4 + 0)/2 = 2.0

    assert abs(res.details["average_back_and_forth"] - 2.0) < 1e-6
