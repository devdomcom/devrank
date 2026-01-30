from datetime import datetime, timezone, timedelta

from impact.metrics.plugins.pr_merge_effectiveness import PRMergeEffectiveness
from impact.domain.models import (
    User, Repository, Branch, PullRequest, PullRequestState,
    ReviewRecord, ReviewState, CommentRecord, CommentType,
    CanonicalBundle
)
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


def _make_pr(number: int, created: datetime, merged_at: datetime | None, user: User, repo: Repository):
    base = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head = Branch(label="head", ref=f"feature-{number}", sha="sha2", user=user, repo=repo)
    merged_flag = merged_at is not None
    return PullRequest(
        id=number,
        number=number,
        title=f"PR {number}",
        state=PullRequestState.CLOSED if merged_flag else PullRequestState.OPEN,
        user=user,
        created_at=created,
        updated_at=merged_at or created,
        closed_at=merged_at,
        merged_at=merged_at,
        merged=merged_flag,
        merge_commit_sha=None,
        repository=repo,
        base=base,
        head=head,
        commits=1,
        additions=1,
        deletions=0,
        changed_files=1,
        merged_by=None,
        comments=0,
        review_comments=0,
    )


def _make_review(id: int, pr_number: int, user: User, submitted_at: datetime, state: ReviewState):
    return ReviewRecord(
        id=id,
        user=user,
        body="Review",
        state=state,
        submitted_at=submitted_at,
        pull_request_number=pr_number,
    )


def _make_comment(id: int, pr_number: int, user: User, created_at: datetime, type_: CommentType, review_id=None):
    return CommentRecord(
        id=id,
        user=user,
        body="Comment",
        created_at=created_at,
        type=type_,
        pull_request_number=pr_number,
        review_id=review_id,
        url="",
        html_url="",
        issue_url="",
        pull_request_url="",
    )


def test_pr_merge_effectiveness():
    user = User(id=1, login="alice")
    reviewer = User(id=2, login="bob")
    owner = User(id=3, login="org")
    repo = Repository(id=1, name="repo", full_name="org/repo", owner=owner)

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pr1 = _make_pr(1, start, start + timedelta(hours=10), user, repo)
    pr2 = _make_pr(2, start + timedelta(hours=1), start + timedelta(hours=20), user, repo)
    pr3 = _make_pr(3, start + timedelta(hours=2), None, user, repo)  # open, ignored

    review1 = _make_review(101, 1, reviewer, start + timedelta(hours=2), ReviewState.APPROVED)
    review2 = _make_review(102, 1, reviewer, start + timedelta(hours=5), ReviewState.COMMENTED)

    comment1 = _make_comment(201, 1, reviewer, start + timedelta(hours=3), CommentType.ISSUE)
    comment2 = _make_comment(202, 1, reviewer, start + timedelta(hours=4), CommentType.REVIEW, review_id=102)

    bundle = CanonicalBundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        commits=[],
        reviews=[review1, review2],
        comments=[comment1, comment2],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)
    context = MetricContext(ledger=ledger, user_login="alice", start_date=start, end_date=start + timedelta(days=10))

    metric = PRMergeEffectiveness()
    res = metric.run(context)

    assert res.metric_slug == "pr_merge_effectiveness"
    assert res.details["merged_pr_count"] == 2
    assert res.details["average_merge_time_hours"] == 14.5  # (10 + 19)/2
    assert res.details["average_back_and_forth"] == 2.0  # pr1: 4 interactions, pr2: 0
    # Actually, pr2 has no interactions, so avg (2 + 0)/2 = 1, but wait, let's check.

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