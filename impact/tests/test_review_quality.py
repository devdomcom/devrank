from datetime import datetime, timezone, timedelta

from impact.metrics.plugins.review_quality import ReviewIterations, TimeToFirstReview, SlowReviewResponse
from impact.domain.models import (
    User, Repository, Branch, PullRequest, PullRequestState,
    CanonicalBundle, ReviewRecord, ReviewState, Commit
)
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


def _make_pr(number: int, created: datetime, merged_at: datetime | None, author: User, repo: Repository):
    base = Branch(label="base", ref="master", sha="sha1", user=author, repo=repo)
    head = Branch(label="head", ref=f"f{number}", sha="sha2", user=author, repo=repo)
    merged_flag = merged_at is not None
    return PullRequest(
        id=number,
        number=number,
        title=f"PR {number}",
        state=PullRequestState.CLOSED if merged_flag else PullRequestState.OPEN,
        user=author,
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


def test_review_quality_metrics():
    author = User(id=1, login="alice")
    reviewer = User(id=2, login="bob")
    owner = User(id=3, login="org")
    repo = Repository(id=10, name="repo", full_name="org/repo", owner=owner)

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pr1 = _make_pr(1, start, start + timedelta(hours=24), author, repo)
    pr2 = _make_pr(2, start + timedelta(hours=1), start + timedelta(hours=48), author, repo)

    review1 = ReviewRecord(
        id=101, user=reviewer, body=None, state=ReviewState.CHANGES_REQUESTED,
        submitted_at=start + timedelta(hours=5), pull_request_number=1
    )
    review2 = ReviewRecord(
        id=102, user=reviewer, body=None, state=ReviewState.APPROVED,
        submitted_at=start + timedelta(hours=6), pull_request_number=1
    )
    review3 = ReviewRecord(
        id=103, user=reviewer, body=None, state=ReviewState.COMMENTED,
        submitted_at=start + timedelta(hours=3), pull_request_number=2
    )

    commit_after_review = Commit(
        sha="c1",
        author=author,
        committer=author,
        message="fix",
        date=start + timedelta(hours=7),
        pull_request_number=1,
        idx=None,
    )

    bundle = CanonicalBundle(
        users=[author, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[commit_after_review],
        reviews=[review1, review2, review3],
        comments=[],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)
    ctx = MetricContext(ledger=ledger, user_login="alice", start_date=start, end_date=start + timedelta(days=5))

    iterations = ReviewIterations().run(ctx)
    assert iterations.details["average_iterations"] == 0.5
    assert iterations.details["per_pr"][0]["iterations"] == 1

    tfr = TimeToFirstReview().run(ctx)
    # PR1: 5h, PR2: 2h -> median 3.5h, p75 4.25h
    assert abs(tfr.details["median_hours"] - 3.5) < 1e-6
    assert abs(tfr.details["p75_hours"] - 4.25) < 1e-6

    slow = SlowReviewResponse().run(ctx)
    assert slow.details["samples"] == 1
    assert abs(slow.details["median_hours"] - 2.0) < 1e-6
    assert slow.details["per_review"][0]["pr"] == 1
