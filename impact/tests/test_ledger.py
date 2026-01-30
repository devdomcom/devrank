import pytest
from datetime import datetime, timezone
from impact.ledger.ledger import Ledger
from impact.domain.models import User, Repository, PullRequest, PullRequestState, ReviewRecord, ReviewState, Branch


def test_ledger_indexes():
    # Mock data
    user = User(id=1, login="alice")
    owner = User(id=2, login="org")
    repo = Repository(id=1, name="repo", full_name="org/repo", owner=owner)
    base = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head = Branch(label="head", ref="feature", sha="sha2", user=user, repo=repo)
    pr1 = PullRequest(
        id=1, number=1, title="PR 1", state=PullRequestState.CLOSED, user=user,
        created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 2, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repo, base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    )
    pr2 = PullRequest(
        id=2, number=2, title="PR 2", state=PullRequestState.CLOSED, user=user,
        created_at=datetime(2024, 12, 3, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 4, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 4, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 4, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repo, base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    )
    review1 = ReviewRecord(id=1, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=1)
    review2 = ReviewRecord(id=2, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 3, tzinfo=timezone.utc), pull_request_number=2)
    review3 = ReviewRecord(id=3, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 5, tzinfo=timezone.utc), pull_request_number=3)

    from impact.domain.models import CanonicalBundle
    bundle = CanonicalBundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[],
        reviews=[review1, review2, review3],
        comments=[],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)

    # Test user_prs
    alice_prs = ledger.user_prs.get('alice', [])
    assert len(alice_prs) == 2
    assert all(pr.user.login == 'alice' for pr in alice_prs)

    # Test pr_reviews
    reviews_1 = ledger.pr_reviews.get(1, [])
    assert len(reviews_1) == 1

    # Test user_reviews
    alice_reviews = ledger.user_reviews.get('alice', [])
    assert len(alice_reviews) == 3  # her reviews

    # Test get_reviews_for_user
    reviews = ledger.get_reviews_for_user('alice')
    assert len(reviews) == 3
    assert all(r.user.login == 'alice' for r in reviews)


def test_ledger_time_filtering():
    # Mock data
    user = User(id=1, login="alice")
    owner = User(id=2, login="org")
    repo = Repository(id=1, name="repo", full_name="org/repo", owner=owner)
    base = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head = Branch(label="head", ref="feature", sha="sha2", user=user, repo=repo)
    pr1 = PullRequest(
        id=1, number=1, title="PR 1", state=PullRequestState.CLOSED, user=user,
        created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 2, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repo, base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    )
    pr2 = PullRequest(
        id=2, number=2, title="PR 2", state=PullRequestState.CLOSED, user=user,
        created_at=datetime(2024, 12, 3, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 4, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 4, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 4, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repo, base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    )
    review1 = ReviewRecord(id=1, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=1)
    review2 = ReviewRecord(id=2, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 3, tzinfo=timezone.utc), pull_request_number=2)
    review3 = ReviewRecord(id=3, user=user, body="Review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 5, tzinfo=timezone.utc), pull_request_number=3)

    from impact.domain.models import CanonicalBundle
    bundle = CanonicalBundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        commits=[],
        reviews=[review1, review2, review3],
        comments=[],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)

    # Assuming manifest dates
    start = datetime(2024, 12, 1, tzinfo=timezone.utc)
    end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    prs = ledger.get_prs_for_user('alice', start, end)
    assert len(prs) == 2

    reviews = ledger.get_reviews_for_user('alice', start, end)
    assert len(reviews) == 3
