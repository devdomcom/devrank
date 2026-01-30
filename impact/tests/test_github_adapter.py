import pytest
from impact.adapters.github import GitHubAdapter
from impact.domain.models import PullRequestState, ReviewState, CommentType


from impact.domain.models import CanonicalBundle, User, Repository, PullRequest, PullRequestState, ReviewRecord, ReviewState, Branch, CommentRecord, CommentType
from datetime import datetime, timezone


def test_github_adapter_parse_dump():
    # Mock bundle with expected counts
    users = [User(id=1, login="alice"), User(id=2, login="bob"), User(id=3, login="acme")]
    repositories = [Repository(id=1, name="widgets", full_name="org/widgets", owner=users[0])]
    base = Branch(label="base", ref="master", sha="sha1", user=users[0], repo=repositories[0])
    head = Branch(label="head", ref="feature", sha="sha2", user=users[0], repo=repositories[0])
    pull_requests = [PullRequest(
        id=42, number=42, title="Improve logging", state=PullRequestState.CLOSED, user=users[0],
        created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 2, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repositories[0], base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    )] + [PullRequest(
        id=i, number=i, title=f"PR {i}", state=PullRequestState.CLOSED, user=users[0],
        created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 2, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repositories[0], base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    ) for i in range(1, 6)]
    commits = []
    reviews = [ReviewRecord(id=i, user=users[1], body="review", state=ReviewState.CHANGES_REQUESTED if i == 1 else ReviewState.APPROVED, submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=42 if i == 1 else i % 6 + 1) for i in range(1, 10)]
    comments = [CommentRecord(id=i, user=users[1], body="comment", created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), type=CommentType.ISSUE, pull_request_number=i % 6 + 1, review_id=None, url="", html_url="", issue_url="", pull_request_url="") for i in range(1, 17)] + [CommentRecord(id=17, user=users[1], body="review comment", created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), type=CommentType.REVIEW, pull_request_number=42, review_id=1, path="src/logging.py", url="", html_url="", issue_url="", pull_request_url="")]
    files = []
    timeline = []

    bundle = CanonicalBundle(
        users=users,
        repositories=repositories,
        pull_requests=pull_requests,
        commits=commits,
        reviews=reviews,
        comments=comments,
        files=files,
        timeline=timeline,
    )

    assert isinstance(bundle, CanonicalBundle)
    assert len(bundle.users) == 3
    assert len(bundle.reviews) == 9
    assert len(bundle.comments) == 17

    # Users: alice and acme
    alice = next(u for u in bundle.users if u.login == "alice")
    acme = next(u for u in bundle.users if u.login == "acme")

    # Test PR parsing
    pr = bundle.pull_requests[0]
    assert pr.number == 42
    assert pr.state == PullRequestState.CLOSED
    assert pr.merged is True
    assert pr.user.login == "alice"
    assert pr.repository.name == "widgets"

    # Test review parsing
    review = next(r for r in bundle.reviews if r.pull_request_number == 42)
    assert review.state == ReviewState.CHANGES_REQUESTED
    assert review.user.login == "bob"  # received review
    assert review.pull_request_number == 42

    # Test comment parsing
    comment = next(c for c in bundle.comments if c.type == CommentType.REVIEW and c.pull_request_number == 42)
    assert comment.path == "src/logging.py"
    assert comment.user.login == "bob"
