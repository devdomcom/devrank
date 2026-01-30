"""
Shared test fixtures and factory functions for impact tests.

These factories help reduce duplication across test modules and provide
consistent test data creation patterns.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional

from impact.domain.models import (
    User,
    Repository,
    Branch,
    PullRequest,
    PullRequestState,
    ReviewRecord,
    ReviewState,
    CommentRecord,
    CommentType,
    Commit,
    CanonicalBundle,
)
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


# Default base datetime for tests
DEFAULT_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_user(id: int = 1, login: str = "alice", type: str = "User") -> User:
    """Create a User for testing."""
    return User(id=id, login=login, type=type)


def make_repo(id: int = 1, name: str = "repo", owner: Optional[User] = None) -> Repository:
    """Create a Repository for testing."""
    if owner is None:
        owner = make_user(id=999, login="org")
    return Repository(id=id, name=name, full_name=f"{owner.login}/{name}", owner=owner)


def make_pr(
    number: int,
    user: User,
    repo: Repository,
    created_at: Optional[datetime] = None,
    merged_at: Optional[datetime] = None,
    *,
    created_delta_hours: Optional[float] = None,
    merged_delta_hours: Optional[float] = None,
    base_time: Optional[datetime] = None,
) -> PullRequest:
    """
    Create a PullRequest for testing.

    Can specify times directly via created_at/merged_at, or use deltas from base_time.

    Args:
        number: PR number (also used as id).
        user: The PR author.
        repo: The repository.
        created_at: Direct datetime for when PR was created.
        merged_at: Direct datetime for when PR was merged (None = not merged).
        created_delta_hours: Hours offset from base_time for created_at.
        merged_delta_hours: Hours offset from base_time for merged_at.
        base_time: Base datetime for delta calculations (defaults to DEFAULT_START).
    """
    base_time = base_time or DEFAULT_START

    # Resolve created_at
    if created_at is None:
        if created_delta_hours is not None:
            created_at = base_time + timedelta(hours=created_delta_hours)
        else:
            created_at = base_time

    # Resolve merged_at
    if merged_at is None and merged_delta_hours is not None:
        merged_at = base_time + timedelta(hours=merged_delta_hours)

    merged_flag = merged_at is not None
    base_branch = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head_branch = Branch(label="head", ref=f"feature-{number}", sha="sha2", user=user, repo=repo)

    return PullRequest(
        id=number,
        number=number,
        title=f"PR {number}",
        state=PullRequestState.CLOSED if merged_flag else PullRequestState.OPEN,
        user=user,
        created_at=created_at,
        updated_at=merged_at or created_at,
        closed_at=merged_at,
        merged_at=merged_at,
        merged=merged_flag,
        merge_commit_sha=None,
        repository=repo,
        base=base_branch,
        head=head_branch,
        commits=1,
        additions=1,
        deletions=0,
        changed_files=1,
        merged_by=None,
        comments=0,
        review_comments=0,
    )


def make_review(
    id: int,
    pr_number: int,
    user: User,
    submitted_at: datetime,
    state: ReviewState = ReviewState.APPROVED,
    body: Optional[str] = "Review",
) -> ReviewRecord:
    """Create a ReviewRecord for testing."""
    return ReviewRecord(
        id=id,
        user=user,
        body=body,
        state=state,
        submitted_at=submitted_at,
        pull_request_number=pr_number,
    )


def make_comment(
    id: int,
    pr_number: int,
    user: User,
    created_at: datetime,
    type: CommentType = CommentType.ISSUE,
    review_id: Optional[int] = None,
    body: str = "Comment",
) -> CommentRecord:
    """Create a CommentRecord for testing."""
    return CommentRecord(
        id=id,
        user=user,
        body=body,
        created_at=created_at,
        type=type,
        pull_request_number=pr_number,
        review_id=review_id,
        url="",
        html_url="",
        issue_url="",
        pull_request_url="",
    )


def make_commit(
    sha: str,
    author: User,
    date: datetime,
    pr_number: int,
    message: str = "commit",
    committer: Optional[User] = None,
) -> Commit:
    """Create a Commit for testing."""
    return Commit(
        sha=sha,
        author=author,
        committer=committer or author,
        message=message,
        date=date,
        pull_request_number=pr_number,
        idx=None,
    )


def make_bundle(
    users: Optional[list] = None,
    repositories: Optional[list] = None,
    pull_requests: Optional[list] = None,
    commits: Optional[list] = None,
    reviews: Optional[list] = None,
    comments: Optional[list] = None,
    files: Optional[list] = None,
    timeline: Optional[list] = None,
) -> CanonicalBundle:
    """Create a CanonicalBundle for testing with sensible defaults."""
    return CanonicalBundle(
        users=users or [],
        repositories=repositories or [],
        pull_requests=pull_requests or [],
        commits=commits or [],
        reviews=reviews or [],
        comments=comments or [],
        files=files or [],
        timeline=timeline or [],
    )


def make_context(
    bundle: CanonicalBundle,
    user_login: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> MetricContext:
    """Create a MetricContext for testing."""
    ledger = Ledger(bundle)
    return MetricContext(
        ledger=ledger,
        user_login=user_login,
        start_date=start_date or DEFAULT_START,
        end_date=end_date or (DEFAULT_START + timedelta(days=10)),
    )
