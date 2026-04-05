"""
Shared test fixtures and factory functions for impact tests.

These factories help reduce duplication across test modules and provide
consistent test data creation patterns.
"""

from datetime import UTC, datetime, timedelta

from impact.domain.models import (
    Branch,
    CanonicalBundle,
    CommentRecord,
    CommentType,
    Commit,
    FileRecord,
    MetricContext,
    PullRequest,
    PullRequestState,
    Repository,
    ReviewRecord,
    ReviewState,
    User,
)
from impact.ledger.ledger import Ledger

# Default base datetime for tests
DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)


def make_user(
    id: int = 1, login: str = "alice", type: str = "User", *, is_bot: bool = False,
) -> User:
    """Create a User for testing."""
    return User(id=id, login=login, type=type, is_bot=is_bot)


def make_repo(id: int = 1, name: str = "repo", owner: User | None = None) -> Repository:
    """Create a Repository for testing."""
    if owner is None:
        owner = make_user(id=999, login="org")
    return Repository(id=id, name=name, full_name=f"{owner.login}/{name}", owner=owner)


def make_pr(
    number: int,
    user: User,
    repo: Repository,
    created_at: datetime | None = None,
    merged_at: datetime | None = None,
    *,
    additions: int = 1,
    deletions: int = 0,
    created_delta_hours: float | None = None,
    merged_delta_hours: float | None = None,
    base_time: datetime | None = None,
    body: str | None = None,
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
        additions: Number of lines added.
        deletions: Number of lines deleted.
        created_delta_hours: Hours offset from base_time for created_at.
        merged_delta_hours: Hours offset from base_time for merged_at.
        base_time: Base datetime for delta calculations (defaults to DEFAULT_START).
        body: PR body text (for quality metrics).
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
        body=body,
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
        additions=additions,
        deletions=deletions,
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
    body: str | None = "Review",
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
    review_id: int | None = None,
    body: str = "Comment",
    position: int | None = None,
    path: str | None = None,
    *,
    has_code_suggestion: bool = False,
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
        position=position,
        path=path,
        has_code_suggestion=has_code_suggestion,
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
    committer: User | None = None,
    parent_count: int = 1,
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
        parent_count=parent_count,
    )


def make_file(
    sha: str,
    filename: str,
    additions: int = 1,
    deletions: int = 0,
    changes: int = 1,
    status: str = "modified",
    pr_number: int = 1,
    patch: str | None = None,
) -> FileRecord:
    """Create a FileRecord for testing (patch for rework tests)."""
    return FileRecord(
        sha=sha,
        filename=filename,
        additions=additions,
        deletions=deletions,
        changes=changes,
        status=status,
        pull_request_number=pr_number,
        patch=patch,
    )


def make_bundle(
    users: list | None = None,
    repositories: list | None = None,
    pull_requests: list | None = None,
    commits: list | None = None,
    reviews: list | None = None,
    comments: list | None = None,
    files: list | None = None,
    timeline: list | None = None,
    releases: list | None = None,
    deployments: list | None = None,
    ci_runs: list | None = None,
    user_timezone: str | None = None,
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
        releases=releases or [],
        deployments=deployments or [],
        ci_runs=ci_runs or [],
        user_timezone=user_timezone,
    )


def make_context(
    bundle: CanonicalBundle,
    user_login: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> MetricContext:
    """Create a MetricContext for testing."""
    ledger = Ledger(bundle)
    return MetricContext(
        ledger=ledger,
        user_login=user_login,
        start_date=start_date or DEFAULT_START,
        end_date=end_date or (DEFAULT_START + timedelta(days=10)),
    )
