from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class UserType(str, Enum):
    USER = "User"
    ORGANIZATION = "Organization"
    BOT = "Bot"


class PullRequestState(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class ReviewState(str, Enum):
    APPROVED = "approved"
    CHANGES_REQUESTED = "changes_requested"
    COMMENTED = "commented"


class CommentType(str, Enum):
    ISSUE = "issue"
    REVIEW = "review"


class User(BaseModel):
    id: int
    login: str
    avatar_url: str | None = None
    type: UserType = UserType.USER
    node_id: str | None = None
    # Canonical bot flag — set by the provider adapter using provider-specific
    # signals (GitHub: type/suffix/node_id, GitLab: bot flag, etc.).
    # Metrics use ``is_bot_user()`` which delegates to this field.
    is_bot: bool = False


class Repository(BaseModel):
    id: int
    name: str
    full_name: str
    owner: User


class Branch(BaseModel):
    label: str
    ref: str
    sha: str
    user: User
    repo: Repository


class PullRequest(BaseModel):
    id: int
    number: int
    title: str
    body: str | None = None
    state: PullRequestState
    user: User
    created_at: datetime
    updated_at: datetime | None = None
    closed_at: datetime | None = None
    merged_at: datetime | None = None
    draft: bool = False
    merged: bool = False
    merge_commit_sha: str | None = None
    repository: Repository
    base: Branch
    head: Branch
    commits: int
    additions: int
    deletions: int
    changed_files: int
    merged_by: User | None = None
    comments: int
    review_comments: int
    # Provider-normalised labels (e.g. ["bug", "enhancement"]).
    # Populated by adapter from provider API (GitHub: labels[].name).
    labels: list[str] = []


class Commit(BaseModel):
    sha: str
    author: User
    committer: User
    message: str
    date: datetime
    pull_request_number: int | None = None
    idx: int | None = None
    # Git DAG parent count: 1 = normal, >=2 = merge commit.
    # Populated by adapter from provider API (e.g. len(parents)).
    parent_count: int = 1


class ReviewRecord(BaseModel):
    id: int
    user: User
    body: str | None = None
    state: ReviewState
    submitted_at: datetime
    pull_request_number: int


class CommentRecord(BaseModel):
    id: int
    user: User
    body: str
    created_at: datetime
    updated_at: datetime | None = None
    type: CommentType
    pull_request_number: int | None = None
    review_id: int | None = None
    in_reply_to_id: int | None = None
    path: str | None = None
    position: int | None = None
    # Canonical code-suggestion flag — set by the adapter when the comment
    # contains a provider-native code suggestion (GitHub: ```suggestion block,
    # GitLab: suggestions API, etc.).  Metrics check this instead of parsing
    # provider-specific markdown.
    has_code_suggestion: bool = False


class FileRecord(BaseModel):
    sha: str
    filename: str
    additions: int
    deletions: int
    changes: int
    status: str
    pull_request_number: int
    # Patch for rework detection (unified diff hunks; loaded from files.jsonl)
    patch: str | None = None
    # Full file content for AST analysis (tree-sitter); fetched via Git Blobs API
    content: str | None = None


class TimelineEvent(BaseModel):
    id: int
    node_id: str | None = None
    url: str | None = None
    event: str
    actor: User
    created_at: datetime
    pull_request_number: int
    commit_id: str | None = None
    commit_url: str | None = None
    comment_id: int | None = None
    state: str | None = None
    html_url: str | None = None
    # requested_reviewer for accurate review_demand (who was requested)
    requested_reviewer: User | None = None


class ReleaseRecord(BaseModel):
    """Release / tag event (DORA Deployment Frequency data source).

    Populated from provider release APIs (GitHub Releases, GitLab
    Releases, Bitbucket tags, etc.).  A new fetch run will pull this
    data; existing dumps without releases will simply have an empty list.
    """
    id: int
    tag_name: str
    name: str | None = None
    created_at: datetime
    published_at: datetime | None = None
    draft: bool = False
    prerelease: bool = False
    author: User | None = None
    target_commitish: str | None = None


class DeploymentRecord(BaseModel):
    """Deployment event (DORA Deployment Frequency data source).

    Populated from provider deployment APIs (GitHub Deployments, GitLab
    Environments, Bitbucket Deployments, Azure DevOps Releases, etc.).
    """
    id: int
    sha: str
    ref: str
    environment: str
    created_at: datetime
    updated_at: datetime | None = None
    creator: User | None = None
    description: str | None = None


class CIRunRecord(BaseModel):
    """CI pipeline run (provider-neutral).

    Populated from CI systems: GitHub Actions, GitLab CI, Bitbucket
    Pipelines, Azure DevOps Builds, etc.  Enables splitting lead time
    into coding, review, CI, and merge phases.
    """
    id: int
    name: str | None = None
    head_sha: str
    event: str | None = None
    status: str | None = None  # queued, in_progress, completed
    conclusion: str | None = None  # success, failure, cancelled, ...
    created_at: datetime
    updated_at: datetime | None = None
    run_started_at: datetime | None = None
    pull_request_number: int | None = None
    # Duration in seconds (run_started_at to updated_at when completed)
    duration_seconds: int | None = None


class CanonicalBundle(BaseModel):
    users: list[User]
    repositories: list[Repository]
    pull_requests: list[PullRequest]
    commits: list[Commit]
    reviews: list[ReviewRecord]
    comments: list[CommentRecord]
    files: list[FileRecord]
    timeline: list[TimelineEvent]
    # New data sources (populated by expanded fetcher; empty in legacy dumps)
    releases: list[ReleaseRecord] = []
    deployments: list[DeploymentRecord] = []
    ci_runs: list[CIRunRecord] = []
    # User TZ for off-hours (e.g. Europe/Istanbul for Turkey; data TZ from manifest/Z=UTC)
    user_timezone: str | None = None


class MetricContext(BaseModel):
    ledger: Any  # Ledger instance
    user_login: str
    start_date: datetime | None = None
    end_date: datetime | None = None


class MetricResult(BaseModel):
    metric_slug: str
    summary: str
    details: dict[str, Any]

    def has_no_data(self) -> bool:
        """Delegate to central no-data check."""
        from impact.metrics.utils import is_no_data
        return is_no_data(self.details or {})


