from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Any, Dict
from enum import Enum


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
    avatar_url: Optional[str] = None
    type: UserType = UserType.USER


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
    body: Optional[str] = None
    state: PullRequestState
    user: User
    created_at: datetime
    updated_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    merged: bool = False
    merge_commit_sha: Optional[str] = None
    repository: Repository
    base: Branch
    head: Branch
    commits: int
    additions: int
    deletions: int
    changed_files: int
    merged_by: Optional[User] = None
    comments: int
    review_comments: int


class Commit(BaseModel):
    sha: str
    author: User
    committer: User
    message: str
    date: datetime
    pull_request_number: Optional[int] = None
    idx: Optional[int] = None


class ReviewRecord(BaseModel):
    id: int
    user: User
    body: Optional[str] = None
    state: ReviewState
    submitted_at: datetime
    pull_request_number: int


class CommentRecord(BaseModel):
    id: int
    user: User
    body: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    type: CommentType
    pull_request_number: Optional[int] = None
    review_id: Optional[int] = None
    in_reply_to_id: Optional[int] = None
    path: Optional[str] = None
    position: Optional[int] = None


class FileRecord(BaseModel):
    sha: str
    filename: str
    additions: int
    deletions: int
    changes: int
    status: str
    pull_request_number: int


class TimelineEvent(BaseModel):
    id: int
    node_id: Optional[str] = None
    url: Optional[str] = None
    event: str
    actor: User
    created_at: datetime
    pull_request_number: int
    commit_id: Optional[str] = None
    commit_url: Optional[str] = None
    comment_id: Optional[int] = None
    state: Optional[str] = None
    html_url: Optional[str] = None


class CanonicalBundle(BaseModel):
    users: List[User]
    repositories: List[Repository]
    pull_requests: List[PullRequest]
    commits: List[Commit]
    reviews: List[ReviewRecord]
    comments: List[CommentRecord]
    files: List[FileRecord]
    timeline: List[TimelineEvent]


class MetricContext(BaseModel):
    ledger: Any  # Ledger instance
    user_login: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class MetricResult(BaseModel):
    metric_slug: str
    summary: str
    details: Dict[str, Any]
