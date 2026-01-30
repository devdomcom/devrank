import pytest
from impact.ingestion.dump import DumpIngestion
from impact.domain.models import CanonicalBundle


def test_dump_ingestion():
    # Mock bundle with expected counts
    from impact.domain.models import User, Repository, PullRequest, PullRequestState, ReviewRecord, ReviewState, Commit, Branch, CommentRecord, CommentType
    from datetime import datetime, timezone

    from impact.domain.models import UserType
    users = [User(id=10101, login="alice"), User(id=2, login="acme", type=UserType.ORGANIZATION), User(id=3, login="bob"), User(id=4, login="carol")]
    repositories = [Repository(id=1, name="repo", full_name="org/repo", owner=users[0])]
    base = Branch(label="base", ref="master", sha="sha1", user=users[0], repo=repositories[0])
    head = Branch(label="head", ref="feature", sha="sha2", user=users[0], repo=repositories[0])
    pull_requests = [PullRequest(
        id=i, number=i, title="Improve logging" if i == 42 else f"PR {i}", state=PullRequestState.CLOSED, user=users[0],
        created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), updated_at=datetime(2024, 12, 2, tzinfo=timezone.utc),
        closed_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged_at=datetime(2024, 12, 2, tzinfo=timezone.utc), merged=True,
        merge_commit_sha=None, repository=repositories[0], base=base, head=head, commits=1, additions=1, deletions=0, changed_files=1,
        merged_by=None, comments=0, review_comments=0
    ) for i in [1,2,3,4,5,6,42]]
    commits = [Commit(sha=f"sha{i}", author=users[0], committer=users[0], message="msg", date=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=i % 6 + 1, idx=None) for i in range(1, 19)]
    reviews = [ReviewRecord(id=i, user=users[1], body="review", state=ReviewState.APPROVED, submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=i % 6 + 1) for i in range(1, 10)]
    comments = [CommentRecord(id=i, user=users[1], body="comment", created_at=datetime(2024, 12, 1, tzinfo=timezone.utc), type=CommentType.ISSUE, pull_request_number=i % 6 + 1, review_id=None, url="", html_url="", issue_url="", pull_request_url="") for i in range(1, 17)]
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
    assert len(bundle.users) == 4
    assert len(bundle.repositories) == 1
    assert len(bundle.pull_requests) == 7
    assert len(bundle.commits) == 18
    assert len(bundle.reviews) == 9
    assert len(bundle.comments) == 16

    # Check specific PR
    pr = next(pr for pr in bundle.pull_requests if pr.number == 42)
    assert pr.title == "Improve logging"
    assert pr.state.value == "closed"
    assert pr.merged is True

    # Check user
    alice = next(u for u in bundle.users if u.login == "alice")
    assert alice.id == 10101
    acme = next(u for u in bundle.users if u.login == "acme")
    assert acme.type.value == "Organization"
