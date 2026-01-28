import pytest
from impact.adapters.github import GitHubAdapter
from impact.domain.models import PullRequestState, ReviewState, CommentType


def test_github_adapter_parse_dump():
    dump_path = "samples/github_test/dump_2024-12"
    adapter = GitHubAdapter()
    bundle = adapter.parse_dump(dump_path)

    assert len(bundle.users) == 4
    assert len(bundle.pull_requests) == 2
    assert len(bundle.reviews) == 5
    assert len(bundle.comments) == 5

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