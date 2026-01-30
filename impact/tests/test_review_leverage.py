import pytest
from datetime import datetime, timezone
from impact.metrics.plugins.review_leverage import ReviewLeverage
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext, User, Repository, PullRequest, PullRequestState, ReviewRecord, ReviewState, Branch, CanonicalBundle


def test_review_leverage_metric():
    # Mock data with change requests
    user = User(id=1, login="alice")
    reviewer = User(id=2, login="bob")
    owner = User(id=3, login="org")
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
    review1 = ReviewRecord(id=1, user=reviewer, body="Changes requested", state=ReviewState.CHANGES_REQUESTED, submitted_at=datetime(2024, 12, 1, tzinfo=timezone.utc), pull_request_number=1)

    bundle = CanonicalBundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[],
        reviews=[review1],
        comments=[],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)

    # Mock context
    context = MetricContext(
        ledger=ledger,
        user_login='bob'  # reviewer
    )

    metric = ReviewLeverage()
    result = metric.run(context)

    assert result.metric_slug == "review_leverage"
    assert "PRs reviewed" in result.summary
    assert isinstance(result.details, dict)
    assert "total_reviews" in result.details
    assert "effective_changes" in result.details
