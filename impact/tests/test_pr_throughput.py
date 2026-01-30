from datetime import datetime, timezone, timedelta

from impact.metrics.plugins.pr_throughput import PRThroughput
from impact.domain.models import (
    User, Repository, Branch, PullRequest, PullRequestState,
    CanonicalBundle
)
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


def _make_pr(number: int, author: User, repo: Repository, created_delta: int, merged_delta: int | None):
    created = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=created_delta)
    merged_at = None
    merged_flag = False
    if merged_delta is not None:
        merged_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=merged_delta)
        merged_flag = True
    base = Branch(label="base", ref="master", sha="sha1", user=author, repo=repo)
    head = Branch(label="head", ref=f"feature-{number}", sha="sha2", user=author, repo=repo)
    return PullRequest(
        id=number,
        number=number,
        title=f"PR {number}",
        state=PullRequestState.OPEN if not merged_flag else PullRequestState.CLOSED,
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


def test_pr_throughput_counts_opened_and_merged():
    user = User(id=1, login="alice")
    owner = User(id=2, login="org")
    repo = Repository(id=1, name="repo", full_name="org/repo", owner=owner)

    pr1 = _make_pr(1, user, repo, created_delta=0, merged_delta=2)   # merged inside window
    pr2 = _make_pr(2, user, repo, created_delta=5, merged_delta=None)  # open only
    pr3 = _make_pr(3, user, repo, created_delta=-5, merged_delta=None) # before window, filtered by ledger

    bundle = CanonicalBundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        commits=[],
        reviews=[],
        comments=[],
        files=[],
        timeline=[],
    )
    ledger = Ledger(bundle)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 10, tzinfo=timezone.utc)
    context = MetricContext(ledger=ledger, user_login="alice", start_date=start, end_date=end)

    metric = PRThroughput()
    res = metric.run(context)

    assert res.metric_slug == "pr_throughput"
    assert res.details["opened_count"] == 2  # pr1 and pr2
    assert res.details["merged_count"] == 1  # only pr1
    assert res.details["merge_ratio"] == 0.5
    assert res.details["merged_pr_numbers"] == [1]
