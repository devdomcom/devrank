from datetime import datetime, timezone, timedelta

from impact.metrics.plugins.cycle_time import CycleTime
from impact.domain.models import (
    User, Repository, Branch, PullRequest, PullRequestState,
    CanonicalBundle
)
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


def _make_pr(number: int, created: datetime, merged_at: datetime | None, user: User, repo: Repository):
    base = Branch(label="base", ref="master", sha="sha1", user=user, repo=repo)
    head = Branch(label="head", ref=f"feature-{number}", sha="sha2", user=user, repo=repo)
    merged_flag = merged_at is not None
    return PullRequest(
        id=number,
        number=number,
        title=f"PR {number}",
        state=PullRequestState.CLOSED if merged_flag else PullRequestState.OPEN,
        user=user,
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


def test_cycle_time_median_and_p75():
    user = User(id=1, login="alice")
    owner = User(id=2, login="org")
    repo = Repository(id=1, name="repo", full_name="org/repo", owner=owner)

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pr1 = _make_pr(1, start, start + timedelta(hours=10), user, repo)
    pr2 = _make_pr(2, start + timedelta(hours=1), start + timedelta(hours=20), user, repo)
    pr3 = _make_pr(3, start + timedelta(hours=2), None, user, repo)  # open, ignored

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
    context = MetricContext(ledger=ledger, user_login="alice", start_date=start, end_date=start + timedelta(days=10))

    metric = CycleTime()
    res = metric.run(context)

    assert res.metric_slug == "cycle_time"
    assert res.details["merged_count"] == 2
    # durations: 10h, 19h => median 14.5, p75 between 19 and 14.5 -> should be 17.75
    assert res.details["median_hours"] == 14.5
    assert abs(res.details["p75_hours"] - 17.75) < 1e-6
