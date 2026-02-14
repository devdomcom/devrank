from datetime import timedelta

from impact.domain.models import PullRequestState
from impact.metrics.plugins.authored.abandoned_pr_rate import AbandonedPRRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_abandoned_pr_rate():
    """% stale open PRs (>=30d)."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START
    # Open stale
    pr1 = make_pr(1, user, repo, created_at=start - timedelta(days=40), merged_at=None)
    pr1.state = PullRequestState.OPEN
    # Open fresh
    pr2 = make_pr(2, user, repo, created_at=start - timedelta(days=10), merged_at=None)
    pr2.state = PullRequestState.OPEN
    # Closed (ignored)
    pr3 = make_pr(3, user, repo, merged_delta_hours=5)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
    )
    end = start + timedelta(days=1)
    context = make_context(bundle, user_login="alice", start_date=start - timedelta(days=50), end_date=end)

    metric = AbandonedPRRate()
    res = metric.run(context)

    assert res.metric_slug == "abandoned_pr_rate"
    # 1/2 open stale -> 50%; weighted_score == rate (no period scaling)
    assert res.details["abandoned_rate"] == 50.0
    assert res.details["weighted_score"] == 50.0
    assert res.details["open_pr_count"] == 2
    assert "Abandoned PR rate:" in res.summary


def test_abandoned_pr_rate_no_open():
    """Edge: no open -> 0."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    metric = AbandonedPRRate()
    res = metric.run(context)
    assert res.details["abandoned_rate"] == 0.0
    assert res.details["open_pr_count"] == 0
