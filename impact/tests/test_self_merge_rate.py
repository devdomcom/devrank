from datetime import timedelta

from impact.domain.models import PullRequestState, ReviewState, User
from impact.metrics.plugins.authored.self_merge_rate import SelfMergeRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_self_merge_rate():
    """% self-merges w/o approval (own+others); repo culture check."""
    author = make_user(id=1, login="alice")
    other = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START
    # PR1: author self-merges own w/ approval (good)
    pr1 = make_pr(1, author, repo, base_time=start, merged_delta_hours=10)
    pr1.merged_by = author  # override
    rev1 = make_review(1, 1, other, start + timedelta(hours=5), ReviewState.APPROVED)
    # PR2: author self-merges other PR w/o approval (bad)
    pr2 = make_pr(2, other, repo, base_time=start, merged_delta_hours=20)
    pr2.merged_by = author
    # PR3: other self-merge (for repo rate)
    pr3 = make_pr(3, other, repo, base_time=start, merged_delta_hours=30)
    pr3.merged_by = other
    rev3 = make_review(3, 3, author, start + timedelta(hours=25), ReviewState.APPROVED)
    # PR4: author self own no approval
    pr4 = make_pr(4, author, repo, base_time=start, merged_delta_hours=40)
    pr4.merged_by = author

    bundle = make_bundle(
        users=[author, other, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3, pr4],
        reviews=[rev1, rev3],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=5))

    metric = SelfMergeRate()
    res = metric.run(context)

    assert res.metric_slug == "self_merge_rate"
    # Engineer: PR1 (w/ approval), PR2/4 (no) -> 2/3=66.7%; repo 3/4=75%
    assert res.details["self_merge_rate"] == 66.66666666666666
    assert res.details["no_approval_count"] == 2
    assert res.details["engineer_merged_count"] == 3
    assert res.details["repo_self_merge_rate"] == 75.0
    assert "Self-merge rate:" in res.summary


def test_self_merge_rate_no_merges():
    """Edge: no merges -> 0."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    metric = SelfMergeRate()
    res = metric.run(context)
    assert res.details["self_merge_rate"] == 0.0
    assert res.details["engineer_merged_count"] == 0
