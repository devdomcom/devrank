from datetime import timedelta

from impact.metrics.plugins.influence.approval_to_merge_ratio import ApprovalToMergeRatio
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_approval_to_merge_high():
    """Approvals as final activity leading to merge."""
    user = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # Final approval
    pr1 = make_pr(1, author, repo, base_time=start, merged_delta_hours=10)
    review1 = make_review(
        10, 1, user, start + timedelta(hours=2), state="approved"
    )  # assume enum ok via make

    # Non-final: later CR
    pr2 = make_pr(2, author, repo, base_time=start, created_delta_hours=24, merged_delta_hours=50)
    review2 = make_review(20, 2, user, start + timedelta(hours=25), state="approved")
    later_cr = make_review(
        21,
        2,
        make_user(id=4, login="charlie"),
        start + timedelta(hours=30),
        state="changes_requested",
    )

    bundle = make_bundle(
        users=[user, author, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        reviews=[review1, review2, later_cr],
    )
    end = start + timedelta(days=3)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ApprovalToMergeRatio()
    res = metric.run(context)

    assert res.metric_slug == "approval_to_merge_ratio"
    assert res.details["total_approvals"] == 2
    assert res.details["final_approvals"] == 1
    assert res.details["ratio"] == 0.5


def test_approval_to_merge_none():
    """No final approvals."""
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user])
    context = make_context(bundle, user_login="alice")

    metric = ApprovalToMergeRatio()
    res = metric.run(context)

    assert res.details["total_approvals"] == 0
    assert res.details["final_approvals"] == 0
    assert res.details["ratio"] == 0.0
