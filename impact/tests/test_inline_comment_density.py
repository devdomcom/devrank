from datetime import timedelta

from impact.metrics.plugins.authored.inline_comment_density import InlineCommentDensity
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


def test_inline_comment_density():
    user = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: reviewed with 2 inline comments (depth)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    rev1 = make_review(101, 1, reviewer, start + timedelta(hours=1))
    c1 = make_comment(201, 1, reviewer, start + timedelta(hours=2), review_id=101, position=5, path="file.py")
    c2 = make_comment(202, 1, reviewer, start + timedelta(hours=3), review_id=101, position=10, path="file.py")
    # PR2: reviewed but no inline (general comment only)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=10)
    rev2 = make_review(102, 2, reviewer, start + timedelta(hours=11))
    c3 = make_comment(203, 2, reviewer, start + timedelta(hours=12), review_id=102)  # no position

    bundle = make_bundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        reviews=[rev1, rev2],
        comments=[c1, c2, c3],  # review_comments via ledger
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = InlineCommentDensity()
    res = metric.run(context)

    assert res.metric_slug == "inline_comment_density"
    assert res.details["reviewed_pr_count"] == 2
    assert res.details["avg_inline_per_pr"] == 1.0  # (2 + 0)/2
    assert res.details["total_inline_comments"] == 2
    assert any(p["inline_comments"] == 2 for p in res.details["per_pr"])
    assert "Avg inline density" in res.summary
