from datetime import timedelta

from impact.metrics.plugins.authored.co_author_contribution_rate import CoAuthorContributionRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_co_author_contribution_rate():
    user = make_user(id=1, login="alice")
    coauthor = make_user(id=2, login="bob")
    other_pr_author = make_user(id=3, login="charlie")
    owner = make_user(id=4, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1 (alice): 3 commits, 1 by bob (inbound co=1)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    c1 = make_commit("sha1", user, start + timedelta(hours=1), 1)
    c2 = make_commit("sha2", coauthor, start + timedelta(hours=2), 1)  # co
    c3 = make_commit("sha3", user, start + timedelta(hours=3), 1)
    # PR2 (alice): all own
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=10)
    c4 = make_commit("sha4", user, start + timedelta(hours=11), 2)

    # Outbound: alice commit on charlie's PR3
    pr3 = make_pr(3, other_pr_author, repo, base_time=start, created_delta_hours=20)
    c5 = make_commit("sha5", user, start + timedelta(hours=21), 3)  # outbound

    bundle = make_bundle(
        users=[user, coauthor, other_pr_author, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        commits=[c1, c2, c3, c4, c5],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = CoAuthorContributionRate()
    res = metric.run(context)

    assert res.metric_slug == "co_author_contribution_rate"
    assert res.details["inbound_rate"] == 25.0
    assert res.details["outbound_rate"] == 25.0
    assert res.details["collab_per_week"] == 1.4  # 2 events / (10/7 weeks)
    assert "collab rate" in res.summary and "events/week" in res.summary
    assert any(p["co_authors"] == 1 for p in res.details["per_pr_co"])
