import pytest
from datetime import timedelta

from impact.metrics.plugins.authored.commit_message_clarity import ConventionalCommitRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_commit_message_clarity():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # Conventional commits (feat/fix)
    c1 = make_commit("sha1", user, start, 1, message="feat: add new feature")
    c2 = make_commit("sha2", user, start + timedelta(hours=1), 1, message="fix: bug fix")
    # Non-conventional
    c3 = make_commit("sha3", user, start + timedelta(hours=2), 2, message="update code")
    pr = make_pr(1, user, repo, base_time=start)  # link

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
        commits=[c1, c2, c3],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ConventionalCommitRate()
    res = metric.run(context)

    assert res.metric_slug == "conventional_commit_rate"
    assert res.details["conventional_commit_rate"] == pytest.approx(66.7, abs=0.1)  # 2/3
    assert res.details["conventional_count"] == 2
    assert "Conventional commit rate" in res.summary
