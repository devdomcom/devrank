import pytest
from datetime import timedelta

from impact.metrics.plugins.authored.unlinked_pr_rate import UnlinkedPRRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_unlinked_pr_rate():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, base_time=start, body="Fixes #123")
    pr2 = make_pr(2, user, repo, base_time=start, body="no link here")
    pr3 = make_pr(3, user, repo, base_time=start, body="Closes https://github.com/org/repo/issues/456")
    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)
    metric = UnlinkedPRRate()
    res = metric.run(context)
    assert res.metric_slug == "unlinked_pr_rate"
    assert res.details["total_prs"] == 3
    assert res.details["unlinked_count"] == 1
    assert res.details["unlinked_rate"] == pytest.approx(33.3, abs=0.1)
    assert 2 in res.details["unlinked_pr_numbers"]
    assert "no_data" not in res.details


def test_unlinked_pr_rate_no_data():
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    metric = UnlinkedPRRate()
    res = metric.run(context)
    assert res.details.get("no_data") is True
