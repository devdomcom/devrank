"""Dedicated tests for CodingTimeToPR."""
from datetime import timedelta

import pytest
from impact.metrics.plugins.authored.coding_time_to_pr import CodingTimeToPR
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


class TestCodingTimeToPR:
    """Comprehensive tests for CodingTimeToPR (first commit → PR open)."""

    def test_coding_time(self):
        """Commits before PR open yield positive hours."""
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, user, repo, base_time=start + timedelta(hours=5))  # open at t=5h

        # First commit at t=1h
        c1 = make_commit("s1", user, start + timedelta(hours=1), 1)

        bundle = make_bundle(
            users=[user],
            repositories=[repo],
            pull_requests=[pr],
            commits=[c1],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = CodingTimeToPR().run(ctx)
        assert res.metric_slug == "coding_time_to_pr"
        assert res.details["median_hours"] > 0  # ~4h
        assert res.details["per_pr_hours"][0]["hours"] == pytest.approx(4.0)

    def test_no_pre_pr_commits(self):
        """Commit at/after open → 0 hours."""
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        pr = make_pr(1, user, repo, base_time=start)
        c1 = make_commit("s1", user, start + timedelta(hours=1), 1)  # after

        bundle = make_bundle(users=[user], repositories=[repo], pull_requests=[pr], commits=[c1])
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

        res = CodingTimeToPR().run(ctx)
        assert res.details["median_hours"] == 0.0

    def test_no_prs(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = CodingTimeToPR().run(ctx)
        assert res.details["no_data"] is True
