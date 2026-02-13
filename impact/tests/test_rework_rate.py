"""Dedicated tests for ReworkRate."""
from datetime import timedelta

import pytest
from impact.metrics.plugins.authored.rework_rate import ReworkRate
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


class TestReworkRate:
    """Comprehensive tests for ReworkRate (self-rework on <=21d prior lines via hunks)."""

    def test_rework_overlap(self):
        """Prior file change overlaps current → rework counted."""
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START
        # Prior PR: modified file.py
        prior_pr = make_pr(10, user, repo, base_time=start - timedelta(days=10))
        prior_patch = "@@ -10,5 +10,5 @@\n old\n+new\n"
        prior_file = make_file("sha1", "file.py", pr_number=10, patch=prior_patch)
        # Current PR: overlaps same lines
        pr = make_pr(1, user, repo, base_time=start)
        patch = "@@ -10,5 +10,5 @@\n old\n+newer\n"
        file = make_file("sha2", "file.py", pr_number=1, patch=patch)

        bundle = make_bundle(
            users=[user],
            repositories=[repo],
            pull_requests=[prior_pr, pr],
            files=[prior_file, file],
        )
        ctx = make_context(bundle, user_login="alice", start_date=start - timedelta(days=30), end_date=start + timedelta(days=1))

        res = ReworkRate().run(ctx)
        assert res.metric_slug == "rework_rate"
        assert res.details["rework_rate"] > 0
        assert res.details["reworked_lines"] > 0

    def test_short_period_no_data(self):
        """Period <21d → no_data."""
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice", start_date=DEFAULT_START, end_date=DEFAULT_START + timedelta(days=10))  # short

        res = ReworkRate().run(ctx)
        assert res.details.get("no_data") is True

    def test_no_prs(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = ReworkRate().run(ctx)
        assert res.details["no_data"] is True
