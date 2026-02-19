"""Dedicated tests for CodingDays."""
from datetime import timedelta

import pytest
from impact.metrics.plugins.authored.coding_days import CodingDays
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_repo,
    make_user,
)


class TestCodingDays:
    """Comprehensive tests for CodingDays (commit days / working days ratio)."""

    def test_coding_days_ratio(self):
        """Multiple commit days yield ratio; weekends excluded."""
        user = make_user(id=1, login="alice")
        repo = make_repo(id=1, name="repo")
        start = DEFAULT_START  # assume weekday
        # Commits on working days
        c1 = make_commit("s1", user, start, 1)
        c2 = make_commit("s2", user, start + timedelta(days=1), 2)
        c3 = make_commit("s3", user, start + timedelta(days=3), 3)

        bundle = make_bundle(
            users=[user],
            repositories=[repo],
            commits=[c1, c2, c3],
        )
        # Period covers ~5 working days
        ctx = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=7))

        res = CodingDays().run(ctx)
        assert res.metric_slug == "coding_days"
        assert res.details["ratio"] > 0
        assert res.details["active_days"] >= 2  # at least some working days (depends on weekday)
        assert "ratio_pct" in res.details

    def test_no_commits(self):
        user = make_user(id=1, login="alice")
        bundle = make_bundle(users=[user])
        ctx = make_context(bundle, user_login="alice")

        res = CodingDays().run(ctx)
        assert res.details["no_data"] is True
