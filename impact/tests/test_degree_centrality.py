from datetime import timedelta

from impact.metrics.plugins.influence.degree_centrality import DegreeCentrality
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_degree_centrality_in_out():
    """Test unique collaborators from reviews given and received."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    charlie = make_user(id=3, login="charlie")
    owner = make_user(id=10, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # alice's PR reviewed by charlie (in)
    alice_pr = make_pr(100, alice, repo, base_time=start)
    # bob's PR reviewed by alice (out)
    bob_pr = make_pr(200, bob, repo, base_time=start)

    # charlie reviews alice's PR
    rev_in = make_review(1, 100, charlie, start + timedelta(hours=1))
    # alice reviews bob's PR
    rev_out = make_review(2, 200, alice, start + timedelta(hours=2))

    bundle = make_bundle(
        users=[alice, bob, charlie, owner],
        repositories=[repo],
        pull_requests=[alice_pr, bob_pr],
        reviews=[rev_in, rev_out],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DegreeCentrality()
    res = metric.run(context)

    assert res.metric_slug == "degree_centrality"
    assert res.details["unique_collaborators"] == 2
    assert res.details["in_degree"] == 1
    assert res.details["out_degree"] == 1
    assert "alice" not in res.details["collaborators"]
    assert set(res.details["collaborators"]) == {"bob", "charlie"}
    assert "unique collaborators" in res.summary


def test_degree_centrality_excludes_self_review():
    """Self-reviews should not count as collaboration."""
    alice = make_user(id=1, login="alice")
    owner = make_user(id=10, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    alice_pr = make_pr(100, alice, repo)
    # alice reviews her own PR (should be ignored)
    self_rev = make_review(1, 100, alice, DEFAULT_START)

    bundle = make_bundle(
        users=[alice, owner],
        repositories=[repo],
        pull_requests=[alice_pr],
        reviews=[self_rev],
    )
    context = make_context(bundle, user_login="alice")

    metric = DegreeCentrality()
    res = metric.run(context)

    assert res.details["unique_collaborators"] == 0
    assert res.details["in_degree"] == 0
    assert res.details["out_degree"] == 0


def test_degree_centrality_no_data():
    """Short period low activity sets no_data."""
    alice = make_user(id=1, login="alice")
    bundle = make_bundle(users=[alice])
    start = DEFAULT_START
    end = start + timedelta(days=5)  # short
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DegreeCentrality()
    res = metric.run(context)

    assert res.details.get("no_data") is True


def test_degree_centrality_zero_collaborators_long_period():
    """Long period (>=21 days) with zero collaborators must set no_data."""
    alice = make_user(id=1, login="alice")
    owner = make_user(id=10, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    # Alice has a PR but no reviews on it, and she gave no reviews
    alice_pr = make_pr(100, alice, repo)

    bundle = make_bundle(
        users=[alice, owner],
        repositories=[repo],
        pull_requests=[alice_pr],
        reviews=[],
    )
    start = DEFAULT_START
    end = start + timedelta(days=30)  # >= 21 days
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = DegreeCentrality()
    res = metric.run(context)

    assert res.details["unique_collaborators"] == 0
    assert res.details.get("no_data") is True
    assert res.details.get("no_data_reason") == "No review collaborations in period"
