from datetime import timedelta

from impact.metrics.plugins.influence.betweenness_centrality import BetweennessCentrality
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


def test_betweenness_centrality_bridge_node():
    """Bridge node in a line graph should have high normalized betweenness."""
    a = make_user(id=1, login="a")
    b = make_user(id=2, login="b")
    c = make_user(id=3, login="c")
    owner = make_user(id=10, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr_a = make_pr(100, a, repo, base_time=start)
    pr_c = make_pr(200, c, repo, base_time=start)

    # b reviews a's PR and c's PR -> b is the bridge
    rev_b_reviews_a = make_review(1, 100, b, start + timedelta(hours=1))
    rev_b_reviews_c = make_review(2, 200, b, start + timedelta(hours=2))

    bundle = make_bundle(
        users=[a, b, c, owner],
        repositories=[repo],
        pull_requests=[pr_a, pr_c],
        reviews=[rev_b_reviews_a, rev_b_reviews_c],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="b", start_date=start, end_date=end)

    metric = BetweennessCentrality()
    res = metric.run(context)

    assert res.metric_slug == "betweenness_centrality"
    assert res.details["graph_size"] == 3
    # In a 3-node line, the center has raw betweenness 1.0
    # Normalized = 1.0 / ((3-1)*(3-2)/2) = 1.0
    assert res.details["normalized_betweenness"] >= 0.99
    assert res.details["raw_betweenness"] == 1.0
    assert res.details.get("no_data") is not True


def test_betweenness_centrality_star_graph_center():
    """Center of a star graph should have the highest betweenness."""
    center = make_user(id=1, login="center")
    leaves = [make_user(id=i + 10, login=f"leaf{i}") for i in range(4)]
    owner = make_user(id=100, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    prs = [make_pr(100 + i, leaf, repo, base_time=start) for i, leaf in enumerate(leaves)]

    # Center reviews every leaf's PR
    reviews = [
        make_review(1000 + i, pr.number, center, start + timedelta(hours=i + 1))
        for i, pr in enumerate(prs)
    ]

    bundle = make_bundle(
        users=[center, owner] + leaves,
        repositories=[repo],
        pull_requests=prs,
        reviews=reviews,
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="center", start_date=start, end_date=end)

    metric = BetweennessCentrality()
    res = metric.run(context)

    assert res.details["graph_size"] == 5  # center + 4 leaves
    # Center has very high betweenness (all leaf-to-leaf paths go through it)
    assert res.details["normalized_betweenness"] > 0.9
    assert res.details.get("no_data") is not True


def test_betweenness_centrality_isolated_user():
    """User with PRs but no review activity should have zero betweenness and no_data."""
    alice = make_user(id=1, login="alice")
    owner = make_user(id=10, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    alice_pr = make_pr(100, alice, repo)

    bundle = make_bundle(
        users=[alice, owner],
        repositories=[repo],
        pull_requests=[alice_pr],
        reviews=[],
    )
    start = DEFAULT_START
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = BetweennessCentrality()
    res = metric.run(context)

    assert res.details["normalized_betweenness"] == 0.0
    assert res.details["raw_betweenness"] == 0.0
    assert res.details["graph_size"] == 0
    assert res.details.get("no_data") is True
    assert res.details.get("no_data_reason") == "No review interactions in period"


def test_betweenness_centrality_short_period_low_activity():
    """Short period with low activity sets no_data."""
    alice = make_user(id=1, login="alice")
    bob = make_user(id=2, login="bob")
    repo = make_repo(id=1, name="repo", owner=alice)

    start = DEFAULT_START
    pr_bob = make_pr(200, bob, repo, base_time=start)
    rev = make_review(1, 200, alice, start + timedelta(hours=1))

    bundle = make_bundle(
        users=[alice, bob],
        repositories=[repo],
        pull_requests=[pr_bob],
        reviews=[rev],
    )
    # Very short period + only 1 collaborator
    end = start + timedelta(days=5)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = BetweennessCentrality()
    res = metric.run(context)

    assert res.details.get("no_data") is True
