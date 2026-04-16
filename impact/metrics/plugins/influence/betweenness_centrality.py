from collections import defaultdict, deque

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


def _brandes_betweenness(adj: dict[str, set[str]]) -> dict[str, float]:
    """Compute raw betweenness centrality using Brandes algorithm (undirected unweighted).

    Multiple reviews between the same pair do not create weighted edges or
    shorten path length — betweenness counts hops, not interaction strength.
    """
    betweenness: dict[str, float] = defaultdict(float)
    nodes = [n for n in adj if adj.get(n)]
    if not nodes:
        return {}

    for s in nodes:
        stack: list[str] = []
        pred: dict[str, list[str]] = {w: [] for w in nodes}
        dist: dict[str, int] = {w: -1 for w in nodes}
        sigma: dict[str, float] = {w: 0.0 for w in nodes}
        dist[s] = 0
        sigma[s] = 1.0
        queue: deque[str] = deque([s])
        while queue:
            v = queue.popleft()
            stack.append(v)
            for w in adj.get(v, ()):
                if dist[w] < 0:
                    dist[w] = dist[v] + 1
                    queue.append(w)
                if dist[w] == dist[v] + 1:
                    sigma[w] += sigma[v]
                    pred[w].append(v)
        delta: dict[str, float] = {w: 0.0 for w in nodes}
        while stack:
            w = stack.pop()
            for v in pred[w]:
                if sigma[w] > 0:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
            if w != s:
                betweenness[w] += delta[w]
    # Undirected graph: each shortest path counted twice
    for v in list(betweenness.keys()):
        betweenness[v] /= 2.0
    return dict(betweenness)


class BetweennessCentrality(Metric):
    """Betweenness centrality in the review collaboration network.

    Measures the extent to which a developer lies on shortest paths between
    other developers (i.e., bridges disconnected parts of the review graph).
    Uses Brandes algorithm on an undirected unweighted graph built from
    review interactions (reviewer <-> PR author).
    """

    @property
    def slug(self) -> str:
        return "betweenness_centrality"

    @property
    def name(self) -> str:
        return "Betweenness Centrality"

    @property
    def description(self) -> str:
        return "How much a developer bridges disconnected parts of the review network (normalized 0-1)."

    @property
    def category(self) -> str:
        return "review_impact"

    @property
    def frameworks(self) -> list[str]:
        return ["Network"]

    def run(self, context: MetricContext) -> MetricResult:
        bundle = context.ledger.bundle

        # Build undirected review collaboration graph (exclude self-reviews)
        # Uses sets → unweighted edges: multiple reviews between same pair
        # do not affect shortest-path hop count.
        adj: dict[str, set[str]] = defaultdict(set)
        for review in bundle.reviews:
            if context.start_date and review.submitted_at < context.start_date:
                continue
            if context.end_date and review.submitted_at > context.end_date:
                continue
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            reviewer = review.user.login
            author = pr.user.login
            if reviewer == author:
                continue
            adj[reviewer].add(author)
            adj[author].add(reviewer)

        # All nodes participating in at least one review interaction (full graph)
        nodes = [n for n in adj if adj[n]]
        n = len(nodes)

        raw_betweenness = _brandes_betweenness(adj)
        if n > 2:
            norm_factor = (n - 1) * (n - 2) / 2.0
            normalized_betweenness = {
                v: raw_betweenness.get(v, 0.0) / norm_factor for v in nodes
            }
        else:
            normalized_betweenness = {v: 0.0 for v in nodes}

        user_betweenness = normalized_betweenness.get(context.user_login, 0.0)
        raw_user = raw_betweenness.get(context.user_login, 0.0)

        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        details: dict[str, object] = {
            "normalized_betweenness": round(user_betweenness, 4),
            "raw_betweenness": round(raw_user, 2),
            "graph_size": n,
            "period_days": round(period_days, 1),
            "collaborators": sorted(nodes)[:50],
        }

        if n < 3 or (user_betweenness == 0 and period_days < 21):
            details["no_data"] = True

        if n == 0:
            details["no_data"] = True
            details["no_data_reason"] = "No review interactions in period"

        summary = (
            f"Betweenness centrality: {user_betweenness:.3f} "
            f"(bridges {n} nodes in review network)."
        )

        return MetricResult(
            metric_slug=self.slug, summary=summary, details=details
        )
