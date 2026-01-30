from datetime import timedelta
from typing import Dict, List

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


class CycleTime(Metric):
    @property
    def slug(self) -> str:
        return "cycle_time"

    @property
    def name(self) -> str:
        return "Cycle Time"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)
        merged_prs = [pr for pr in prs if pr.merged and pr.merged_at]

        durations_hours: List[float] = []
        per_pr = []
        for pr in merged_prs:
            delta: timedelta = pr.merged_at - pr.created_at
            hours = delta.total_seconds() / 3600
            durations_hours.append(hours)
            per_pr.append({"number": pr.number, "hours": hours})

        median = _percentile(durations_hours, 0.5) if durations_hours else 0.0
        p75 = _percentile(durations_hours, 0.75) if durations_hours else 0.0

        summary = f"{len(merged_prs)} merged PRs. Median: {median:.2f}h, p75: {p75:.2f}h."
        details: Dict[str, object] = {
            "merged_count": len(merged_prs),
            "median_hours": median,
            "p75_hours": p75,
            "per_pr_hours": per_pr,
        }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
