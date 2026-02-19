from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import get_pr_size_category


class DeliveryVolume(Metric):
    """
    Measures absolute output volume: non-trivial merged PRs per week.

    Unlike pr_throughput (which measures merge ratio), this metric captures
    how much an engineer actually ships. Trivial PRs (<10 lines changed)
    are excluded to prevent gaming via PR splitting.

    Details returned:
        - total_merged: Total merged authored PRs in window
        - nontrivial_merged: Merged PRs excluding trivial ones
        - trivial_excluded: Count of trivial PRs filtered out
        - period_weeks: Assessment period in weeks
        - merged_per_week: Non-trivial merged PRs per week (scored key)
    """

    @property
    def slug(self) -> str:
        return "delivery_volume"

    @property
    def name(self) -> str:
        return "Delivery Volume"

    @property
    def description(self) -> str:
        return "Measures non-trivial merged PRs per week as an absolute output volume signal."

    @property
    def category(self) -> str:
        return "delivery_velocity"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        merged = [pr for pr in prs if pr.merged]
        total_merged = len(merged)

        # Filter out trivial PRs (<10 lines changed)
        nontrivial = []
        trivial_count = 0
        for pr in merged:
            changes = pr.additions + pr.deletions
            if get_pr_size_category(changes) == "trivial":
                trivial_count += 1
            else:
                nontrivial.append(pr)

        nontrivial_merged = len(nontrivial)

        if context.start_date and context.end_date:
            period_days = (context.end_date - context.start_date).total_seconds() / 86400
        else:
            period_days = 30.0

        period_weeks = period_days / 7.0
        merged_per_week = nontrivial_merged / period_weeks if period_weeks > 0 else 0.0

        summary = (
            f"{nontrivial_merged} non-trivial merged PRs in {period_weeks:.1f} weeks "
            f"({merged_per_week:.2f}/week)"
        )
        details: dict[str, object] = {
            "total_merged": total_merged,
            "nontrivial_merged": nontrivial_merged,
            "trivial_excluded": trivial_count,
            "period_weeks": round(period_weeks, 2),
            "merged_per_week": round(merged_per_week, 3),
            "nontrivial_pr_numbers": [pr.number for pr in nontrivial],
        }

        # No-data guard: short period + low count
        if nontrivial_merged == 0 or (period_days < 14 and nontrivial_merged < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
