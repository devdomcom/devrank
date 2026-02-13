from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_rework_rate, filter_prs_for_contribution


class ReworkRate(Metric):
    """% code changes touching author's prior 21d lines (self-rework on recent code)."""

    @property
    def slug(self) -> str:
        return "rework_rate"

    @property
    def name(self) -> str:
        return "Rework Rate"

    @property
    def description(self) -> str:
        return "% changes that rework own code from prior 21 days (DORA-style effort waste)."

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        # All PRs (rework can occur in open/closed; filter drafts)
        filtered_prs = filter_prs_for_contribution(prs, exclude_drafts=True, only_merged=False)

        details = compute_rework_rate(
            context.ledger, context.user_login, filtered_prs, window_days=21
        )

        rate = details["rework_rate"]
        summary = f"Rework rate: {rate:.1f}% ({details['reworked_lines']}/{details['total_changed']} lines; {details['pr_count']} PRs)."
        if details.get("no_data"):
            summary += " (non-computable: short period or no patch data)."
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
