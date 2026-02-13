from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_immediate_approval


class FirstTimeApprovalRate(Metric):
    """Percentage of PRs receiving immediate approval without CRs or inline comments."""

    @property
    def slug(self) -> str:
        return "first_time_approval_rate"

    @property
    def name(self) -> str:
        return "First-Time Approval Rate"

    @property
    def description(self) -> str:
        return "% PRs approved on first review (no prior CRs/inline comments)."

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        merged_prs = filter_prs_for_contribution(
            prs, exclude_drafts=True, only_merged=True
        )

        immediate_count = 0
        per_pr = []
        for pr in merged_prs:
            is_first_time = is_immediate_approval(
                context.ledger, pr.number, context.user_login
            )
            if is_first_time:
                immediate_count += 1
            per_pr.append(
                {
                    "number": pr.number,
                    "immediate_approval": is_first_time,
                }
            )

        total = len(merged_prs)
        rate = immediate_count / total if total else 0.0

        summary = f"{immediate_count}/{total} PRs first-time approved ({rate:.2f} rate)."
        details: dict[str, object] = {
            "rate": rate,
            "immediate_count": immediate_count,
            "merged_pr_count": total,
            "per_pr": per_pr,
        }
        if not merged_prs:
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
