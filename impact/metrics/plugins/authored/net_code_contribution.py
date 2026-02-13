from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class NetCodeContribution(Metric):
    @property
    def slug(self) -> str:
        return "net_code_contribution"

    @property
    def name(self) -> str:
        return "Net Code Contribution"

    @property
    def description(self) -> str:
        return "Net lines (add - del) + add/del ratio over period (create/maintain/refactor signal)."

    @property
    def category(self) -> str:
        return "descriptive"

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        total_add = 0
        total_del = 0
        per_pr = []
        for pr in prs:
            # Use PR-level for net (or sum files; PR has it)
            add = pr.additions or 0
            del_ = pr.deletions or 0
            net = add - del_
            ratio = (add / del_) if del_ > 0 else (add / 1.0)  # avoid div0; high if no del
            total_add += add
            total_del += del_
            per_pr.append({"number": pr.number, "additions": add, "deletions": del_, "net": net, "ratio": ratio})
        total_net = total_add - total_del
        overall_ratio = (total_add / total_del) if total_del > 0 else (total_add / 1.0)
        # Period norm: net/month, ratio
        start = context.start_date or context.end_date or None
        end = context.end_date
        period_days = (end - start).days if start and end else 30
        months = max(1, period_days / 30.0)
        net_per_month = total_net / months
        summary = f"Net code: {total_net} lines ({net_per_month:.1f}/month); add/del ratio {overall_ratio:.1f}."
        details: dict[str, object] = {
            "net_lines": total_net,
            "net_per_month": net_per_month,
            "add_to_del_ratio": overall_ratio,
            "total_additions": total_add,
            "total_deletions": total_del,
            "period_days": period_days,
            "pr_count": len(prs),
            "per_pr": per_pr,
            "analyzed_pr_numbers": [p.number for p in all_prs],
        }
        if not prs:
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
