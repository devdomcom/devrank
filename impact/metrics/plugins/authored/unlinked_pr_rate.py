from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
import re

from impact.metrics.utils import filter_prs_for_contribution


class UnlinkedPRRate(Metric):
    @property
    def slug(self) -> str:
        return "unlinked_pr_rate"

    @property
    def name(self) -> str:
        return "Unlinked PR Rate"

    @property
    def description(self) -> str:
        return "% PRs without issue tracker links."

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        link_pattern = re.compile(r"(?i)(?:fixes|closes|resolves)\s*#?\d+|#\d+|github.com/.+/issues/\d+")
        unlinked = []
        linked = []
        for pr in prs:
            text = (pr.title or "") + " " + (pr.body or "")
            if link_pattern.search(text):
                linked.append(pr)
            else:
                unlinked.append(pr)
        total = len(prs)
        rate = (len(unlinked) / total * 100) if total else 0.0
        summary = f"{rate:.1f}% unlinked PRs ({len(unlinked)}/{total})."
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 30
        details: dict[str, object] = {
            "unlinked_rate": rate,
            "unlinked_count": len(unlinked),
            "linked_count": len(linked),
            "total_prs": total,
            "unlinked_pr_numbers": [p.number for p in unlinked],
            "period_days": period_days,
        }
        if total == 0 or (period_days < 14 and total < 3):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
