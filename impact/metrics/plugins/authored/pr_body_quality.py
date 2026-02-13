from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import compute_pr_body_quality, filter_prs_for_contribution


class PRBodyQualityScore(Metric):
    @property
    def slug(self) -> str:
        return "pr_body_quality_score"

    @property
    def name(self) -> str:
        return "PR Body Quality Score"

    @property
    def description(self) -> str:
        return "Assesses PR body structure (markdown sections), length, and references to issues/PRs."

    @property
    def category(self) -> str:
        return "code_quality_size"

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)
        scores = []
        per_pr = []
        for pr in prs:
            score = compute_pr_body_quality(pr.body)
            scores.append(score)
            per_pr.append({"number": pr.number, "score": score, "length": len(pr.body or "")})
        avg_score = sum(scores) / len(scores) if scores else 0.0
        summary = f"Avg body quality score: {avg_score:.1f}/100 ({len(prs)} PRs analyzed)."
        details: dict[str, object] = {
            "average_score": avg_score,
            "pr_count": len(prs),
            "per_pr": per_pr,
            "analyzed_pr_numbers": [pr.number for pr in all_prs],
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if not prs or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
