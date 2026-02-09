from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution


class InlineCommentDensity(Metric):
    @property
    def slug(self) -> str:
        return "inline_comment_density"

    @property
    def name(self) -> str:
        return "Inline Comment Density"

    @property
    def description(self) -> str:
        return "Avg inline comments (path/position) per reviewed PR/files (depth measure)."

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        reviewed_prs = [pr for pr in filter_prs_for_contribution(prs) if context.ledger.get_reviews_for_pr(pr.number)]
        inline_counts = []
        per_pr = []
        for pr in reviewed_prs:
            # Inline: review comments with position/path (no dup iterations)
            inlines = 0
            for rev in context.ledger.get_reviews_for_pr(pr.number):
                comments = context.ledger.get_review_comments_for_review(rev.id)
                inlines += sum(1 for c in comments if c.position is not None)
            inline_counts.append(inlines)
            per_pr.append({"number": pr.number, "inline_comments": inlines, "files_changed": pr.changed_files})
        avg_density = sum(inline_counts) / len(inline_counts) if inline_counts else 0.0
        summary = f"Avg inline density: {avg_density:.1f} per reviewed PR (across {len(reviewed_prs)} PRs)."
        details: dict[str, object] = {
            "avg_inline_per_pr": avg_density,
            "reviewed_pr_count": len(reviewed_prs),
            "total_inline_comments": sum(inline_counts),
            "per_pr": per_pr,
            "analyzed_pr_numbers": [pr.number for pr in prs],
        }
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
