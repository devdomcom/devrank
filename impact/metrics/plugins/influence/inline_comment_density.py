from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class InlineCommentDensity(Metric):
    @property
    def slug(self) -> str:
        return "inline_comment_density"

    @property
    def name(self) -> str:
        return "Inline Comment Density"

    @property
    def description(self) -> str:
        return "Avg inline comments given per PR reviewed (measures review depth on others' PRs)."

    @property
    def category(self) -> str:
        return "review_impact"

    def run(self, context: MetricContext) -> MetricResult:
        # Get reviews given by the user in the time window
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        # Filter out self-reviews (where the PR author is the user)
        unique_prs: dict[int, object] = {}
        inline_counts: list[int] = []
        per_pr: list[dict[str, object]] = []

        for review in reviews:
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            # Skip self-reviews
            if pr.user.login == context.user_login:
                continue

            pr_num = pr.number
            if pr_num not in unique_prs:
                unique_prs[pr_num] = pr

        # For each unique PR reviewed, count inline comments given by the user
        for pr_num, pr in unique_prs.items():
            inlines = 0
            for rev in context.ledger.get_reviews_for_pr(pr_num):
                if rev.user.login != context.user_login:
                    continue
                comments = context.ledger.get_review_comments_for_review(rev.id)
                inlines += sum(1 for c in comments if c.position is not None)
            inline_counts.append(inlines)
            per_pr.append({"number": pr_num, "inline_comments": inlines, "files_changed": pr.changed_files})

        avg_density = sum(inline_counts) / len(inline_counts) if inline_counts else 0.0
        summary = f"Avg inline density: {avg_density:.1f} per reviewed PR (across {len(unique_prs)} PRs)."
        details: dict[str, object] = {
            "avg_inline_per_pr": avg_density,
            "reviewed_pr_count": len(unique_prs),
            "total_inline_comments": sum(inline_counts),
            "per_pr": per_pr,
            "reviewed_pr_numbers": list(unique_prs.keys()),
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if period_days < 14 and len(unique_prs) < 3:
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
