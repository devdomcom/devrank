import re

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


def _score_comment(body: str) -> int:
    """Score a review comment 0-100 based on substance indicators."""
    if not body or not body.strip():
        return 0
    body = body.strip()
    score = 0

    # Length contribution (up to 40 points)
    length = len(body)
    if length >= 200:
        score += 40
    elif length >= 100:
        score += 30
    elif length >= 50:
        score += 20
    elif length >= 20:
        score += 10

    # Code suggestions / code blocks (up to 25 points)
    if "```" in body:
        score += 25
    elif "`" in body:
        score += 10

    # Questions (shows engagement; up to 15 points)
    if "?" in body:
        score += 15

    # References to docs/links/issues (up to 10 points)
    if re.search(r"https?://", body) or re.search(r"#\d+", body):
        score += 10

    # Actionable language (up to 10 points)
    actionable = [
        "should", "could", "consider", "suggest", "recommend",
        "instead", "rather", "prefer", "nit:", "todo:",
    ]
    if any(w in body.lower() for w in actionable):
        score += 10

    return min(score, 100)


class ReviewCommentSubstance(Metric):
    @property
    def slug(self) -> str:
        return "review_comment_substance"

    @property
    def name(self) -> str:
        return "Review Comment Substance"

    @property
    def description(self) -> str:
        return "Avg quality score of review comments (length, code suggestions, questions, refs)."

    @property
    def category(self) -> str:
        return "review_impact"

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(
            context.user_login, context.start_date, context.end_date
        )

        scores: list[int] = []
        per_comment: list[dict] = []
        reviewed_prs: set[int] = set()

        for review in reviews:
            pr = context.ledger.get_pr(review.pull_request_number)
            if not pr:
                continue
            # Skip self-reviews
            if pr.user.login == context.user_login:
                continue
            reviewed_prs.add(pr.number)

            # Score the review body itself
            if review.body and review.body.strip():
                s = _score_comment(review.body)
                scores.append(s)
                per_comment.append({
                    "pr_number": pr.number,
                    "review_id": review.id,
                    "score": s,
                    "length": len(review.body),
                    "type": "review_body",
                })

            # Score inline review comments
            comments = context.ledger.get_review_comments_for_review(review.id)
            for c in comments:
                s = _score_comment(c.body)
                scores.append(s)
                per_comment.append({
                    "pr_number": pr.number,
                    "review_id": review.id,
                    "score": s,
                    "length": len(c.body),
                    "type": "inline_comment",
                })

        avg_score = sum(scores) / len(scores) if scores else 0.0

        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        summary = (
            f"Avg review comment substance: {avg_score:.1f}/100 "
            f"({len(scores)} comments across {len(reviewed_prs)} PRs)."
        )
        details: dict[str, object] = {
            "avg_substance_score": avg_score,
            "total_comments": len(scores),
            "reviewed_pr_count": len(reviewed_prs),
            "period_days": round(period_days, 1),
            "per_comment": per_comment[:50],  # cap output size
        }
        if period_days < 14 and len(scores) < 3:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
