"""
Knowledge Sharing Index Metric

Measures how evenly code reviews distribute across the team (0-1 scale).
A value of 1.0 indicates perfectly equal distribution (everyone reviews equally),
while 0.0 indicates all reviews are done by a single person.

Uses Shannon entropy normalized by maximum possible entropy to compute
the evenness of review distribution across team members.

Critical in the AI era: Heavy AI bot reviewing can mask human knowledge
sharing patterns. This metric focuses on human reviewers only.
"""

import math
from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import is_bot_user


class KnowledgeSharingIndex(Metric):
    """
    Knowledge Sharing Index: how evenly reviews distribute across team (0-1).

    A team where one person does all the reviewing has index 0.
    A team where everyone reviews equally has index 1.

    This is a team-level descriptive metric (not a personal metric).
    It measures the health of knowledge distribution in the codebase.

    Uses Shannon entropy normalized by log(n) where n is the number of reviewers.
    """

    # Minimum reviewers for meaningful calculation
    MIN_REVIEWERS = 3
    MIN_REVIEWS = 5

    @property
    def slug(self) -> str:
        return "knowledge_sharing_index"

    @property
    def name(self) -> str:
        return "Knowledge Sharing Index"

    @property
    def description(self) -> str:
        return (
            "How evenly code reviews distribute across the team (0-1 scale). "
            "1.0 = perfect equality, 0.0 = one person does all reviews. "
            "Measures team knowledge distribution health."
        )

    @property
    def category(self) -> str:
        return "review_impact"

    @property
    def frameworks(self) -> list[str]:
        return ["Network"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        bundle = context.ledger.bundle

        # Collect review counts per human reviewer (excluding bots and self-reviews)
        reviewer_counts: dict[str, int] = defaultdict(int)
        total_reviews = 0
        bot_review_count = 0
        self_review_count = 0

        for review in bundle.reviews:
            # Filter to analysis window
            if context.start_date and review.submitted_at < context.start_date:
                continue
            if context.end_date and review.submitted_at > context.end_date:
                continue

            reviewer = review.user.login

            # Skip bot reviews
            if is_bot_user(review.user):
                bot_review_count += 1
                continue

            # Skip self-reviews (reviewer is PR author)
            pr = context.ledger.get_pr(review.pull_request_number)
            if pr and pr.user.login == reviewer:
                self_review_count += 1
                continue

            reviewer_counts[reviewer] += 1
            total_reviews += 1

        # Calculate Knowledge Sharing Index using normalized Shannon entropy
        sharing_index, calculation_details = self._calculate_sharing_index(
            reviewer_counts, total_reviews
        )

        # Build result
        num_reviewers = len(reviewer_counts)

        # No-data guards
        no_data = False
        no_data_reason = None

        if num_reviewers < self.MIN_REVIEWERS:
            no_data = True
            no_data_reason = f"Insufficient reviewers ({num_reviewers} < {self.MIN_REVIEWERS})"
        elif total_reviews < self.MIN_REVIEWS:
            no_data = True
            no_data_reason = f"Insufficient reviews ({total_reviews} < {self.MIN_REVIEWS})"

        # Build summary
        if no_data:
            summary = f"Insufficient data: {no_data_reason}"
        else:
            # Find most/least active reviewers
            sorted_reviewers = sorted(reviewer_counts.items(), key=lambda x: -x[1])
            top_reviewer = sorted_reviewers[0]
            bottom_reviewer = sorted_reviewers[-1]

            summary = (
                f"Knowledge Sharing Index: {sharing_index:.2f} "
                f"({num_reviewers} reviewers, {total_reviews} reviews). "
                f"Top: {top_reviewer[0]} ({top_reviewer[1]}), "
                f"Bottom: {bottom_reviewer[0]} ({bottom_reviewer[1]})"
            )

        details: dict[str, object] = {
            "sharing_index": round(sharing_index, 3),
            "reviewer_count": num_reviewers,
            "total_reviews": total_reviews,
            "bot_review_count": bot_review_count,
            "self_review_count": self_review_count,
            "reviewer_distribution": dict(sorted(reviewer_counts.items(), key=lambda x: -x[1])[:10]),
            "entropy": round(calculation_details.get("entropy", 0), 3),
            "max_entropy": round(calculation_details.get("max_entropy", 0), 3),
            "period_days": round(period_days, 1),
        }

        if no_data:
            details["no_data"] = True
            if no_data_reason:
                details["no_data_reason"] = no_data_reason

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )

    def _calculate_sharing_index(
        self, reviewer_counts: dict[str, int], total_reviews: int
    ) -> tuple[float, dict]:
        """
        Calculate Knowledge Sharing Index using normalized Shannon entropy.

        Shannon entropy: H = -sum(p_i * log(p_i))
        Normalized: H_norm = H / log(n) where n is number of reviewers

        Returns:
            (sharing_index, calculation_details)
        """
        n = len(reviewer_counts)

        if n == 0 or total_reviews == 0:
            return 0.0, {"entropy": 0.0, "max_entropy": 0.0}

        if n == 1:
            # Only one reviewer - index is 0 (no sharing)
            return 0.0, {"entropy": 0.0, "max_entropy": 0.0}

        # Calculate Shannon entropy
        entropy = 0.0
        for count in reviewer_counts.values():
            if count > 0:
                p = count / total_reviews
                entropy -= p * math.log(p)

        # Maximum entropy is log(n) for n reviewers with equal distribution
        max_entropy = math.log(n)

        # Normalized entropy (0-1 scale)
        sharing_index = entropy / max_entropy if max_entropy > 0 else 0.0

        return sharing_index, {
            "entropy": entropy,
            "max_entropy": max_entropy,
        }
