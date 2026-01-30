from datetime import timedelta
from typing import Dict

from impact.metrics.base import Metric
from impact.metrics.utils import has_pr_event_after, is_pr_merged_after, is_change_request
from impact.domain.models import MetricContext, MetricResult


class ReviewLeverage(Metric):
    """
    Measures how effective a reviewer's change requests are at driving improvements.

    This metric analyzes reviews where the user requested changes (formally or via
    inline comments) and determines how many of those requests led to actual code
    changes by the PR author within a 72-hour window. A higher effectiveness rate
    indicates impactful code review feedback.

    An "effective" change request is one where:
        - The PR was eventually merged
        - The PR author committed changes after the review
        - No other reviews occurred between the change request and the follow-up commit
        - If inline comments targeted specific files, those files were modified

    Details returned:
        - total_reviews: Total reviews by the user
        - change_requests: Number of change-request reviews
        - effective_changes: Change requests that led to follow-up commits
        - effectiveness_percentage: Percentage of effective change requests
        - updated_after_review: PRs with activity after review
        - merged_after_review: PRs merged after review
    """

    @property
    def slug(self) -> str:
        return "review_leverage"

    @property
    def name(self) -> str:
        return "Review Leverage"



    def _is_effective_change_request(self, review, context):
        pr = context.ledger.get_pr(review.pull_request_number)
        if not pr or not pr.merged:
            return False
        # Time window
        window_end = pr.merged_at or pr.closed_at
        if window_end is None:
            window_end = review.submitted_at + timedelta(hours=72)
        max_time = review.submitted_at + timedelta(hours=72)
        window_end = min(window_end, max_time)

        # review comments for file paths
        review_comment_paths = {
            c.path for c in context.ledger.get_review_comments_for_review(review.id) if c.path
        }

        # Check commits after the review within window by PR author, respecting latest-review gate and path overlap
        commits = context.ledger.get_commits_for_pr(review.pull_request_number)
        pr_author = pr.user.login
        other_reviews = [
            r for r in context.ledger.get_reviews_for_pr(review.pull_request_number)
            if r.submitted_at > review.submitted_at
        ]
        effective = False
        for c in commits:
            if c.date <= review.submitted_at or c.date > window_end:
                continue
            if c.author.login != pr_author:
                continue
            # If another review happened before this commit, don't attribute to this review
            if any(r.submitted_at <= c.date for r in other_reviews):
                continue
            if review_comment_paths:
                pr_files = {f.filename for f in context.ledger.get_files_for_pr(pr.number)}
                if not pr_files.intersection(review_comment_paths):
                    continue
            effective = True
            break
        return effective

    def run(self, context: MetricContext) -> MetricResult:
        reviews = context.ledger.get_reviews_for_user(context.user_login, context.start_date, context.end_date)
        # Treat formal change requests OR inline-comment reviews as “change requests” for leverage.
        change_requests = [r for r in reviews if is_change_request(r, context.ledger)]

        if not change_requests:
            summary = "No change requests made."
            details = {}
        else:
            effective_changes = sum(1 for r in change_requests if self._is_effective_change_request(r, context))
            total_change_requests = len(change_requests)
            percentage = (effective_changes / total_change_requests) * 100 if total_change_requests > 0 else 0

            # Track whether reviewed PRs were updated or merged after the review
            updated_after_review = sum(1 for r in reviews if has_pr_event_after(context.ledger, r.pull_request_number, r.submitted_at))
            merged_after_review = sum(1 for r in reviews if is_pr_merged_after(context.ledger, r.pull_request_number, r.submitted_at))

            summary = (
                f"{len(reviews)} PRs reviewed, {effective_changes} effective change requests "
                f"out of {total_change_requests} ({percentage:.1f}%). "
                f"Updated after review: {updated_after_review}, merged after review: {merged_after_review}"
            )

            details = {
                "total_reviews": len(reviews),
                "change_requests": total_change_requests,
                "effective_changes": effective_changes,
                "effectiveness_percentage": percentage,
                "updated_after_review": updated_after_review,
                "merged_after_review": merged_after_review,
                "change_request_details": [
                    {
                        "pr_number": r.pull_request_number,
                        "effective": self._is_effective_change_request(r, context)
                    } for r in change_requests
                ]
            }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details
        )
