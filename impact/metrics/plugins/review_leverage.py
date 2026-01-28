from typing import Dict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult
from datetime import timedelta


class ReviewLeverage(Metric):
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
        change_requests = [r for r in reviews if r.state.value == "changes_requested"]

        if not change_requests:
            summary = "No change requests made."
            details = {}
        else:
            effective_changes = sum(1 for r in change_requests if self._is_effective_change_request(r, context))
            total_change_requests = len(change_requests)
            percentage = (effective_changes / total_change_requests) * 100 if total_change_requests > 0 else 0

            # Track whether reviewed PRs were updated or merged after the review
            updated_after_review = 0
            merged_after_review = 0
            for r in reviews:
                pr = context.ledger.get_pr(r.pull_request_number)
                timeline = context.ledger.get_timeline_for_pr(r.pull_request_number)
                if any(evt.created_at > r.submitted_at for evt in timeline):
                    updated_after_review += 1
                merged_flag = False
                if pr and pr.merged and (not pr.merged_at or pr.merged_at >= r.submitted_at):
                    merged_flag = True
                else:
                    merged_flag = any(evt.event == "merged" and evt.created_at >= r.submitted_at for evt in timeline)
                if merged_flag:
                    merged_after_review += 1

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
