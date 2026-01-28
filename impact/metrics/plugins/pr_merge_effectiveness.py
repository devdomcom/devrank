from datetime import timedelta
from typing import Any, Dict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


class PRMergeEffectiveness(Metric):
    @property
    def slug(self) -> str:
        return "pr_merge_effectiveness"

    @property
    def name(self) -> str:
        return "PR Merge Effectiveness"

    def run(self, context: MetricContext) -> MetricResult:
        prs = context.ledger.get_prs_for_user(context.user_login, context.start_date, context.end_date)
        merged_prs = [pr for pr in prs if pr.merged and pr.merged_at]

        if not merged_prs:
            summary = "No PRs merged in the period."
            details = {}
        else:
            count = len(merged_prs)
            merge_times = []
            back_forths = []

            for pr in merged_prs:
                if pr.merged_at and pr.created_at:
                    merge_time = pr.merged_at - pr.created_at
                    merge_times.append(merge_time.total_seconds() / 3600)  # hours

                reviews = context.ledger.get_reviews_for_pr(pr.number)
                comments = context.ledger.get_comments_for_pr(pr.number)
                back_forth = len(reviews) + len(comments)
                back_forths.append(back_forth)

            avg_merge_time = sum(merge_times) / len(merge_times) if merge_times else 0
            avg_back_forth = sum(back_forths) / len(back_forths) if back_forths else 0

            summary = f"{count} PRs merged, average merge time: {avg_merge_time:.1f} hours, average back-and-forth: {avg_back_forth:.1f}"

            details = {
                "merged_pr_count": count,
                "average_merge_time_hours": avg_merge_time,
                "average_back_and_forth": avg_back_forth,
                "pr_details": [
                    {
                        "number": pr.number,
                        "merge_time_hours": (pr.merged_at - pr.created_at).total_seconds() / 3600 if pr.merged_at and pr.created_at else None,
                        "back_and_forth": len(context.ledger.get_reviews_for_pr(pr.number)) + len(context.ledger.get_comments_for_pr(pr.number))
                    } for pr in merged_prs
                ]
            }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details
        )