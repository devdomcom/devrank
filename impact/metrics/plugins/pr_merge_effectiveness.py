from typing import Dict, List

from impact.metrics.base import Metric
from impact.metrics.utils import calculate_merge_time_hours, collect_pr_interactions
from impact.domain.models import MetricContext, MetricResult


class PRMergeEffectiveness(Metric):
    @property
    def slug(self) -> str:
        return "pr_merge_effectiveness"

    @property
    def name(self) -> str:
        return "PR Merge Effectiveness"

    def run(self, context: MetricContext) -> MetricResult:
        merged_prs = context.ledger.get_merged_prs_for_user(context.user_login, context.start_date, context.end_date)

        if not merged_prs:
            summary = "No PRs merged in the period."
            details = {}
        else:
            count = len(merged_prs)
            merge_times = []
            back_forths = []
            pr_rows = []

            for pr in merged_prs:
                merge_time_hours = calculate_merge_time_hours(pr)
                if merge_time_hours is not None:
                    merge_times.append(merge_time_hours)

                interactions = collect_pr_interactions(context, pr.number, pr.user.login, pr.merged_at)
                back_forths.append(len(interactions))

                # per-PR breakdown
                kinds = {}
                for i in interactions:
                    kinds[i["kind"]] = kinds.get(i["kind"], 0) + 1
                pr_rows.append(
                    {
                        "number": pr.number,
                        "merge_time_hours": merge_time_hours,
                        "back_and_forth": len(interactions),
                        "breakdown": kinds,
                    }
                )

            avg_merge_time = sum(merge_times) / len(merge_times) if merge_times else 0
            avg_back_forth = sum(back_forths) / len(back_forths) if back_forths else 0

            summary = f"{count} PRs merged, average merge time: {avg_merge_time:.1f} hours, average back-and-forth: {avg_back_forth:.1f}"

            details = {
                "merged_pr_count": count,
                "average_merge_time_hours": avg_merge_time,
                "average_back_and_forth": avg_back_forth,
                "pr_details": pr_rows,
            }

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details
        )
