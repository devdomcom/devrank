from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import calculate_merge_time_hours, collect_pr_interactions


class PRMergeEffectiveness(Metric):
    """
    Measures the effectiveness of a user's merged pull requests.

    This metric combines merge time with the amount of back-and-forth interaction
    (reviews, comments, timeline events) that occurred before merge. It helps
    identify how smoothly PRs are being merged - fewer interactions with faster
    merge times indicate more effective PRs.

    Details returned:
        - merged_pr_count: Number of merged PRs
        - average_merge_time_hours: Average time from creation to merge
        - average_back_and_forth: Average number of interactions before merge
        - pr_details: Per-PR breakdown with interaction types
    """

    @property
    def slug(self) -> str:
        return "pr_merge_effectiveness"

    @property
    def name(self) -> str:
        return "PR Merge Effectiveness"

    @property
    def description(self) -> str:
        return "Combines merge speed with review interaction count for merge smoothness."

    @property
    def category(self) -> str:
        return "pr_hygiene_process"

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        merged_prs = context.ledger.get_merged_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )

        if not merged_prs:
            summary = "No PRs merged in the period."
            details = {"no_data": True, "period_days": period_days}
        else:
            count = len(merged_prs)
            merge_times = []
            back_forths = []
            pr_rows = []

            for pr in merged_prs:
                merge_time_hours = calculate_merge_time_hours(pr)
                if merge_time_hours is not None:
                    merge_times.append(merge_time_hours)

                interactions = collect_pr_interactions(
                    context, pr.number, pr.user.login, pr.merged_at
                )
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
                "period_days": period_days,
            }
            if not merged_prs or (period_days < 14 and len(merged_prs) < 3):
                details["no_data"] = True

        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
