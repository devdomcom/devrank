from datetime import timedelta
from typing import Any, Dict, List, TypedDict

from impact.metrics.base import Metric
from impact.domain.models import MetricContext, MetricResult


class PRMergeEffectiveness(Metric):
    @property
    def slug(self) -> str:
        return "pr_merge_effectiveness"

    @property
    def name(self) -> str:
        return "PR Merge Effectiveness"

    class Interaction(TypedDict):
        actor: str
        kind: str  # review|comment_issue|comment_review|timeline
        created_at: Any

    def _collect_interactions(self, context: MetricContext, pr, merge_at) -> List[Interaction]:
        interactions: List[PRMergeEffectiveness.Interaction] = []
        author = pr.user.login

        # Reviews
        for rev in context.ledger.get_reviews_for_pr(pr.number):
            if rev.user.login == author:
                continue
            if merge_at and rev.submitted_at >= merge_at:
                continue
            interactions.append({"actor": rev.user.login, "kind": "review", "created_at": rev.submitted_at})

        # Comments (issue + review)
        for c in context.ledger.get_comments_for_pr(pr.number):
            if c.user.login == author:
                continue
            ts = c.created_at
            if merge_at and ts >= merge_at:
                continue
            kind = "comment_review" if c.type.value == "review" else "comment_issue"
            interactions.append({"actor": c.user.login, "kind": kind, "created_at": ts})

        # Timeline fallbacks (covers events not already represented)
        seen_ts_ids = {(i["kind"], i["actor"], i["created_at"]) for i in interactions}
        for evt in context.ledger.get_timeline_for_pr(pr.number):
            if evt.actor.login == author:
                continue
            if merge_at and evt.created_at >= merge_at:
                continue
            if evt.event in ("reviewed", "commented"):
                key = ("timeline", evt.actor.login, evt.created_at)
                if key not in seen_ts_ids:
                    interactions.append({"actor": evt.actor.login, "kind": "timeline", "created_at": evt.created_at})
                    seen_ts_ids.add(key)

        interactions.sort(key=lambda i: i["created_at"])
        return interactions

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
            pr_rows = []

            for pr in merged_prs:
                if pr.merged_at and pr.created_at:
                    merge_time = pr.merged_at - pr.created_at
                    merge_times.append(merge_time.total_seconds() / 3600)  # hours

                interactions = self._collect_interactions(context, pr, pr.merged_at)
                back_forths.append(len(interactions))

                # per-PR breakdown
                kinds = {}
                for i in interactions:
                    kinds[i["kind"]] = kinds.get(i["kind"], 0) + 1
                pr_rows.append(
                    {
                        "number": pr.number,
                        "merge_time_hours": (pr.merged_at - pr.created_at).total_seconds() / 3600 if pr.merged_at and pr.created_at else None,
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
