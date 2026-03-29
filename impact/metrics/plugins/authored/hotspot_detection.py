from collections import defaultdict
from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class HotspotDetection(Metric):
    """
    Identifies 'Hotspots' in the codebase by tracking files with high revision
    frequency multiplied by complexity (change volume per revision).

    Based on Adam Tornhill's "Software Design X-Rays" (2018) hotspot model:
        hotspot_score = change_frequency * complexity_proxy * recency_weight

    In our implementation:
        - change_frequency  = number of PRs touching the file in the period
        - complexity_proxy  = average (additions + deletions) across those PRs
          (serves as a size/churn proxy when full AST complexity is unavailable)
        - recency_weight    = 1.0 (all PRs in period weighted equally; future
          enhancement could apply exponential time-decay)

    Coefficients rationale:
        - Equal weighting of frequency and complexity is the simplest unbiased
          formulation. Tornhill's original uses log(complexity) for large files,
          but our per-PR-diff scoping already bounds the magnitude.
        - The score is dimensionless (revisions * lines) and is only meaningful
          relative to other files in the same analysis.

    Returns top 10 hotspots for the user.
    """

    @property
    def slug(self) -> str:
        return "hotspot_detection"

    @property
    def name(self) -> str:
        return "Hotspot Detection"

    @property
    def description(self) -> str:
        return "Identifies files with high revision frequency × complexity (churn/instability signal)."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        all_prs = context.ledger.get_prs_for_user(
            user, context.start_date, context.end_date
        )
        # Exclude drafts for stability; hotspots are about delivered changes.
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        file_stats = defaultdict(lambda: {"frequency": 0, "total_changes": 0, "pr_numbers": []})

        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            for f in files:
                if is_generated_file(f.filename, f.patch):
                    continue
                
                stats = file_stats[f.filename]
                stats["frequency"] += 1
                stats["total_changes"] += f.changes
                if pr.number not in stats["pr_numbers"]:
                    stats["pr_numbers"].append(pr.number)

        hotspots = []
        for filename, stats in file_stats.items():
            avg_changes = stats["total_changes"] / stats["frequency"]
            score = stats["frequency"] * avg_changes
            hotspots.append({
                "filename": filename,
                "frequency": stats["frequency"],
                "avg_changes": round(avg_changes, 1),
                "hotspot_score": round(score, 1),
                "pr_numbers": stats["pr_numbers"],
            })

        # Sort by score descending
        hotspots.sort(key=lambda x: x["hotspot_score"], reverse=True)
        top_hotspots = hotspots[:10]

        max_score = top_hotspots[0]["hotspot_score"] if top_hotspots else 0.0

        summary = f"Detected {len(top_hotspots)} hotspots (max score: {max_score:.1f})."
        if top_hotspots:
            summary += f" Top hotspot: {top_hotspots[0]['filename']}."

        details = {
            "top_hotspots": top_hotspots,
            "hotspot_count": len(hotspots),
            "max_hotspot_score": max_score,
            "period_days": (context.end_date - context.start_date).days if context.start_date and context.end_date else 30,
        }

        if not prs:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
