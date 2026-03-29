from collections import defaultdict
from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class TemporalLogicalCoupling(Metric):
    """
    Identify files that frequently change together (temporal/logical coupling).

    Coupling ratio = shared_revisions / avg_revisions * 100
    - shared_revisions: number of PRs where the top-coupled pair changed together
    - avg_revisions: average of each file's revision counts

    Returns the most coupled file pairs and their ratios.
    """

    @property
    def slug(self) -> str:
        return "temporal_logical_coupling"

    @property
    def name(self) -> str:
        return "Temporal / Logical Coupling"

    @property
    def description(self) -> str:
        return "Files that repeatedly change together (hidden dependency indicator)."

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
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        # Cap per-PR file count to avoid O(n^2) explosion on mega-PRs.
        # Common practice in coupling tools (CodeScene caps at 40 files).
        MAX_FILES_FOR_COUPLING = 50

        file_revision_counts: dict[str, int] = defaultdict(int)
        pair_shared_counts: dict[tuple[str, str], int] = defaultdict(int)

        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            filenames = sorted({f.filename for f in files if not is_generated_file(f.filename, f.patch)})
            if len(filenames) > MAX_FILES_FOR_COUPLING:
                continue  # skip mega-PRs -- too noisy for coupling signal
            for filename in filenames:
                file_revision_counts[filename] += 1
            for i in range(len(filenames)):
                for j in range(i + 1, len(filenames)):
                    pair = (filenames[i], filenames[j])
                    pair_shared_counts[pair] += 1

        coupled_pairs = []
        for pair, shared_revisions in pair_shared_counts.items():
            f1, f2 = pair
            rev1 = file_revision_counts.get(f1, 0)
            rev2 = file_revision_counts.get(f2, 0)
            avg_revisions = (rev1 + rev2) / 2
            if avg_revisions == 0:
                continue
            coupling_ratio = shared_revisions / avg_revisions * 100
            coupled_pairs.append({
                "file_a": f1,
                "file_b": f2,
                "shared_revisions": shared_revisions,
                "avg_revisions": round(avg_revisions, 2),
                "coupling_ratio": round(coupling_ratio, 1),
                "file_a_revisions": rev1,
                "file_b_revisions": rev2,
            })

        coupled_pairs.sort(key=lambda item: item["coupling_ratio"], reverse=True)
        top_pairs = coupled_pairs[:10]

        max_ratio = top_pairs[0]["coupling_ratio"] if top_pairs else 0.0
        summary = f"Detected {len(top_pairs)} coupled file pairs (max ratio: {max_ratio:.1f}%)."
        if top_pairs:
            summary += f" Top pair: {top_pairs[0]['file_a']} + {top_pairs[0]['file_b']}."

        period_days = (context.end_date - context.start_date).days if context.start_date and context.end_date else 30
        details: dict[str, object] = {
            "top_coupled_pairs": top_pairs,
            "pair_count": len(coupled_pairs),
            "max_coupling_ratio": max_ratio,
            "period_days": period_days,
        }

        if not prs:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
