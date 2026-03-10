from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import detect_module_boundary, filter_prs_for_contribution, is_generated_file


class EntityFragmentation(Metric):
    """
    Measure how fragmented changes are across codebase entities using a Herfindahl-like index.

    Entities are module boundaries derived from file paths. We compute the share of total
    changes per entity, then calculate:
        - HHI (concentration) = sum(share^2)
        - fragmentation_index = 1 - HHI

    Higher fragmentation implies work is spread across many entities (lower focus).
    """

    @property
    def slug(self) -> str:
        return "entity_fragmentation"

    @property
    def name(self) -> str:
        return "Entity Fragmentation"

    @property
    def description(self) -> str:
        return "Herfindahl-like fragmentation of changes across modules (higher = more scattered)."

    @property
    def category(self) -> str:
        return "process_discipline"

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        entity_changes: dict[str, int] = defaultdict(int)
        total_changes = 0

        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            for file in files:
                if is_generated_file(file.filename, file.patch):
                    continue
                entity = detect_module_boundary(file.filename)
                changes = file.changes if file.changes is not None else file.additions + file.deletions
                entity_changes[entity] += changes
                total_changes += changes

        entity_count = len(entity_changes)
        hhi = 0.0
        entity_shares: dict[str, float] = {}
        if total_changes > 0:
            for entity, changes in entity_changes.items():
                share = changes / total_changes
                entity_shares[entity] = round(share, 4)
                hhi += share * share

        fragmentation_index = 1.0 - hhi if total_changes > 0 else 0.0
        summary = (
            f"Fragmentation index: {fragmentation_index:.2f} across {entity_count} entities "
            f"({total_changes} total changes)."
        )

        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        details: dict[str, object] = {
            "fragmentation_index": round(fragmentation_index, 4),
            "hhi": round(hhi, 4),
            "entity_count": entity_count,
            "total_changes": total_changes,
            "entity_shares": dict(sorted(entity_shares.items(), key=lambda item: item[1], reverse=True)),
            "period_days": period_days,
        }

        if not prs or total_changes == 0 or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
