"""
Entity Ownership Metric

Reports per-author contribution percentages per file — the canonical CodeScene
ownership breakdown. Unlike Main Developer (which picks the top contributor)
or Knowledge Islands (which flags extreme concentration), Entity Ownership
exposes the full distribution for every file in scope.

DRY: Reuses _build_file_contributors from main_developer.py for the heavy
lifting of attributing contributions to authors.
"""

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.plugins.authored.main_developer import _build_file_contributors
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class EntityOwnership(Metric):
    """
    Per-author contribution percentages per file.

    For each file the target user touched in the period, reports what share
    of that file's commits (or line changes) each author contributed. This is
    the raw ownership data that powers Main Developer, Knowledge Islands, and
    Bus Factor metrics — surfaced here for direct inspection.

    Supports two weighting modes (matching Main Developer):
      - by="revisions": counts commits per author
      - by="lines": weights by additions + deletions per file
    """

    def __init__(self, by: str = "revisions") -> None:
        """Initialize with weighting mode: 'revisions' (default) or 'lines'."""
        if by not in ("revisions", "lines"):
            raise ValueError(f"by must be 'revisions' or 'lines', got {by!r}")
        self._by = by

    @property
    def slug(self) -> str:
        return "entity_ownership"

    @property
    def name(self) -> str:
        return "Entity Ownership"

    @property
    def description(self) -> str:
        mode = "commit count" if self._by == "revisions" else "line changes"
        return (
            f"Per-author contribution percentages per file (weighted by {mode}). "
            "Exposes the full ownership breakdown for every file in scope."
        )

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["CodeScene"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 30.0  # period fallback per AGENTS.md
        )

        bundle = context.ledger.bundle

        # Step 1: Determine which files the *target user* touched
        user_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        user_prs = filter_prs_for_contribution(user_prs, exclude_drafts=True, only_merged=False)

        files_in_scope: set[str] = set()
        for pr in user_prs:
            for f in context.ledger.get_files_for_pr(pr.number):
                if not is_generated_file(f.filename, getattr(f, "patch", None)):
                    files_in_scope.add(f.filename)

        if not files_in_scope:
            return MetricResult(
                metric_slug=self.slug,
                summary="No files found in analysis period.",
                details={
                    "total_files": 0,
                    "avg_top_owner_pct": 0.0,
                    "files": [],
                    "by": self._by,
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Step 2: Build per-file contributor attribution (reuses DRY helper)
        file_contributors = _build_file_contributors(
            files_in_scope, bundle, context.ledger, by=self._by  # type: ignore[arg-type]
        )

        # Step 3: Convert raw counts to ownership percentages per file
        file_ownership: list[dict] = []
        for filename in sorted(files_in_scope):
            contributors = file_contributors.get(filename, {})
            total = sum(contributors.values())

            if total <= 0:
                continue

            # Sort authors by contribution descending, then alphabetically for determinism
            sorted_authors = sorted(contributors.items(), key=lambda kv: (-kv[1], kv[0]))
            ownership_entries = [
                {
                    "author": author,
                    "contribution": float(score),
                    "ownership_pct": round((score / total) * 100, 1),
                }
                for author, score in sorted_authors
            ]

            file_ownership.append({
                "file": filename,
                "total_contributions": float(total),
                "author_count": len(ownership_entries),
                "ownership": ownership_entries,
            })

        total_files = len(file_ownership)

        # Compute aggregate: average top-owner percentage across all files.
        # This gives a single scalar indicating overall ownership concentration.
        avg_top_owner_pct = 0.0
        if total_files > 0:
            top_pcts = [
                fo["ownership"][0]["ownership_pct"]
                for fo in file_ownership
                if fo["ownership"]
            ]
            if top_pcts:
                avg_top_owner_pct = sum(top_pcts) / len(top_pcts)

        # Build summary
        if total_files == 0:
            summary = "No ownership data available."
        elif total_files == 1:
            fo = file_ownership[0]
            top = fo["ownership"][0]
            summary = (
                f"Ownership for {fo['file']}: {top['author']} owns "
                f"{top['ownership_pct']:.0f}% ({self._by})."
            )
        else:
            summary = (
                f"Computed ownership for {total_files} files "
                f"(avg top-owner {avg_top_owner_pct:.0f}%, {self._by}, "
                f"{round(period_days, 1)} days)."
            )

        details: dict = {
            "total_files": total_files,
            "avg_top_owner_pct": round(avg_top_owner_pct, 1),
            "files": file_ownership,
            "by": self._by,
            "period_days": round(period_days, 1),
        }

        if total_files < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
