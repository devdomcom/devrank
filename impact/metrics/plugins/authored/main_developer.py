"""
Main Developer Metrics

Identify the primary author per file — the contributor with the most
ownership of each file. Two variants exist:

- Main Developer (by revisions): ranks by commit count
- Main Developer (by lines): ranks by line changes (additions + deletions)

Both metrics share the same scaffolding for per-file contributor attribution.
"""

from collections import defaultdict
from typing import Literal

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import (
    build_file_contributors,
    filter_prs_for_contribution,
    is_generated_file,
)


def _find_main_developer(contributors: dict[str, float]) -> tuple[str | None, float, float]:
    """
    Find the main (top) developer from a contributors map.

    Args:
        contributors: author -> contribution score mapping.

    Returns:
        A tuple of (main_author, main_author_score, total_score).
        If contributors is empty, returns (None, 0.0, 0.0).
    """
    if not contributors:
        return None, 0.0, 0.0

    total = sum(contributors.values())
    if total <= 0:
        return None, 0.0, 0.0

    # Tie-break alphabetically for determinism (first alphabetically wins on ties)
    top_author = sorted(contributors.keys(), key=lambda a: (-contributors[a], a))[0]
    top_score = contributors[top_author]
    return top_author, top_score, total


class MainDeveloperByRevisions(Metric):
    """
    Main Developer (by revisions): primary author per file by commit count.

    For each file, identifies the author with the most commits touching it.
    """

    @property
    def slug(self) -> str:
        return "main_developer_by_revisions"

    @property
    def name(self) -> str:
        return "Main Developer (by revisions)"

    @property
    def description(self) -> str:
        return (
            "Primary author per file determined by commit count. "
            "Identifies who has the most revisions touching each file."
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
            else 0
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
                    "files_with_main_developer": 0,
                    "main_developers": [],
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Step 2: Build per-file contributor attribution by revisions (commit count)
        file_contributors = build_file_contributors(
            files_in_scope, bundle, context.ledger, by="revisions"
        )

        # Step 3: Identify main developer per file
        main_developers = []
        for filename in sorted(files_in_scope):
            contributors = file_contributors.get(filename, {})
            main_author, main_score, total = _find_main_developer(contributors)
            if main_author is None:
                continue

            ownership_pct = (main_score / total * 100) if total > 0 else 0.0
            main_developers.append({
                "file": filename,
                "main_author": main_author,
                "revision_count": int(main_score),
                "total_revisions": int(total),
                "ownership_pct": round(ownership_pct, 1),
            })

        files_with_main = len(main_developers)
        total_files = len(files_in_scope)

        # Build summary
        if files_with_main == 0:
            summary = f"No main developers identified ({total_files} files analyzed)."
        elif files_with_main == 1:
            md = main_developers[0]
            summary = f"Main developer for {md['file']}: {md['main_author']} ({md['ownership_pct']:.0f}% of revisions)."
        else:
            summary = f"Identified main developers for {files_with_main} of {total_files} files."

        details = {
            "total_files": total_files,
            "files_with_main_developer": files_with_main,
            "main_developers": main_developers,
            "period_days": round(period_days, 1),
        }

        if total_files < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )


class MainDeveloperByLines(Metric):
    """
    Main Developer (by lines): primary author per file by line changes.

    For each file, identifies the author with the most line changes
    (additions + deletions) touching it.
    """

    @property
    def slug(self) -> str:
        return "main_developer_by_lines"

    @property
    def name(self) -> str:
        return "Main Developer (by lines)"

    @property
    def description(self) -> str:
        return (
            "Primary author per file determined by line changes. "
            "Identifies who contributed the most additions and deletions to each file."
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
            else 0
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
                    "files_with_main_developer": 0,
                    "main_developers": [],
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Step 2: Build per-file contributor attribution by lines (weighted changes)
        file_contributors = build_file_contributors(
            files_in_scope, bundle, context.ledger, by="lines"
        )

        # Step 3: Identify main developer per file
        main_developers = []
        for filename in sorted(files_in_scope):
            contributors = file_contributors.get(filename, {})
            main_author, main_score, total = _find_main_developer(contributors)
            if main_author is None:
                continue

            ownership_pct = (main_score / total * 100) if total > 0 else 0.0
            main_developers.append({
                "file": filename,
                "main_author": main_author,
                "line_contribution": int(main_score),
                "total_lines": int(total),
                "ownership_pct": round(ownership_pct, 1),
            })

        files_with_main = len(main_developers)
        total_files = len(files_in_scope)

        # Build summary
        if files_with_main == 0:
            summary = f"No main developers identified ({total_files} files analyzed)."
        elif files_with_main == 1:
            md = main_developers[0]
            summary = f"Main developer for {md['file']}: {md['main_author']} ({md['ownership_pct']:.0f}% of lines)."
        else:
            summary = f"Identified main developers for {files_with_main} of {total_files} files."

        details = {
            "total_files": total_files,
            "files_with_main_developer": files_with_main,
            "main_developers": main_developers,
            "period_days": round(period_days, 1),
        }

        if total_files < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
