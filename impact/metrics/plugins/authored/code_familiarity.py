"""
Code Familiarity Metric

Measures the percentage of the codebase (files in scope) that has been touched
by at least one active contributor. A file is "familiar" if any team member
with commits in the analysis period has touched it.

This is a knowledge diffusion metric — it answers "How much of the code we
care about is known by someone currently active on the team?"
"""

from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class CodeFamiliarity(Metric):
    """
    Code Familiarity: % of codebase known by current active team.

    A file counts as "familiar" if at least one contributor with commits in
    the analysis period has touched it. This is a binary per-file check —
    no dominance threshold required. The metric simply measures how much of
    the scoped codebase has any active knowledge.

    This is the positive counterpart to Knowledge Loss: instead of asking
    "what code is dominated by people who left?", we ask "what code has
    someone active touched at all?"
    """

    @property
    def slug(self) -> str:
        return "code_familiarity"

    @property
    def name(self) -> str:
        return "Code Familiarity"

    @property
    def description(self) -> str:
        return (
            "Percentage of the codebase (files touched by the target user) that has "
            "been touched by at least one active contributor — someone with commits "
            "in the analysis period. High familiarity means the active team has "
            "knowledge spread across the code."
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
                    "familiar_file_count": 0,
                    "total_files": 0,
                    "familiarity_pct": 0.0,
                    "active_contributors_count": 0,
                    "active_contributors": [],
                    "unfamiliar_files": [],
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Step 2: Build file -> set of PR numbers (from ALL repo files touching scoped files)
        file_to_prs: dict[str, set[int]] = defaultdict(set)
        for f in getattr(bundle, "files", []):
            if f.filename in files_in_scope and not is_generated_file(f.filename, getattr(f, "patch", None)):
                file_to_prs[f.filename].add(f.pull_request_number)

        # Step 3: Build PR -> commits mapping
        pr_to_commits: dict[int, list] = defaultdict(list)
        for commit in bundle.commits:
            if commit.pull_request_number is not None:
                pr_to_commits[commit.pull_request_number].append(commit)

        # Step 4: Collect per-file author contributions (commit counts)
        file_contributors: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for filename, pr_numbers in file_to_prs.items():
            for pr_num in pr_numbers:
                for commit in pr_to_commits.get(pr_num, []):
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        file_contributors[filename][author] += 1

        # Also credit PR authors directly for files with no linked commits
        for pr in bundle.pull_requests:
            for f in context.ledger.get_files_for_pr(pr.number):
                if f.filename in files_in_scope and not is_generated_file(f.filename, getattr(f, "patch", None)):
                    if not file_contributors[f.filename]:
                        file_contributors[f.filename][pr.user.login] += 1

        # Step 5: Identify active contributors (have commits within analysis period)
        active_contributors: set[str] = set()
        for commit in bundle.commits:
            commit_date = commit.date
            if context.start_date and context.end_date:
                if commit_date.tzinfo is None and context.start_date.tzinfo is not None:
                    commit_date = commit_date.replace(tzinfo=context.start_date.tzinfo)
                if context.start_date <= commit_date <= context.end_date:
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        active_contributors.add(author)

        # Step 6: Determine familiarity per file
        familiar_files: set[str] = set()
        unfamiliar_files: list[str] = []

        for filename in files_in_scope:
            contributors = file_contributors.get(filename, {})
            # A file is familiar if any of its contributors is active
            has_active = any(author in active_contributors for author in contributors)
            if has_active:
                familiar_files.add(filename)
            else:
                unfamiliar_files.append(filename)

        total_files = len(files_in_scope)
        familiar_count = len(familiar_files)
        familiarity_pct = (familiar_count / total_files * 100) if total_files > 0 else 0.0

        # Sort unfamiliar files for stable output
        unfamiliar_files.sort()

        # Build summary
        if familiar_count == 0:
            summary = f"No files are familiar to the active team ({total_files} files analyzed)."
        elif familiar_count == total_files:
            summary = f"All {total_files} files are familiar to the active team (100%)."
        else:
            summary = (
                f"{familiarity_pct:.1f}% of codebase known by active team "
                f"({familiar_count} of {total_files} files)."
            )

        details = {
            "familiar_file_count": familiar_count,
            "total_files": total_files,
            "familiarity_pct": round(familiarity_pct, 1),
            "active_contributors_count": len(active_contributors),
            "active_contributors": sorted(active_contributors),
            "unfamiliar_file_count": len(unfamiliar_files),
            "unfamiliar_files": unfamiliar_files[:20],  # cap for payload size
            "period_days": round(period_days, 1),
        }

        # No-data guard for sparse periods (mirrors Bus Factor pattern)
        if total_files < 3 or (period_days < 14 and len(user_prs) < 5):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
