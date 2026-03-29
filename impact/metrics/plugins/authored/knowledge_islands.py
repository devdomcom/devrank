"""
Knowledge Islands Metric

Identifies files/modules where 95%+ of the code is written by a single person.
This is a risk metric: high ownership concentration means no one else understands
the code, creating a maintenance risk.

Critical in the AI era: AI-generated code (Claude, Copilot, etc.) often creates
"phantom ownership" where a single AI (or AI-assisted human) dominates a file,
leaving no human with deep understanding.

Uses DRY principles by reusing file contribution tracking pattern from bus_factor.
"""

from collections import defaultdict

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class KnowledgeIslands(Metric):
    """
    Knowledge Islands: files where 95%+ written by one person.

    Identifies files with extreme ownership concentration. If one author
    contributed 95%+ of a file's code, that file is a "knowledge island" —
    risky because no one else truly understands it.

    In the AI era, this captures phantom ownership: when Claude, Copilot, or
    another AI tool is the dominant "author" of a file.

    DRY: Reuses file contribution tracking pattern from bus_factor.
    """

    # Threshold for knowledge island detection
    ISLAND_THRESHOLD = 0.95  # 95% ownership = island

    @property
    def slug(self) -> str:
        return "knowledge_islands"

    @property
    def name(self) -> str:
        return "Knowledge Islands"

    @property
    def description(self) -> str:
        return (
            "Files/modules where 95%+ written by one person (or AI). High ownership "
            "concentration is a maintenance risk — no one else understands the code. "
            "In the AI era, captures phantom ownership by Claude/Copilot/etc."
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

        # Access the full bundle to compute true file ownership from commits
        bundle = context.ledger.bundle

        # Build file -> set of PR numbers that touched it
        file_to_prs: dict[str, set[int]] = defaultdict(set)
        for f in getattr(bundle, "files", []):
            if is_generated_file(f.filename, getattr(f, "patch", None)):
                continue
            file_to_prs[f.filename].add(f.pull_request_number)

        # Build PR number -> list of commits
        pr_to_commits: dict[int, list] = defaultdict(list)
        for commit in bundle.commits:
            if commit.pull_request_number is not None:
                pr_to_commits[commit.pull_request_number].append(commit)

        # Collect file ownership: filename -> {author_login: commit_count}
        file_contributors: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        files_in_scope: set[str] = set()

        for filename, pr_numbers in file_to_prs.items():
            files_in_scope.add(filename)
            for pr_num in pr_numbers:
                for commit in pr_to_commits.get(pr_num, []):
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        file_contributors[filename][author] += 1

        if not files_in_scope:
            return MetricResult(
                metric_slug=self.slug,
                summary="No files found in analysis period.",
                details={
                    "island_count": 0,
                    "total_files": 0,
                    "islands": [],
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Identify knowledge islands (files with one author >= 95% of commits)
        islands = []
        for filename, contributors in file_contributors.items():
            if not contributors:
                continue

            total = sum(contributors.values())
            if total <= 0:
                continue

            # Find top contributor by commit count
            top_author = max(contributors.keys(), key=lambda c: contributors[c])
            top_count = contributors[top_author]
            ownership_pct = top_count / total

            if ownership_pct >= self.ISLAND_THRESHOLD:
                islands.append({
                    "file": filename,
                    "owner": top_author,
                    "ownership_pct": round(ownership_pct * 100, 1),
                    "total_commits": total,
                    "owner_commits": top_count,
                })

        # Sort islands by ownership percentage (highest first)
        islands.sort(key=lambda i: i["ownership_pct"], reverse=True)

        island_count = len(islands)
        total_files = len(files_in_scope)

        # Build summary
        if island_count == 0:
            summary = f"No knowledge islands found ({total_files} files analyzed)."
        else:
            top_islands = islands[:3]
            top_str = ", ".join(
                f"{i['file']} ({i['owner']}: {i['ownership_pct']:.0f}%)"
                for i in top_islands
            )
            summary = (
                f"Found {island_count} knowledge island(s) out of {total_files} files. "
                f"Top: {top_str}"
            )

        details = {
            "island_count": island_count,
            "total_files": total_files,
            "island_pct": round((island_count / total_files * 100), 1) if total_files > 0 else 0.0,
            "islands": islands[:20],
            "period_days": round(period_days, 1),
        }

        # No-data guard
        if total_files < 3:
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
