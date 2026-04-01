"""
Knowledge Loss Metric

Identifies code where 50%+ was written by departed or inactive contributors.
This is a risk metric: when contributors who wrote significant portions of
a file are no longer active, the remaining team lacks knowledge about that code.

Critical in the AI era: AI-generated code often obscures original author
information, and when human contributors leave, knowledge about both AI and
human contributions is lost.

Uses DRY principles by reusing file contribution tracking pattern from
bus_factor and knowledge_islands.
"""

from collections import defaultdict
from datetime import datetime

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


class KnowledgeLoss(Metric):
    """
    Knowledge Loss: code where 50%+ written by departed/inactive contributors.

    Identifies files where a significant portion of code was written by
    contributors who are no longer active. This creates knowledge gaps
    where the current team may not understand critical code.

    The 50% threshold captures meaningful knowledge risk while avoiding
    noise from minor contributors. For extreme concentration (95%+ by
    one person), see Knowledge Islands.

    Inactive contributors are those with no commits within the analysis
    period (context.start_date to context.end_date).
    """

    # Threshold for knowledge loss detection
    LOSS_THRESHOLD = 0.50  # 50% ownership by inactive = knowledge loss

    @property
    def slug(self) -> str:
        return "knowledge_loss"

    @property
    def name(self) -> str:
        return "Knowledge Loss"

    @property
    def description(self) -> str:
        return (
            "Code where 50%+ written by departed/inactive contributors. "
            "High values indicate knowledge gaps - the current team may not "
            "understand code written by people who have left or stopped contributing."
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
        user_files: set[str] = set()
        for pr in user_prs:
            for f in context.ledger.get_files_for_pr(pr.number):
                if not is_generated_file(f.filename, getattr(f, "patch", None)):
                    user_files.add(f.filename)

        # Step 2: Build file -> set of PR numbers from ALL repo PRs (not just user)
        file_to_prs: dict[str, set[int]] = defaultdict(set)
        for f in getattr(bundle, "files", []):
            if f.filename not in user_files:
                continue
            if is_generated_file(f.filename, getattr(f, "patch", None)):
                continue
            file_to_prs[f.filename].add(f.pull_request_number)

        # Build PR number -> list of commits
        pr_to_commits: dict[int, list] = defaultdict(list)
        for commit in bundle.commits:
            if commit.pull_request_number is not None:
                pr_to_commits[commit.pull_request_number].append(commit)

        # Step 3: Collect file ownership from ALL contributors
        file_contributors: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        files_in_scope: set[str] = set()

        for filename, pr_numbers in file_to_prs.items():
            files_in_scope.add(filename)
            for pr_num in pr_numbers:
                for commit in pr_to_commits.get(pr_num, []):
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        file_contributors[filename][author] += 1

        # Also credit PR authors directly for files with no linked commits
        for pr in bundle.pull_requests:
            for f in context.ledger.get_files_for_pr(pr.number):
                if f.filename in user_files and not is_generated_file(f.filename, getattr(f, "patch", None)):
                    files_in_scope.add(f.filename)
                    if not file_contributors[f.filename]:
                        file_contributors[f.filename][pr.user.login] += 1

        # Step 4: Determine active contributors (have commits within analysis period)
        active_contributors: set[str] = set()
        for commit in bundle.commits:
            commit_date = commit.date
            if context.start_date and context.end_date:
                # Normalize timezone if needed
                if commit_date.tzinfo is None and context.start_date.tzinfo is not None:
                    commit_date = commit_date.replace(tzinfo=context.start_date.tzinfo)
                if context.start_date <= commit_date <= context.end_date:
                    author = getattr(commit.author, "login", None) or str(commit.author)
                    if author:
                        active_contributors.add(author)

        if not files_in_scope:
            return MetricResult(
                metric_slug=self.slug,
                summary="No files found in analysis period.",
                details={
                    "loss_count": 0,
                    "total_files": 0,
                    "at_risk_files": [],
                    "period_days": round(period_days, 1),
                    "no_data": True,
                },
            )

        # Step 5: Identify knowledge loss files (50%+ by inactive contributors)
        at_risk_files = []
        total_inactive_lines = 0
        total_lines = 0

        for filename, contributors in file_contributors.items():
            if not contributors:
                continue

            total = sum(contributors.values())
            if total <= 0:
                continue

            # Calculate contribution from inactive contributors
            inactive_contribution = 0
            inactive_authors = []

            for author, count in contributors.items():
                if author not in active_contributors:
                    inactive_contribution += count
                    inactive_authors.append(author)

            inactive_pct = inactive_contribution / total

            if inactive_pct >= self.LOSS_THRESHOLD:
                at_risk_files.append({
                    "file": filename,
                    "inactive_contributors": inactive_authors,
                    "inactive_pct": round(inactive_pct * 100, 1),
                    "total_commits": total,
                    "inactive_commits": inactive_contribution,
                })

            total_inactive_lines += inactive_contribution
            total_lines += total

        # Sort by inactive percentage (highest first)
        at_risk_files.sort(key=lambda f: f["inactive_pct"], reverse=True)

        loss_count = len(at_risk_files)
        total_files = len(files_in_scope)

        # Calculate overall knowledge loss percentage
        overall_loss_pct = (
            (total_inactive_lines / total_lines * 100) if total_lines > 0 else 0.0
        )

        # Build summary
        if loss_count == 0:
            summary = f"No knowledge loss detected ({total_files} files analyzed)."
        else:
            top_files = at_risk_files[:3]
            top_str = ", ".join(
                f"{f['file']} ({f['inactive_pct']:.0f}%)"
                for f in top_files
            )
            summary = (
                f"Found {loss_count} file(s) with knowledge loss out of {total_files} files "
                f"({overall_loss_pct:.1f}% overall). Top: {top_str}"
            )

        details = {
            "loss_count": loss_count,
            "total_files": total_files,
            "loss_pct": round((loss_count / total_files * 100), 1) if total_files > 0 else 0.0,
            "overall_loss_pct": round(overall_loss_pct, 1),
            "at_risk_files": at_risk_files[:20],
            "active_contributors_count": len(active_contributors),
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
