"""
Review Coverage Metric

Measures the percentage of PR files that received at least one inline review
comment from a human (non-author, non-bot) reviewer.  High review coverage
is a quality gate: it ensures human eyes have inspected the actual code
changes before they merge.

Critical in the AI era: as AI tools generate more code, review coverage
prevents quality collapse by measuring whether humans actually examine
the generated output at the file level.

Data requirements (all present in canonical dump):
  - FileRecord.filename + pull_request_number
  - CommentRecord.path + pull_request_number + type == REVIEW
"""

from impact.domain.models import CommentType, MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import (
    filter_prs_for_contribution,
    is_bot_user,
    is_generated_file,
)


class ReviewCoverage(Metric):
    """
    Review Coverage: % of PR files with at least one inline review comment.

    For each of the developer's PRs in the analysis period, computes the
    ratio of files that received at least one inline review comment from
    a human reviewer (excluding PR author self-comments and bot reviews).

    Generated files (lockfiles, protobuf output, etc.) are excluded from
    the denominator to avoid penalising coverage on unreviewed boilerplate.
    """

    @property
    def slug(self) -> str:
        return "review_coverage"

    @property
    def name(self) -> str:
        return "Review Coverage"

    @property
    def description(self) -> str:
        return "% of PR files with at least one human inline review comment."

    @property
    def category(self) -> str:
        return "code_quality"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        # 1. Get the user's PRs
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(
            all_prs, exclude_drafts=True, only_merged=False,
        )

        total_files = 0
        reviewed_files = 0
        generated_excluded = 0
        per_pr: list[dict] = []
        fully_reviewed_prs = 0
        unreviewed_prs = 0

        for pr in prs:
            # 2. Get non-generated files for this PR
            files = context.ledger.get_files_for_pr(pr.number)
            pr_filenames: set[str] = set()
            pr_generated = 0
            for f in files:
                if is_generated_file(f.filename, getattr(f, "patch", None)):
                    pr_generated += 1
                    continue
                pr_filenames.add(f.filename)

            generated_excluded += pr_generated

            if not pr_filenames:
                continue

            # 3. Get inline review comments for this PR.
            #    Only count review-type comments with a path (inline comments)
            #    from humans who are NOT the PR author and NOT bots.
            comments = context.ledger.get_comments_for_pr(pr.number)
            reviewed_paths: set[str] = set()
            for c in comments:
                # Must be a review comment (inline), not an issue comment
                if c.type != CommentType.REVIEW:
                    continue
                # Must have a file path (inline = file-level)
                if not c.path:
                    continue
                # Exclude PR author self-comments
                if c.user.login == pr.user.login:
                    continue
                # Exclude bot reviewers
                if is_bot_user(c.user):
                    continue
                reviewed_paths.add(c.path)

            # 4. Compute coverage for this PR
            pr_covered = pr_filenames & reviewed_paths
            pr_total = len(pr_filenames)
            pr_covered_count = len(pr_covered)
            pr_pct = (pr_covered_count / pr_total * 100) if pr_total > 0 else 0.0

            total_files += pr_total
            reviewed_files += pr_covered_count

            if pr_covered_count == pr_total:
                fully_reviewed_prs += 1
            elif pr_covered_count == 0:
                unreviewed_prs += 1

            per_pr.append({
                "number": pr.number,
                "total_files": pr_total,
                "reviewed_files": pr_covered_count,
                "coverage_pct": round(pr_pct, 1),
                "unreviewed": sorted(pr_filenames - reviewed_paths)[:5],
            })

        # 5. Aggregate
        coverage_pct = (
            (reviewed_files / total_files * 100) if total_files > 0 else 0.0
        )

        # Summary
        if not prs:
            summary = "No PRs found in the analysis period."
        elif total_files == 0:
            summary = "No non-generated files found across PRs."
        else:
            summary = (
                f"Review coverage: {coverage_pct:.1f}% "
                f"({reviewed_files}/{total_files} files across {len(per_pr)} PRs). "
            )
            if fully_reviewed_prs:
                summary += f"{fully_reviewed_prs} PR(s) fully reviewed. "
            if unreviewed_prs:
                summary += f"{unreviewed_prs} PR(s) with no file-level comments."

        details: dict[str, object] = {
            "coverage_pct": round(coverage_pct, 1),
            "total_files": total_files,
            "reviewed_files": reviewed_files,
            "unreviewed_files": total_files - reviewed_files,
            "total_pr_count": len(per_pr),
            "fully_reviewed_prs": fully_reviewed_prs,
            "unreviewed_prs": unreviewed_prs,
            "generated_files_excluded": generated_excluded,
            "per_pr": per_pr,
            "period_days": round(period_days, 1),
        }

        # No-data guard: combined period+count
        if len(prs) == 0 or (period_days < 14 and len(per_pr) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
