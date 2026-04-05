import logging
from collections import defaultdict
from datetime import UTC, datetime

from impact.domain.models import (
    CanonicalBundle,
    CIRunRecord,
    CommentRecord,
    Commit,
    DeploymentRecord,
    FileRecord,
    PullRequest,
    ReleaseRecord,
    ReviewRecord,
)

log = logging.getLogger(__name__)


class Ledger:
    """
    In-memory, read-only ledger that builds deterministic and time-ordered indexes
    from a CanonicalBundle for query-oriented access.
    """

    def __init__(self, bundle: CanonicalBundle):
        if bundle is None:
            raise ValueError("CanonicalBundle cannot be None")

        self.bundle = bundle

        log.debug(
            "Initializing Ledger with %d PRs, %d reviews, %d commits, %d comments",
            len(bundle.pull_requests),
            len(bundle.reviews),
            len(bundle.commits),
            len(bundle.comments),
        )

        # Indexes: user_login -> sorted list of PRs by created_at
        self.user_prs: dict[str, list[PullRequest]] = defaultdict(list)

        # pr_number -> sorted list of reviews by submitted_at
        self.pr_reviews: dict[int, list[ReviewRecord]] = defaultdict(list)

        # pr_number -> sorted list of comments by created_at
        self.pr_comments: dict[int, list[CommentRecord]] = defaultdict(list)
        self.review_comments_by_review: dict[int, list[CommentRecord]] = defaultdict(list)

        # pr_number -> sorted list of commits by date
        self.pr_commits: dict[int, list[Commit]] = defaultdict(list)
        # pr_number -> files
        self.pr_files: dict[int, list[FileRecord]] = defaultdict(list)

        # user_login -> sorted list of commits by date
        self.user_commits: dict[str, list[Commit]] = defaultdict(list)

        # user_login -> sorted list of reviews by submitted_at
        self.user_reviews: dict[str, list[ReviewRecord]] = defaultdict(list)

        # Populate indexes
        self._build_indexes()
        # PR lookup
        self.pr_by_number: dict[int, PullRequest] = {
            pr.number: pr for pr in self.bundle.pull_requests
        }
        # Timeline indexes
        self.pr_timeline: dict[int, list] = defaultdict(list)
        self._build_timeline_indexes()

        # DORA data indexes (repo-scoped, time-ordered)
        self.releases: list[ReleaseRecord] = sorted(
            bundle.releases, key=lambda r: r.created_at,
        )
        self.deployments: list[DeploymentRecord] = sorted(
            bundle.deployments, key=lambda d: d.created_at,
        )
        self.ci_runs: list[CIRunRecord] = sorted(
            bundle.ci_runs, key=lambda c: c.created_at,
        )
        # CI runs by PR for per-PR lead-time decomposition
        self.pr_ci_runs: dict[int, list[CIRunRecord]] = defaultdict(list)
        for ci in self.ci_runs:
            if ci.pull_request_number is not None:
                self.pr_ci_runs[ci.pull_request_number].append(ci)

    def _build_indexes(self):
        # PRs by user
        for pr in self.bundle.pull_requests:
            self.user_prs[pr.user.login].append(pr)

        for user_prs in self.user_prs.values():
            user_prs.sort(key=lambda p: p.created_at)

        # Reviews by PR
        for review in self.bundle.reviews:
            self.pr_reviews[review.pull_request_number].append(review)
            self.user_reviews[review.user.login].append(review)

        for reviews in self.pr_reviews.values():
            reviews.sort(key=lambda r: r.submitted_at)

        for reviews in self.user_reviews.values():
            reviews.sort(key=lambda r: r.submitted_at)

        # Comments by PR
        for comment in self.bundle.comments:
            if comment.pull_request_number:
                self.pr_comments[comment.pull_request_number].append(comment)
            if comment.review_id:
                self.review_comments_by_review[comment.review_id].append(comment)

        for comments in self.pr_comments.values():
            comments.sort(key=lambda c: c.created_at)
        for comments in self.review_comments_by_review.values():
            comments.sort(key=lambda c: c.created_at)

        # Commits by PR and by user
        for commit in self.bundle.commits:
            if commit.pull_request_number:
                self.pr_commits[commit.pull_request_number].append(commit)
            self.user_commits[commit.author.login].append(commit)

        for commits in self.pr_commits.values():
            commits.sort(key=lambda c: c.date)

        for commits in self.user_commits.values():
            commits.sort(key=lambda c: c.date)

        # Files by PR
        if hasattr(self.bundle, "files"):
            for file in self.bundle.files:
                self.pr_files[file.pull_request_number].append(file)

    def _build_timeline_indexes(self):
        if not hasattr(self.bundle, "timeline"):
            return
        for evt in getattr(self.bundle, "timeline", []):
            self.pr_timeline[evt.pull_request_number].append(evt)
        for events in self.pr_timeline.values():
            events.sort(key=lambda e: e.created_at)

    def _filter_by_date(
        self,
        records: list,
        start_date: datetime | None,
        end_date: datetime | None,
        date_attr: str,
    ) -> list:
        """DRY helper for date-range filtering on records (by attr like 'created_at')."""
        if not (start_date or end_date):
            return records
        if not records:
            return []
        # TZ normalization (first record's tz)
        sample = records[0]
        tz = getattr(sample, date_attr).tzinfo or UTC
        if start_date and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=tz)
        if end_date and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=tz)
        filtered = []
        for rec in records:
            rec_date = getattr(rec, date_attr)
            if start_date and rec_date < start_date:
                continue
            if end_date and rec_date > end_date:
                continue
            filtered.append(rec)
        return filtered

    def get_prs_for_user(
        self, user_login: str, start_date: datetime | None = None, end_date: datetime | None = None
    ) -> list[PullRequest]:
        """Get PRs for a user within an optional time period."""
        prs = self.user_prs.get(user_login, [])
        return self._filter_by_date(prs, start_date, end_date, "created_at")

    def get_reviews_for_pr(self, pr_number: int) -> list[ReviewRecord]:
        """Get reviews for a PR, time-ordered."""
        return self.pr_reviews.get(pr_number, [])

    def get_comments_for_pr(self, pr_number: int) -> list[CommentRecord]:
        """Get comments for a PR, time-ordered."""
        return self.pr_comments.get(pr_number, [])

    def get_commits_for_pr(self, pr_number: int) -> list[Commit]:
        """Get commits for a PR, time-ordered."""
        return self.pr_commits.get(pr_number, [])

    def get_commits_for_user(
        self, user_login: str, start_date: datetime | None = None, end_date: datetime | None = None
    ) -> list[Commit]:
        """Get commits for a user within an optional time period."""
        commits = self.user_commits.get(user_login, [])
        return self._filter_by_date(commits, start_date, end_date, "date")

    def get_reviews_for_user(
        self, user_login: str, start_date: datetime | None = None, end_date: datetime | None = None
    ) -> list[ReviewRecord]:
        """Get reviews for a user within an optional time period."""
        reviews = self.user_reviews.get(user_login, [])
        return self._filter_by_date(reviews, start_date, end_date, "submitted_at")

    def get_timeline_for_pr(self, pr_number: int) -> list:
        """Get timeline events for a PR, time-ordered."""
        return self.pr_timeline.get(pr_number, [])

    def get_pr(self, pr_number: int) -> PullRequest | None:
        return self.pr_by_number.get(pr_number)

    def get_files_for_pr(self, pr_number: int) -> list[FileRecord]:
        return self.pr_files.get(pr_number, [])

    def get_review_comments_for_review(self, review_id: int) -> list[CommentRecord]:
        return self.review_comments_by_review.get(review_id, [])

    def get_merged_prs_for_user(
        self, user_login: str, start_date: datetime | None = None, end_date: datetime | None = None
    ) -> list[PullRequest]:
        """Get merged PRs for a user within an optional time period (filtered by merged_at)."""
        prs = self.user_prs.get(user_login, [])
        if not prs:
            return []
        # Filter merged first, then apply date range (reuses helper)
        merged_prs = [pr for pr in prs if pr.merged and pr.merged_at]
        return self._filter_by_date(merged_prs, start_date, end_date, "merged_at")

    def get_releases(
        self, start_date: datetime | None = None, end_date: datetime | None = None,
    ) -> list[ReleaseRecord]:
        """Get releases within an optional time period."""
        return self._filter_by_date(self.releases, start_date, end_date, "created_at")

    def get_deployments(
        self, start_date: datetime | None = None, end_date: datetime | None = None,
        *, environment: str | None = None,
    ) -> list[DeploymentRecord]:
        """Get deployments within an optional time period, optionally filtered by environment."""
        deploys = self._filter_by_date(self.deployments, start_date, end_date, "created_at")
        if environment:
            deploys = [d for d in deploys if d.environment == environment]
        return deploys

    def get_ci_runs_for_pr(self, pr_number: int) -> list[CIRunRecord]:
        """Get CI runs associated with a PR, time-ordered."""
        return self.pr_ci_runs.get(pr_number, [])

    def get_ci_runs(
        self, start_date: datetime | None = None, end_date: datetime | None = None,
    ) -> list[CIRunRecord]:
        """Get CI runs within an optional time period."""
        return self._filter_by_date(self.ci_runs, start_date, end_date, "created_at")
