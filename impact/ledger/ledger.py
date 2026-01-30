from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from impact.domain.models import CanonicalBundle, PullRequest, ReviewRecord, CommentRecord, Commit, User, TimelineEvent, FileRecord


class Ledger:
    """
    In-memory, read-only ledger that builds deterministic and time-ordered indexes
    from a CanonicalBundle for query-oriented access.
    """

    def __init__(self, bundle: CanonicalBundle):
        self.bundle = bundle

        # Indexes: user_login -> sorted list of PRs by created_at
        self.user_prs: Dict[str, List[PullRequest]] = defaultdict(list)

        # pr_number -> sorted list of reviews by submitted_at
        self.pr_reviews: Dict[int, List[ReviewRecord]] = defaultdict(list)

        # pr_number -> sorted list of comments by created_at
        self.pr_comments: Dict[int, List[CommentRecord]] = defaultdict(list)
        self.review_comments_by_review: Dict[int, List[CommentRecord]] = defaultdict(list)

        # pr_number -> sorted list of commits by date
        self.pr_commits: Dict[int, List[Commit]] = defaultdict(list)
        # pr_number -> files
        self.pr_files: Dict[int, List[FileRecord]] = defaultdict(list)

        # user_login -> sorted list of commits by date
        self.user_commits: Dict[str, List[Commit]] = defaultdict(list)

        # user_login -> sorted list of reviews by submitted_at
        self.user_reviews: Dict[str, List[ReviewRecord]] = defaultdict(list)

        # Populate indexes
        self._build_indexes()
        # PR lookup
        self.pr_by_number: Dict[int, PullRequest] = {pr.number: pr for pr in self.bundle.pull_requests}
        # Timeline indexes
        self.pr_timeline: Dict[int, List] = defaultdict(list)
        self._build_timeline_indexes()

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

    def get_prs_for_user(self, user_login: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[PullRequest]:
        """Get PRs for a user within an optional time period."""
        prs = self.user_prs.get(user_login, [])
        if start_date or end_date:
            if prs:
                tz = prs[0].created_at.tzinfo or timezone.utc
            else:
                tz = timezone.utc
            if start_date and start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=tz)
            if end_date and end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=tz)
            filtered = []
            for pr in prs:
                if start_date and pr.created_at < start_date:
                    continue
                if end_date and pr.created_at > end_date:
                    continue
                filtered.append(pr)
            return filtered
        return prs

    def get_reviews_for_pr(self, pr_number: int) -> List[ReviewRecord]:
        """Get reviews for a PR, time-ordered."""
        return self.pr_reviews.get(pr_number, [])

    def get_comments_for_pr(self, pr_number: int) -> List[CommentRecord]:
        """Get comments for a PR, time-ordered."""
        return self.pr_comments.get(pr_number, [])

    def get_commits_for_pr(self, pr_number: int) -> List[Commit]:
        """Get commits for a PR, time-ordered."""
        return self.pr_commits.get(pr_number, [])

    def get_commits_for_user(self, user_login: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Commit]:
        """Get commits for a user within an optional time period."""
        commits = self.user_commits.get(user_login, [])
        if start_date or end_date:
            tz = commits[0].date.tzinfo if commits else timezone.utc
            if start_date and start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=tz)
            if end_date and end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=tz)
            filtered = []
            for commit in commits:
                if start_date and commit.date < start_date:
                    continue
                if end_date and commit.date > end_date:
                    continue
                filtered.append(commit)
            return filtered
        return commits

    def get_reviews_for_user(self, user_login: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[ReviewRecord]:
        """Get reviews for a user within an optional time period."""
        reviews = self.user_reviews.get(user_login, [])
        if start_date or end_date:
            tz = reviews[0].submitted_at.tzinfo if reviews else timezone.utc
            if start_date and start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=tz)
            if end_date and end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=tz)
            filtered = []
            for review in reviews:
                if start_date and review.submitted_at < start_date:
                    continue
                if end_date and review.submitted_at > end_date:
                    continue
                filtered.append(review)
            return filtered
        return reviews

    def get_timeline_for_pr(self, pr_number: int) -> List:
        """Get timeline events for a PR, time-ordered."""
        return self.pr_timeline.get(pr_number, [])

    def get_pr(self, pr_number: int) -> Optional[PullRequest]:
        return self.pr_by_number.get(pr_number)

    def get_files_for_pr(self, pr_number: int) -> List[FileRecord]:
        return self.pr_files.get(pr_number, [])

    def get_review_comments_for_review(self, review_id: int) -> List[CommentRecord]:
        return self.review_comments_by_review.get(review_id, [])

    def get_merged_prs_for_user(self, user_login: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[PullRequest]:
        """Get merged PRs for a user within an optional time period (filtered by merged_at)."""
        prs = self.user_prs.get(user_login, [])
        if not prs:
            return []
        tz = prs[0].created_at.tzinfo or timezone.utc
        if start_date and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=tz)
        if end_date and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=tz)
        filtered = []
        for pr in prs:
            if not pr.merged or not pr.merged_at:
                continue
            if start_date and pr.merged_at < start_date:
                continue
            if end_date and pr.merged_at > end_date:
                continue
            filtered.append(pr)
        return filtered
