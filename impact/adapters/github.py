import json
import logging
from datetime import datetime
from pathlib import Path

from impact.adapters.base import ProviderAdapter
from impact.exceptions import ManifestInvalidError, ManifestNotFoundError

log = logging.getLogger(__name__)
from impact.domain.models import (
    Branch,
    CanonicalBundle,
    CommentRecord,
    CommentType,
    Commit,
    FileRecord,
    PullRequest,
    PullRequestState,
    Repository,
    ReviewRecord,
    ReviewState,
    TimelineEvent,
    User,
    UserType,
)


class GitHubAdapter(ProviderAdapter):
    """
    Parse a filesystem dump produced by the live GitHub fetcher into the
    canonical in‑memory bundle. Only keeps data:
      - within the manifest's [from, to] window
      - where the vetted user authored the PR *or* acted (review, comment,
        commit, timeline event) on the PR.
    This trims noisy data (e.g., PRs where the user was merely assigned or
    requested as reviewer but never acted).
    """

    @staticmethod
    def _normalize_timeline_event(
        tl_dict: dict, ensure_user_fn: callable,
    ) -> TimelineEvent | None:
        """Normalize a raw timeline dict into a TimelineEvent, or None if unparseable.

        Centralizes all timeline-specific parsing: skipping non-event entries
        (e.g. 'committed' which lack id/actor/event), extracting PR number,
        actor resolution, and requested_reviewer handling.
        """
        # 'committed' entries have no 'event' key — skip (commits parsed separately)
        if "event" not in tl_dict:
            return None
        # Require id and created_at
        if "id" not in tl_dict or not tl_dict.get("created_at"):
            return None

        actor_dict = tl_dict.get("actor") or {}
        try:
            actor = ensure_user_fn(actor_dict)
        except (ValueError, KeyError, TypeError):
            return None

        created_dt = datetime.fromisoformat(
            tl_dict["created_at"].replace("Z", "+00:00")
        )

        # PR number: prefer enriched field, fall back to URL parsing for legacy dumps
        pr_number = tl_dict.get("pull_request_number")
        if pr_number is None:
            url = tl_dict.get("url", "")
            try:
                pr_number = int(url.rstrip("/").split("/")[-2])
            except (ValueError, IndexError):
                return None

        # requested_reviewer for review_demand
        requested = None
        if tl_dict["event"] == "review_requested":
            req_dict = (
                tl_dict.get("requested_reviewer")
                or tl_dict.get("review_requester")
            )
            if req_dict:
                try:
                    requested = ensure_user_fn(req_dict)
                except (ValueError, KeyError, TypeError):
                    pass

        return TimelineEvent(
            id=tl_dict["id"],
            node_id=tl_dict.get("node_id"),
            url=tl_dict.get("url"),
            event=tl_dict["event"],
            actor=actor,
            created_at=created_dt,
            pull_request_number=pr_number,
            commit_id=tl_dict.get("commit_id"),
            commit_url=tl_dict.get("commit_url"),
            comment_id=tl_dict.get("comment_id"),
            state=tl_dict.get("state"),
            html_url=tl_dict.get("html_url"),
            requested_reviewer=requested,
        )

    def parse_dump(self, dump_path: str) -> CanonicalBundle:
        path = Path(dump_path)

        # Manifest drives user + date window
        manifest_path = path / "dump_manifest.json"
        if not manifest_path.exists():
            raise ManifestNotFoundError(
                f"Manifest file not found: {manifest_path}", path=str(manifest_path)
            )
        try:
            manifest = json.loads(manifest_path.read_text())
        except json.JSONDecodeError as e:
            raise ManifestInvalidError(f"Invalid JSON in manifest: {e}", path=str(manifest_path)) from e
        user_login: str = manifest["user"]
        start_dt = datetime.fromisoformat(manifest["from"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(manifest["to"].replace("Z", "+00:00"))
        # User TZ from manifest (e.g. Europe/Istanbul for Turkey; data is UTC/Z)
        user_tz = manifest.get("user_timezone")

        # Robust canonical path: support top-level dump or direct canonical/ subdir (for tests/report).
        # Mistake surfaced: prior hardcoded subdir caused 0-load when passing canonical dir (msyavuz data reviewer-only; Vanessa PRs present but filtered).
        # No business logic affected (user filter correct per manifest=msyavuz).
        canonical_path = path / "canonical"
        if not canonical_path.exists():
            canonical_path = path

        users: dict[int, User] = {}
        repos: dict[int, Repository] = {}
        pr_raw: dict[int, dict] = {}
        commits: list[Commit] = []
        reviews: list[ReviewRecord] = []
        comments: list[CommentRecord] = []
        files: list[FileRecord] = []
        timeline_events: list[TimelineEvent] = []

        acted_pr_numbers: set[int] = set()  # PRs where vetted user acted

        def ensure_user(user_dict: dict) -> User:
            """
            Normalize missing type and cache users.
            """
            if not user_dict:
                raise ValueError("Missing user")
            # Default to regular user if type missing
            utype = user_dict.get("type") or UserType.USER.value
            normalized = {**user_dict, "type": utype}
            uid = normalized["id"]
            if uid not in users:
                users[uid] = User(**normalized)
            return users[uid]

        # ---------------------------
        # Pull requests (raw storage)
        # ---------------------------
        pr_file = canonical_path / "pull_requests.jsonl"
        if pr_file.exists():
            with pr_file.open() as f:
                for line in f:
                    pr_dict = json.loads(line)
                    created_at = datetime.fromisoformat(
                        pr_dict["created_at"].replace("Z", "+00:00")
                    )
                    if not (start_dt <= created_at <= end_dt):
                        continue
                    pr_raw[pr_dict["number"]] = pr_dict
                    # Author counts as action
                    if pr_dict.get("user", {}).get("login") == user_login:
                        acted_pr_numbers.add(pr_dict["number"])

        # ---------------------------
        # Reviews
        # ---------------------------
        review_file = canonical_path / "reviews.jsonl"
        if review_file.exists():
            with review_file.open() as f:
                for line in f:
                    review_dict = json.loads(line)
                    submitted_at = datetime.fromisoformat(
                        review_dict["submitted_at"].replace("Z", "+00:00")
                    )
                    if not (start_dt <= submitted_at <= end_dt):
                        continue

                    pr_number = int(review_dict["pull_request_url"].split("/")[-1])
                    if pr_number not in pr_raw:
                        # skip reviews for PRs outside window
                        continue

                    user = ensure_user(review_dict["user"])
                    state_norm = review_dict["state"].lower()
                    if state_norm not in {e.value for e in ReviewState}:
                        state_norm = ReviewState.COMMENTED.value

                    reviews.append(
                        ReviewRecord(
                            id=review_dict["id"],
                            user=user,
                            body=review_dict.get("body"),
                            state=ReviewState(state_norm),
                            submitted_at=submitted_at,
                            pull_request_number=pr_number,
                        )
                    )
                    if user.login == user_login:
                        acted_pr_numbers.add(pr_number)

        # ---------------------------
        # Commits
        # ---------------------------
        commit_file = canonical_path / "commits.jsonl"
        if commit_file.exists():
            with commit_file.open() as f:
                for line in f:
                    commit_dict = json.loads(line)
                    meta = commit_dict.get("commit") or {}
                    meta_author = meta.get("author") or {}
                    commit_dt_raw = meta_author.get("date")
                    if not commit_dt_raw:
                        continue
                    commit_dt = datetime.fromisoformat(commit_dt_raw.replace("Z", "+00:00"))
                    if not (start_dt <= commit_dt <= end_dt):
                        continue

                    pr_number = commit_dict.get("pull_request_number")
                    if pr_number is None or pr_number not in pr_raw:
                        continue

                    author_dict = commit_dict.get("author")
                    committer_dict = commit_dict.get("committer") or author_dict
                    if not (author_dict and committer_dict):
                        continue

                    try:
                        author = ensure_user(author_dict)
                        committer = ensure_user(committer_dict)
                    except (ValueError, KeyError, TypeError) as e:
                        log.debug(
                            "Skipping commit %s: invalid user data - %s",
                            commit_dict.get("sha", "unknown"),
                            e,
                        )
                        continue

                    message = meta.get("message")
                    if not message:
                        continue

                    commits.append(
                        Commit(
                            sha=commit_dict["sha"],
                            author=author,
                            committer=committer,
                            message=message,
                            date=commit_dt,
                            pull_request_number=pr_number,
                            idx=commit_dict.get("idx"),
                        )
                    )
                    if author.login == user_login:
                        acted_pr_numbers.add(pr_number)

        # ---------------------------
        # Review comments
        # ---------------------------
        rc_file = canonical_path / "review_comments.jsonl"
        if rc_file.exists():
            with rc_file.open() as f:
                for line in f:
                    comment_dict = json.loads(line)
                    pr_number = int(comment_dict["pull_request_url"].split("/")[-1])
                    created_at = datetime.fromisoformat(
                        comment_dict["created_at"].replace("Z", "+00:00")
                    )
                    if not (start_dt <= created_at <= end_dt):
                        continue
                    if pr_number not in pr_raw:
                        continue

                    user = ensure_user(comment_dict["user"])
                    comments.append(
                        CommentRecord(
                            id=comment_dict["id"],
                            user=user,
                            body=comment_dict["body"],
                            created_at=created_at,
                            updated_at=comment_dict.get("updated_at"),
                            type=CommentType.REVIEW,
                            pull_request_number=pr_number,
                            review_id=comment_dict.get("pull_request_review_id"),
                            in_reply_to_id=comment_dict.get("in_reply_to_id"),
                            path=comment_dict.get("path"),
                            position=comment_dict.get("position"),
                        )
                    )
                    if user.login == user_login:
                        acted_pr_numbers.add(pr_number)

        # ---------------------------
        # Issue comments (PR thread)
        # ---------------------------
        ic_file = canonical_path / "issue_comments.jsonl"
        if ic_file.exists():
            with ic_file.open() as f:
                for line in f:
                    comment_dict = json.loads(line)
                    issue_number = int(comment_dict["issue_url"].split("/")[-1])
                    created_at = datetime.fromisoformat(
                        comment_dict["created_at"].replace("Z", "+00:00")
                    )
                    if not (start_dt <= created_at <= end_dt):
                        continue
                    if issue_number not in pr_raw:
                        continue

                    user = ensure_user(comment_dict["user"])
                    comments.append(
                        CommentRecord(
                            id=comment_dict["id"],
                            user=user,
                            body=comment_dict["body"],
                            created_at=created_at,
                            updated_at=comment_dict.get("updated_at"),
                            type=CommentType.ISSUE,
                            pull_request_number=issue_number,
                            review_id=None,
                            in_reply_to_id=None,
                            path=None,
                            position=None,
                        )
                    )
                    if user.login == user_login:
                        acted_pr_numbers.add(issue_number)

        # ---------------------------
        # Timeline events
        # ---------------------------
        tl_file = canonical_path / "timeline.jsonl"
        if tl_file.exists():
            with tl_file.open() as f:
                for line in f:
                    tl_dict = json.loads(line)
                    evt = self._normalize_timeline_event(tl_dict, ensure_user)
                    if evt is None:
                        continue
                    if evt.pull_request_number not in pr_raw:
                        continue
                    if not (start_dt <= evt.created_at <= end_dt):
                        continue
                    timeline_events.append(evt)
                    if evt.actor.login == user_login:
                        acted_pr_numbers.add(evt.pull_request_number)

        # ---------------------------
        # Files
        # ---------------------------
        files_file = canonical_path / "files.jsonl"
        if files_file.exists():
            with files_file.open() as f:
                for line in f:
                    file_dict = json.loads(line)
                    pr_number = file_dict.get("pull_request_number")
                    if pr_number in pr_raw:
                        files.append(
                            FileRecord(
                                sha=file_dict["sha"],
                                filename=file_dict["filename"],
                                additions=file_dict["additions"],
                                deletions=file_dict["deletions"],
                                changes=file_dict["changes"],
                                status=file_dict["status"],
                                pull_request_number=pr_number,
                                # Patch loaded for rework (hunk-based line overlap)
                                patch=file_dict.get("patch"),
                            )
                        )

        # ---------------------------
        # Build PR objects now that acted_pr_numbers is known
        # ---------------------------
        prs: list[PullRequest] = []
        for pr_number, pr_dict in pr_raw.items():
            include = pr_dict["user"]["login"] == user_login or pr_number in acted_pr_numbers
            if not include:
                continue

            repo_dict = pr_dict["base"]["repo"]
            owner = ensure_user(
                {
                    **repo_dict["owner"],
                    "type": repo_dict["owner"].get("type") or UserType.ORGANIZATION.value,
                }
            )
            repo_id = repo_dict["id"]
            if repo_id not in repos:
                repos[repo_id] = Repository(
                    id=repo_id,
                    name=repo_dict["name"],
                    full_name=repo_dict["full_name"],
                    owner=owner,
                )
            repo = repos[repo_id]

            base_user = ensure_user(pr_dict["base"]["user"])
            head_user = ensure_user(pr_dict["head"]["user"])
            base = Branch(
                label=pr_dict["base"]["label"],
                ref=pr_dict["base"]["ref"],
                sha=pr_dict["base"]["sha"],
                user=base_user,
                repo=repo,
            )
            head = Branch(
                label=pr_dict["head"]["label"],
                ref=pr_dict["head"]["ref"],
                sha=pr_dict["head"]["sha"],
                user=head_user,
                repo=repo,
            )

            merged_by = None
            if pr_dict.get("merged_by"):
                merged_by = ensure_user(pr_dict["merged_by"])

            prs.append(
                PullRequest(
                    id=pr_dict["id"],
                    number=pr_number,
                    title=pr_dict["title"],
                    body=pr_dict.get("body"),
                    state=PullRequestState(pr_dict["state"]),
                    user=ensure_user(pr_dict["user"]),
                    created_at=datetime.fromisoformat(pr_dict["created_at"].replace("Z", "+00:00")),
                    updated_at=(
                        datetime.fromisoformat(pr_dict["updated_at"].replace("Z", "+00:00"))
                        if pr_dict.get("updated_at")
                        else None
                    ),
                    closed_at=(
                        datetime.fromisoformat(pr_dict["closed_at"].replace("Z", "+00:00"))
                        if pr_dict.get("closed_at")
                        else None
                    ),
                    merged_at=(
                        datetime.fromisoformat(pr_dict["merged_at"].replace("Z", "+00:00"))
                        if pr_dict.get("merged_at")
                        else None
                    ),
                    draft=pr_dict.get("draft", False),
                    merged=pr_dict.get("merged", False),
                    merge_commit_sha=pr_dict.get("merge_commit_sha"),
                    repository=repo,
                    base=base,
                    head=head,
                    commits=pr_dict.get("commits", 0),
                    additions=pr_dict.get("additions", 0),
                    deletions=pr_dict.get("deletions", 0),
                    changed_files=pr_dict.get("changed_files", 0),
                    merged_by=merged_by,
                    comments=pr_dict.get("comments", 0),
                    review_comments=pr_dict.get("review_comments", 0),
                )
            )

        keep_pr_numbers = {pr.number for pr in prs}
        commits = [c for c in commits if c.pull_request_number in keep_pr_numbers]
        reviews = [r for r in reviews if r.pull_request_number in keep_pr_numbers]
        comments = [c for c in comments if c.pull_request_number in keep_pr_numbers]
        files = [f for f in files if f.pull_request_number in keep_pr_numbers]
        timeline_events = [t for t in timeline_events if t.pull_request_number in keep_pr_numbers]

        return CanonicalBundle(
            users=list(users.values()),
            repositories=list(repos.values()),
            pull_requests=prs,
            commits=commits,
            reviews=reviews,
            comments=comments,
            files=files,
            timeline=timeline_events,
            user_timezone=user_tz,
        )
