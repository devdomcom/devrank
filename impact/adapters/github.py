import json
import logging
import re
from datetime import datetime
from pathlib import Path

from impact.adapters.base import ProviderAdapter
from impact.exceptions import ManifestInvalidError, ManifestNotFoundError

log = logging.getLogger(__name__)
from impact.domain.models import (
    Branch,
    CanonicalBundle,
    CIRunRecord,
    CommentRecord,
    CommentType,
    Commit,
    DeploymentRecord,
    FileRecord,
    PullRequest,
    PullRequestState,
    ReleaseRecord,
    Repository,
    ReviewRecord,
    ReviewState,
    TimelineEvent,
    User,
    UserType,
)

# GitHub-specific code-suggestion block pattern (```suggestion ... ```)
_GITHUB_SUGGESTION_RE = re.compile(r"```suggestion\b", re.IGNORECASE)


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
    def _is_github_bot(user_dict: dict) -> bool:
        """GitHub-specific bot detection (three-layer defense-in-depth).

        Layer 1: ``user.type == "Bot"`` (GitHub API; most reliable)
        Layer 2: login ends with ``[bot]`` (GitHub Apps naming convention)
        Layer 3: ``node_id`` starts with ``BOT_`` (GraphQL global-ID prefix)

        The critical edge case: GitHub Copilot's inline-comment identity is
        ``Copilot`` (type=Bot, **no** ``[bot]`` suffix).  A suffix-only check
        would mis-classify it as human.
        """
        utype = user_dict.get("type", "")
        if utype == UserType.BOT.value or utype == UserType.BOT:
            return True
        if isinstance(utype, str) and utype == "Bot":
            return True
        if (user_dict.get("login") or "").endswith("[bot]"):
            return True
        if (user_dict.get("node_id") or "").startswith("BOT_"):
            return True
        return False

    @staticmethod
    def _has_github_suggestion(body: str | None) -> bool:
        """Detect GitHub-native ````` ```suggestion ````` blocks in a comment."""
        if not body:
            return False
        return bool(_GITHUB_SUGGESTION_RE.search(body))

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
            """Normalize missing type, set canonical ``is_bot``, and cache users."""
            if not user_dict:
                raise ValueError("Missing user")
            # Default to regular user if type missing
            utype = user_dict.get("type") or UserType.USER.value
            normalized = {**user_dict, "type": utype}
            uid = normalized["id"]
            if uid not in users:
                users[uid] = User(
                    id=uid,
                    login=normalized["login"],
                    avatar_url=normalized.get("avatar_url"),
                    type=utype,
                    node_id=normalized.get("node_id"),
                    is_bot=GitHubAdapter._is_github_bot(normalized),
                )
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
                    # Include PRs that were *active* during the window:
                    #   created before window end  AND  not fully closed before window start.
                    # A PR "closed before start" means it has a closed_at < start_dt
                    # and was not updated during the window (no review/comment activity).
                    if created_at > end_dt:
                        continue
                    closed_raw = pr_dict.get("closed_at")
                    if closed_raw:
                        closed_at = datetime.fromisoformat(closed_raw.replace("Z", "+00:00"))
                        updated_raw = pr_dict.get("updated_at")
                        updated_at = (
                            datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
                            if updated_raw else closed_at
                        )
                        # Fully resolved before window AND no updates during window → skip
                        if closed_at < start_dt and updated_at < start_dt:
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

                    # Git DAG parent count for merge-commit detection
                    parents = commit_dict.get("parents")
                    parent_count = len(parents) if isinstance(parents, list) else 1

                    commits.append(
                        Commit(
                            sha=commit_dict["sha"],
                            author=author,
                            committer=committer,
                            message=message,
                            date=commit_dt,
                            pull_request_number=pr_number,
                            idx=commit_dict.get("idx"),
                            parent_count=parent_count,
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
                    body = comment_dict.get("body") or ""
                    comments.append(
                        CommentRecord(
                            id=comment_dict["id"],
                            user=user,
                            body=body,
                            created_at=created_at,
                            updated_at=comment_dict.get("updated_at"),
                            type=CommentType.REVIEW,
                            pull_request_number=pr_number,
                            review_id=comment_dict.get("pull_request_review_id"),
                            in_reply_to_id=comment_dict.get("in_reply_to_id"),
                            path=comment_dict.get("path"),
                            position=comment_dict.get("position"),
                            has_code_suggestion=self._has_github_suggestion(body),
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
                    if pr_number in pr_raw and file_dict.get("sha") is not None:
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
                                # Full file content for AST analysis (tree-sitter)
                                content=file_dict.get("content"),
                            )
                        )

        # ---------------------------
        # Releases (repo-scoped, DORA)
        # ---------------------------
        release_records: list[ReleaseRecord] = []
        rel_file = canonical_path / "releases.jsonl"
        if rel_file.exists():
            with rel_file.open() as f:
                for line in f:
                    rel = json.loads(line)
                    created_at = datetime.fromisoformat(
                        rel["created_at"].replace("Z", "+00:00")
                    )
                    if created_at < start_dt or created_at > end_dt:
                        continue
                    author = None
                    if rel.get("author"):
                        try:
                            author = ensure_user(rel["author"])
                        except (ValueError, KeyError, TypeError):
                            pass
                    published_at = None
                    if rel.get("published_at"):
                        published_at = datetime.fromisoformat(
                            rel["published_at"].replace("Z", "+00:00")
                        )
                    release_records.append(
                        ReleaseRecord(
                            id=rel["id"],
                            tag_name=rel["tag_name"],
                            name=rel.get("name"),
                            created_at=created_at,
                            published_at=published_at,
                            draft=rel.get("draft", False),
                            prerelease=rel.get("prerelease", False),
                            author=author,
                            target_commitish=rel.get("target_commitish"),
                        )
                    )

        # ---------------------------
        # Deployments (repo-scoped, DORA)
        # ---------------------------
        deployment_records: list[DeploymentRecord] = []
        dep_file = canonical_path / "deployments.jsonl"
        if dep_file.exists():
            with dep_file.open() as f:
                for line in f:
                    dep = json.loads(line)
                    created_at = datetime.fromisoformat(
                        dep["created_at"].replace("Z", "+00:00")
                    )
                    if created_at < start_dt or created_at > end_dt:
                        continue
                    creator = None
                    if dep.get("creator"):
                        try:
                            creator = ensure_user(dep["creator"])
                        except (ValueError, KeyError, TypeError):
                            pass
                    updated_at = None
                    if dep.get("updated_at"):
                        updated_at = datetime.fromisoformat(
                            dep["updated_at"].replace("Z", "+00:00")
                        )
                    deployment_records.append(
                        DeploymentRecord(
                            id=dep["id"],
                            sha=dep["sha"],
                            ref=dep["ref"],
                            environment=dep.get("environment", "unknown"),
                            created_at=created_at,
                            updated_at=updated_at,
                            creator=creator,
                            description=dep.get("description"),
                        )
                    )

        # ---------------------------
        # CI runs (repo-scoped, DORA / flow)
        # ---------------------------
        ci_run_records: list[CIRunRecord] = []
        ci_file = canonical_path / "ci_runs.jsonl"
        if ci_file.exists():
            with ci_file.open() as f:
                for line in f:
                    run = json.loads(line)
                    created_at = datetime.fromisoformat(
                        run["created_at"].replace("Z", "+00:00")
                    )
                    if created_at < start_dt or created_at > end_dt:
                        continue
                    updated_at = None
                    if run.get("updated_at"):
                        updated_at = datetime.fromisoformat(
                            run["updated_at"].replace("Z", "+00:00")
                        )
                    run_started_at = None
                    if run.get("run_started_at"):
                        run_started_at = datetime.fromisoformat(
                            run["run_started_at"].replace("Z", "+00:00")
                        )
                    # Compute duration from run_started_at to updated_at (completed)
                    duration_seconds = None
                    if run_started_at and updated_at:
                        duration_seconds = int(
                            (updated_at - run_started_at).total_seconds()
                        )
                    # Extract PR number from pull_requests array if present
                    pr_number = None
                    pr_list = run.get("pull_requests", [])
                    if pr_list:
                        pr_number = pr_list[0].get("number")
                    ci_run_records.append(
                        CIRunRecord(
                            id=run["id"],
                            name=run.get("name"),
                            head_sha=run.get("head_sha", ""),
                            event=run.get("event"),
                            status=run.get("status"),
                            conclusion=run.get("conclusion"),
                            created_at=created_at,
                            updated_at=updated_at,
                            run_started_at=run_started_at,
                            pull_request_number=pr_number,
                            duration_seconds=duration_seconds,
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
                    labels=[l["name"] for l in pr_dict.get("labels", []) if isinstance(l, dict) and "name" in l],
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
            releases=release_records,
            deployments=deployment_records,
            ci_runs=ci_run_records,
            user_timezone=user_tz,
        )
