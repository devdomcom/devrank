from __future__ import annotations

import base64
import logging
import time
from datetime import UTC, datetime
from typing import Any

from impact.providers.github.client import GitHubClient, GitHubRateLimitError

log = logging.getLogger(__name__)

# Extensions for which we fetch full file content (source files only, not binary)
_SOURCE_EXTENSIONS: frozenset[str] = frozenset({
    ".py", ".js", ".jsx", ".ts", ".tsx", ".go", ".rs", ".java", ".kt",
    ".c", ".cpp", ".cc", ".h", ".hpp", ".cs", ".rb", ".swift", ".scala",
    ".dart", ".lua", ".r", ".jl", ".ex", ".exs", ".clj", ".hs",
    ".sh", ".bash", ".zsh", ".sql", ".graphql", ".proto",
    ".vue", ".svelte", ".astro",
})

# Throttle blob fetches: max N per second to avoid secondary rate limits
_BLOB_FETCH_DELAY = 0.1  # 100ms between blob requests


def _is_fetchable_source(filename: str, status: str) -> bool:
    """Should we fetch full content for this file?"""
    if status in ("removed", "deleted"):
        return False
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return ext in _SOURCE_EXTENSIONS


class GitHubFetcher:
    """
    Fetches GitHub PR-centric data for a set of repos and time window.
    Designed to output raw API payloads; transformation/anonymization happens downstream.
    """

    def __init__(self, client: GitHubClient):
        self.client = client
        # SHA -> content cache for deduplication across files/PRs
        self._blob_cache: dict[str, str | None] = {}

    def _since_param(self, since: datetime | None) -> str | None:
        if not since:
            return None
        if since.tzinfo is None:
            since = since.replace(tzinfo=UTC)
        return since.isoformat().replace("+00:00", "Z")

    def _fetch_blob_content(self, repo: str, sha: str) -> str | None:
        """Fetch file content via Git Blobs API. Returns decoded text or None."""
        if sha in self._blob_cache:
            return self._blob_cache[sha]
        try:
            # Throttle blob fetches to avoid secondary rate limits
            time.sleep(_BLOB_FETCH_DELAY)
            resp = self.client.get(f"/repos/{repo}/git/blobs/{sha}")
            data = resp.json()
            encoding = data.get("encoding", "")
            raw = data.get("content", "")
            if encoding == "base64":
                content = base64.b64decode(raw).decode("utf-8", errors="replace")
            elif encoding == "utf-8":
                content = raw
            else:
                content = None  # binary or unknown encoding
            self._blob_cache[sha] = content
            return content
        except GitHubRateLimitError:
            # Don't cache rate limit failures — retry should succeed later
            log.warning("Rate limited fetching blob %s/%s, skipping", repo, sha)
            return None
        except Exception:
            log.debug("Failed to fetch blob %s/%s", repo, sha, exc_info=True)
            self._blob_cache[sha] = None
            return None

    def list_prs(
        self, repo: str, since: datetime | None, until: datetime | None
    ) -> list[dict[str, Any]]:
        params = {
            "state": "all",
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        }
        results = []
        for pr in self.client.paginate(f"/repos/{repo}/pulls", params=params):
            updated_at = datetime.fromisoformat(pr["updated_at"].replace("Z", "+00:00"))
            # API returns in descending updated order; stop when we fall below the lower bound.
            if since and updated_at < since:
                break
            if until and updated_at > until:
                continue
            results.append(pr)
        return results

    def fetch_pr_bundle(
        self, repo: str, number: int, *, fetch_contents: bool = False
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {}
        bundle["pull_request"] = self.client.get(f"/repos/{repo}/pulls/{number}").json()
        bundle["timeline"] = list(self.client.paginate(f"/repos/{repo}/issues/{number}/timeline"))
        bundle["reviews"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/reviews"))
        bundle["review_comments"] = list(
            self.client.paginate(f"/repos/{repo}/pulls/{number}/comments")
        )
        bundle["issue_comments"] = list(
            self.client.paginate(f"/repos/{repo}/issues/{number}/comments")
        )
        bundle["commits"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/commits"))
        bundle["files"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/files"))

        # Optionally fetch full file contents for AST analysis (tree-sitter)
        if fetch_contents:
            for file_dict in bundle["files"]:
                filename = file_dict.get("filename", "")
                status = file_dict.get("status", "")
                sha = file_dict.get("sha", "")
                if sha and _is_fetchable_source(filename, status):
                    content = self._fetch_blob_content(repo, sha)
                    if content is not None:
                        file_dict["content"] = content

        return bundle


__all__ = ["GitHubFetcher"]
