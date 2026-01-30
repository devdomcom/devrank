from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

from impact.providers.github.client import GitHubClient


class GitHubFetcher:
    """
    Fetches GitHub PR-centric data for a set of repos and time window.
    Designed to output raw API payloads; transformation/anonymization happens downstream.
    """

    def __init__(self, client: GitHubClient):
        self.client = client

    def _since_param(self, since: Optional[datetime]) -> Optional[str]:
        if not since:
            return None
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return since.isoformat().replace("+00:00", "Z")

    def list_prs(self, repo: str, since: Optional[datetime], until: Optional[datetime]) -> List[Dict[str, Any]]:
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

    def fetch_pr_bundle(self, repo: str, number: int) -> Dict[str, Any]:
        bundle: Dict[str, Any] = {}
        bundle["pull_request"] = self.client.get(f"/repos/{repo}/pulls/{number}").json()
        bundle["timeline"] = list(self.client.paginate(f"/repos/{repo}/issues/{number}/timeline"))
        bundle["reviews"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/reviews"))
        bundle["review_comments"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/comments"))
        bundle["issue_comments"] = list(self.client.paginate(f"/repos/{repo}/issues/{number}/comments"))
        bundle["commits"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/commits"))
        bundle["files"] = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/files"))
        return bundle


__all__ = ["GitHubFetcher"]
