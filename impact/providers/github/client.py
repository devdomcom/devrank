from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Iterable, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_BASE_URL = "https://api.github.com"
DEFAULT_ACCEPT = "application/vnd.github+json"


class GitHubRateLimitError(Exception):
    pass


def _headers(token: str, accept: str = DEFAULT_ACCEPT, etag: Optional[str] = None) -> Dict[str, str]:
    h = {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
    }
    if etag:
        h["If-None-Match"] = etag
    return h


class GitHubClient:
    """
    Thin GitHub REST client with retry/backoff and simple pagination.
    Synchronous interface via httpx.Client for CLI use.
    """

    def __init__(self, token: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = httpx.Client(timeout=timeout, headers=_headers(token))

    def close(self):
        self.client.close()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, GitHubRateLimitError)),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get(self, path: str, params: Optional[Dict[str, Any]] = None, etag: Optional[str] = None) -> httpx.Response:
        url = f"{self.base_url}{path}"
        headers = _headers(self.token, etag=etag)
        resp = self.client.get(url, params=params, headers=headers)
        if resp.status_code == 304:
            return resp
        # Handle rate limiting proactively: sleep until reset then retry via tenacity.
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset = resp.headers.get("X-RateLimit-Reset")
            if remaining == "0" and reset:
                sleep_for = max(int(reset) - int(time.time()), 0) + 1
                time.sleep(min(sleep_for, 900))  # cap to 15 minutes
            raise GitHubRateLimitError(resp.text)
        resp.raise_for_status()
        return resp

    def paginate(self, path: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        """
        Follows GitHub Link headers. After the first request we stop sending the original params
        because the `next` URL already contains its own query string (including page).
        """
        params = params or {}
        next_path = path
        last_url = None
        while True:
            resp = self.get(next_path, params=params)
            if resp.status_code == 304:
                return
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data

            links = resp.headers.get("Link", "")
            next_link = None
            for part in links.split(","):
                if 'rel="next"' in part:
                    next_link = part[part.find("<") + 1 : part.find(">")]
                    break
            if not next_link or next_link == last_url:
                break
            last_url = next_link
            # next_link already contains query params; avoid duplicating by clearing params
            next_path = next_link.replace(self.base_url, "")
            params = None


__all__ = ["GitHubClient", "GitHubRateLimitError"]
