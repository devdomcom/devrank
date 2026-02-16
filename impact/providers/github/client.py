from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

DEFAULT_BASE_URL = "https://api.github.com"
DEFAULT_ACCEPT = "application/vnd.github+json"

log = logging.getLogger(__name__)


class GitHubRateLimitError(Exception):
    pass


class GitHubSecondaryRateLimitError(GitHubRateLimitError):
    """HTTP 429 — secondary rate limit (abuse detection)."""
    pass


def _headers(token: str, accept: str = DEFAULT_ACCEPT, etag: str | None = None) -> dict[str, str]:
    h = {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
    }
    if etag:
        h["If-None-Match"] = etag
    return h


class GitHubClient:
    """
    Thin GitHub REST client with retry/backoff, rate limit awareness, and pagination.
    Synchronous interface via httpx.Client for CLI use.
    """

    # Pause proactively when remaining requests fall below this threshold
    _LOW_REMAINING_THRESHOLD = 50

    def __init__(self, token: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = httpx.Client(timeout=timeout, headers=_headers(token))

    def close(self) -> None:
        self.client.close()

    def _check_rate_limit(self, resp: httpx.Response) -> None:
        """Proactively sleep when rate limit remaining is low, before hitting 403."""
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None:
            try:
                rem = int(remaining)
                rst = int(reset)
            except ValueError:
                return
            if rem <= self._LOW_REMAINING_THRESHOLD:
                sleep_for = max(rst - int(time.time()), 0) + 1
                sleep_for = min(sleep_for, 900)  # cap 15 min
                log.warning(
                    "Rate limit low (%s remaining). Sleeping %ss until reset.",
                    rem, sleep_for,
                )
                time.sleep(sleep_for)

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, GitHubRateLimitError)),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(8),
        reraise=True,
    )
    def get(
        self, path: str, params: dict[str, Any] | None = None, etag: str | None = None
    ) -> httpx.Response:
        url = f"{self.base_url}{path}"
        headers = _headers(self.token, etag=etag)
        resp = self.client.get(url, params=params, headers=headers)

        if resp.status_code == 304:
            return resp

        # HTTP 429 — secondary rate limit (abuse detection)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                sleep_for = min(int(retry_after), 300)  # cap 5 min
            else:
                sleep_for = 60  # default 60s for secondary limits
            log.warning("HTTP 429 secondary rate limit. Sleeping %ss.", sleep_for)
            time.sleep(sleep_for)
            raise GitHubSecondaryRateLimitError(resp.text)

        # HTTP 403 — primary rate limit exhausted
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                sleep_for = max(int(reset) - int(time.time()), 0) + 1
                sleep_for = min(sleep_for, 900)  # cap 15 min
                log.warning("Primary rate limit exhausted. Sleeping %ss.", sleep_for)
                time.sleep(sleep_for)
            raise GitHubRateLimitError(resp.text)

        # HTTP 403 with Retry-After (sometimes GitHub uses 403 instead of 429)
        if resp.status_code == 403:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                sleep_for = min(int(retry_after), 300)
                log.warning("HTTP 403 with Retry-After. Sleeping %ss.", sleep_for)
                time.sleep(sleep_for)
                raise GitHubRateLimitError(resp.text)

        resp.raise_for_status()

        # Proactive check: sleep before next request if rate limit is getting low
        self._check_rate_limit(resp)

        return resp

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterable[dict[str, Any]]:
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


__all__ = ["GitHubClient", "GitHubRateLimitError", "GitHubSecondaryRateLimitError"]
