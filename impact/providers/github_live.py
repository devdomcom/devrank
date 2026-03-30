from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from impact.adapters.github import GitHubAdapter  # reusing adapter for parsing canonical dump
from impact.domain.models import CanonicalBundle
from impact.persistence.filesystem import FileSystemDumpWriter
from impact.providers.base import FetchConfig, ProviderFetcher
from impact.providers.github.client import GitHubClient
from impact.providers.github.fetcher import GitHubFetcher


@dataclass
class LiveFetchConfig(FetchConfig):
    """GitHub-specific fetch configuration.

    Extends the provider-neutral ``FetchConfig`` with GitHub-only options.
    """

    fetch_contents: bool = False


class GitHubLiveFetcher(ProviderFetcher):
    """Fetch live GitHub data for a user-selected repo set and time window.

    Writes a canonical dump via the persistence layer and returns a
    ``CanonicalBundle``.  Implements the ``ProviderFetcher`` interface so
    that the fetch pipeline can dispatch to it via the fetcher registry.
    """

    def __init__(self, cfg: FetchConfig):
        # Accept any FetchConfig; extract GitHub-specific options gracefully.
        self.cfg = cfg
        self._fetch_contents: bool = getattr(cfg, "fetch_contents", False)

    def run(self) -> CanonicalBundle:
        client = GitHubClient(self.cfg.token)
        fetcher = GitHubFetcher(client)
        writer = FileSystemDumpWriter(self.cfg.out_dir)
        log = logging.getLogger(__name__)

        # Pre-flight: if rate limit is low, sleep until reset to avoid immediate 403s.
        try:
            rate = client.get("/rate_limit").json()["resources"]["core"]
            remaining = rate.get("remaining", 0)
            reset = rate.get("reset")
            if remaining < 50 and reset:
                sleep_for = max(int(reset) - int(datetime.now(UTC).timestamp()), 0) + 5
                log.warning(
                    "Low rate limit (%s remaining). Sleeping %ss before fetch.",
                    remaining,
                    sleep_for,
                )
                time.sleep(sleep_for)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not preflight rate limit check: %s", exc)

        manifest = {
            "provider": "github",
            "api_version": "2022-11-28",
            "user": self.cfg.user_login,
            "from": self.cfg.start.isoformat().replace("+00:00", "Z"),
            "to": self.cfg.end.isoformat().replace("+00:00", "Z"),
            "repositories": self.cfg.repos,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "notes": self.cfg.notes or "Live fetch dump",
        }
        if self.cfg.user_timezone:
            manifest["user_timezone"] = self.cfg.user_timezone
        writer.write_manifest(manifest)

        pr_numbers: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()
        for repo in self.cfg.repos:
            prs = fetcher.list_prs(repo, since=self.cfg.start, until=self.cfg.end)
            for pr in prs:
                login = pr.get("user", {}).get("login")
                # Stage 1: author-based inclusion
                include = login == self.cfg.user_login
                # Stage 2: activity-based inclusion — only pull PRs where the user actually acted
                if not include:
                    # cheap look at PR to see if user is a requested reviewer or assignee
                    requested = [r.get("login") for r in pr.get("requested_reviewers", [])]
                    assignees = [a.get("login") for a in pr.get("assignees", [])]
                    if self.cfg.user_login in requested or self.cfg.user_login in assignees:
                        include = True

                if include:
                    key = (repo, pr["number"])
                    if key not in seen:
                        seen.add(key)
                        pr_numbers.append(key)

            # Stage 3: Search for PRs the user reviewed but was NOT the
            # author/assignee/requested-reviewer of.  The /pulls list only
            # exposes *current* requested_reviewers — once a review is
            # submitted the user disappears from that field.  The Search API
            # ``reviewed-by:`` qualifier captures historical reviews.
            start_iso = self.cfg.start.strftime("%Y-%m-%d")
            end_iso = self.cfg.end.strftime("%Y-%m-%d")
            query = (
                f"is:pr reviewed-by:{self.cfg.user_login} "
                f"repo:{repo} updated:{start_iso}..{end_iso}"
            )
            try:
                reviewed_prs = client.search_issues(query)
                for item in reviewed_prs:
                    number = item.get("number")
                    if number:
                        key = (repo, number)
                        if key not in seen:
                            seen.add(key)
                            pr_numbers.append(key)
                log.info(
                    "Search reviewed-by:%s in %s found %s PRs (%s new)",
                    self.cfg.user_login, repo, len(reviewed_prs),
                    len(reviewed_prs) - sum(1 for i in reviewed_prs if (repo, i.get("number")) in seen and i.get("number")),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Search for reviewed-by:%s in %s failed (non-fatal): %s",
                    self.cfg.user_login, repo, exc,
                )

        log.info(
            "Queued %s pull requests for fetch after author/activity/review prefilter",
            len(pr_numbers),
        )

        # Parallelize lightly to avoid hammering the API; also we rely on client backoff.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future_to_pr = {
                executor.submit(
                    fetcher.fetch_pr_bundle, repo, number,
                    fetch_contents=self._fetch_contents,
                ): (repo, number)
                for repo, number in pr_numbers
            }
            for future in concurrent.futures.as_completed(future_to_pr):
                repo, number = future_to_pr[future]
                try:
                    bundle = future.result()
                    writer.write_pr_bundle(bundle)
                    log.info("Fetched PR %s#%s", repo, number)
                except Exception as exc:
                    log.error("Failed fetching PR %s#%s: %s", repo, number, exc)
                time.sleep(1.0)  # small delay between PRs to ease rate limits

        client.close()

        adapter = GitHubAdapter()
        return adapter.parse_dump(str(self.cfg.out_dir))

    def check_health(self) -> bool:
        """Verify GitHub API connectivity and token validity."""
        try:
            client = GitHubClient(self.cfg.token)
            resp = client.get("/rate_limit")
            client.close()
            return resp.status_code == 200
        except Exception:  # noqa: BLE001
            return False
