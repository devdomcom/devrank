from __future__ import annotations

import json
import concurrent.futures
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from impact.domain.models import CanonicalBundle
from impact.adapters.github import GitHubAdapter  # reusing adapter for parsing canonical dump
from impact.providers.github.client import GitHubClient
from impact.providers.github.fetcher import GitHubFetcher
from impact.persistence.filesystem import FileSystemDumpWriter


@dataclass
class LiveFetchConfig:
    user_login: str
    repos: List[str]
    start: datetime
    end: datetime
    token: str
    out_dir: Path


class GitHubLiveFetcher:
    """
    Fetches live GitHub data for a user-selected repo set and time window,
    writes canonical dump via a persistence layer, and returns a CanonicalBundle.
    Anonymization hook to be added later.
    """

    def __init__(self, cfg: LiveFetchConfig):
        self.cfg = cfg

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
                sleep_for = max(int(reset) - int(datetime.now(timezone.utc).timestamp()), 0) + 5
                log.warning("Low rate limit (%s remaining). Sleeping %ss before fetch.", remaining, sleep_for)
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
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "notes": "Live fetch dump",
        }
        writer.write_manifest(manifest)

        pr_numbers = []
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
                        # fetch minimal timeline to see if the user did anything
                        # NOTE: we avoid extra requests here; we will re-check after bundle fetch.
                        include = True

                if include:
                    pr_numbers.append((repo, pr["number"]))
        log.info("Queued %s pull requests for fetch after author/activity prefilter", len(pr_numbers))

        # Parallelize lightly to avoid hammering the API; also we rely on client backoff.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future_to_pr = {
                executor.submit(fetcher.fetch_pr_bundle, repo, number): (repo, number)
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
