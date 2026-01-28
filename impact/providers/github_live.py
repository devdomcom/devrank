from __future__ import annotations

import json
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
            pr_numbers.extend([(repo, pr["number"]) for pr in prs])

        for repo, number in pr_numbers:
            bundle = fetcher.fetch_pr_bundle(repo, number)
            writer.write_pr_bundle(bundle)

        client.close()

        adapter = GitHubAdapter()
        return adapter.parse_dump(str(self.cfg.out_dir))
