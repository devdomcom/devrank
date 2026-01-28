from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from impact.domain.models import CanonicalBundle
from impact.ingestion.base import Ingestion
from impact.providers.github.client import GitHubClient
from impact.providers.github.fetcher import GitHubFetcher
from impact.adapters.github import GitHubAdapter


@dataclass
class LiveFetchConfig:
    user_login: str
    repos: List[str]
    start: datetime
    end: datetime
    token: str
    out_dir: Optional[Path] = None  # if set, write canonical dump


class GitHubLiveIngestion(Ingestion):
    """
    Fetches live GitHub data for a user-selected repo set and time window,
    writes optional canonical dump, and returns a CanonicalBundle.
    Anonymization is a future hook; currently pass-through.
    """

    def __init__(self, cfg: LiveFetchConfig):
        self.cfg = cfg

    def ingest(self) -> CanonicalBundle:
        client = GitHubClient(self.cfg.token)
        fetcher = GitHubFetcher(client)
        pr_numbers_seen = set()
        out_dir = self.cfg.out_dir
        if out_dir:
            (out_dir / "canonical").mkdir(parents=True, exist_ok=True)

        # Collect raw bundles
        raw_prs = []
        for repo in self.cfg.repos:
            prs = fetcher.list_prs(repo, since=self.cfg.start, until=self.cfg.end)
            for pr in prs:
                number = pr["number"]
                pr_numbers_seen.add((repo, number))
                raw_prs.append((repo, number))

        # Dump manifest
        if out_dir:
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
            (out_dir / "dump_manifest.json").write_text(json.dumps(manifest, indent=2))

        # Write raw canonical files incrementally
        can_dir = out_dir / "canonical" if out_dir else None
        if can_dir:
            files = {
                "pull_requests": open(can_dir / "pull_requests.jsonl", "w"),
                "reviews": open(can_dir / "reviews.jsonl", "w"),
                "review_comments": open(can_dir / "review_comments.jsonl", "w"),
                "issue_comments": open(can_dir / "issue_comments.jsonl", "w"),
                "commits": open(can_dir / "commits.jsonl", "w"),
                "files": open(can_dir / "files.jsonl", "w"),
                "timeline": open(can_dir / "timeline.jsonl", "w"),
            }
        else:
            files = None

        try:
            for repo, number in raw_prs:
                bundle = fetcher.fetch_pr_bundle(repo, number)
                if files:
                    files["pull_requests"].write(json.dumps(bundle["pull_request"]) + "\n")
                    for item in bundle["reviews"]:
                        files["reviews"].write(json.dumps(item) + "\n")
                    for item in bundle["review_comments"]:
                        files["review_comments"].write(json.dumps(item) + "\n")
                    for item in bundle["issue_comments"]:
                        files["issue_comments"].write(json.dumps(item) + "\n")
                    for item in bundle["commits"]:
                        files["commits"].write(json.dumps(item) + "\n")
                    for item in bundle["files"]:
                        files["files"].write(json.dumps(item) + "\n")
                    for item in bundle["timeline"]:
                        files["timeline"].write(json.dumps(item) + "\n")
        finally:
            if files:
                for f in files.values():
                    f.close()
            client.close()

        # If we wrote a dump, reuse GitHubAdapter to parse it into CanonicalBundle
        if out_dir:
            adapter = GitHubAdapter()
            return adapter.parse_dump(str(out_dir))
        else:
            # Not writing to disk: assemble canonical bundle by reusing adapter parsing via temp dir
            raise NotImplementedError("Non-dump live ingestion not implemented yet")
