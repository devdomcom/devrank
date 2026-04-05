from __future__ import annotations

import json
from pathlib import Path


_CANONICAL_FILES = (
    "pull_requests.jsonl",
    "reviews.jsonl",
    "review_comments.jsonl",
    "issue_comments.jsonl",
    "commits.jsonl",
    "files.jsonl",
    "timeline.jsonl",
    "releases.jsonl",
    "deployments.jsonl",
    "ci_runs.jsonl",
)


class FileSystemDumpWriter:
    """Writes canonical dump files to a target directory.

    Provider-neutral: the canonical file names (``pull_requests.jsonl``,
    ``reviews.jsonl``, etc.) are consistent with the canonical domain
    model, not tied to any specific hosting provider.
    """

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.canonical_dir = self.base_dir / "canonical"
        self.canonical_dir.mkdir(parents=True, exist_ok=True)
        # Remove stale JSONL from a prior run so re-fetches don't duplicate.
        for fname in _CANONICAL_FILES:
            p = self.canonical_dir / fname
            if p.exists():
                p.unlink()

    def write_manifest(self, manifest: dict):
        (self.base_dir / "dump_manifest.json").write_text(json.dumps(manifest, indent=2))

    def write_pr_bundle(self, bundle: dict):
        # Each key writes to its respective jsonl
        pr_number = bundle.get("pull_request", {}).get("number")
        files = {
            "pull_request": "pull_requests.jsonl",
            "reviews": "reviews.jsonl",
            "review_comments": "review_comments.jsonl",
            "issue_comments": "issue_comments.jsonl",
            "commits": "commits.jsonl",
            "files": "files.jsonl",
            "timeline": "timeline.jsonl",
        }
        for key, fname in files.items():
            data = bundle.get(key)
            if data is None:
                continue
            path = self.canonical_dir / fname
            with path.open("a") as f:
                if isinstance(data, list):
                    for idx, item in enumerate(data):
                        # Enrich commits and files with PR context for downstream parsing.
                        if key == "commits":
                            item = dict(item)
                            item["pull_request_number"] = pr_number
                            item["idx"] = idx
                        if key == "files":
                            item = dict(item)
                            item["pull_request_number"] = pr_number
                        if key == "timeline":
                            item = dict(item)
                            item["pull_request_number"] = pr_number
                        f.write(json.dumps(item) + "\n")
                else:
                    f.write(json.dumps(data) + "\n")

    def write_repo_data(self, kind: str, records: list[dict], repo: str):
        """Write repo-scoped records (releases, deployments, ci_runs) to JSONL.

        Each record is enriched with the source ``repository`` full name so the
        adapter can group/filter by repo downstream.
        """
        fname_map = {
            "releases": "releases.jsonl",
            "deployments": "deployments.jsonl",
            "ci_runs": "ci_runs.jsonl",
        }
        fname = fname_map.get(kind)
        if not fname or not records:
            return
        path = self.canonical_dir / fname
        with path.open("a") as f:
            for record in records:
                record = dict(record)
                record["_repository"] = repo
                f.write(json.dumps(record) + "\n")
