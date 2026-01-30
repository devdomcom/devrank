from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


class FileSystemDumpWriter:
    """
    Writes canonical GitHub dump files to a target directory.
    """

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.canonical_dir = self.base_dir / "canonical"
        self.canonical_dir.mkdir(parents=True, exist_ok=True)

    def write_manifest(self, manifest: Dict):
        (self.base_dir / "dump_manifest.json").write_text(json.dumps(manifest, indent=2))

    def write_pr_bundle(self, bundle: Dict):
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
                        f.write(json.dumps(item) + "\n")
                else:
                    f.write(json.dumps(data) + "\n")
