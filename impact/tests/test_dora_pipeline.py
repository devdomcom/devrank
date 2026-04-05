"""Tests for the DORA data pipeline: writer, adapter parsing, and ledger indexing
for releases, deployments, and CI runs."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from impact.adapters.github import GitHubAdapter
from impact.domain.models import (
    CIRunRecord,
    DeploymentRecord,
    ReleaseRecord,
)
from impact.ledger.ledger import Ledger
from impact.persistence.filesystem import FileSystemDumpWriter
from impact.tests.conftest import make_bundle, make_user


# ── Helpers ──────────────────────────────────────────────────────────

NOW = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
START = datetime(2026, 3, 1, tzinfo=UTC)
END = datetime(2026, 3, 31, tzinfo=UTC)


def _write_manifest(base: Path, *, user: str = "testuser"):
    manifest = {
        "provider": "github",
        "user": user,
        "from": START.isoformat().replace("+00:00", "Z"),
        "to": END.isoformat().replace("+00:00", "Z"),
        "repositories": ["org/repo"],
    }
    (base / "dump_manifest.json").write_text(json.dumps(manifest))


def _make_raw_release(*, id: int = 1, tag: str = "v1.0.0", created_at: datetime = NOW):
    return {
        "id": id,
        "tag_name": tag,
        "name": f"Release {tag}",
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "published_at": created_at.isoformat().replace("+00:00", "Z"),
        "draft": False,
        "prerelease": False,
        "author": {"id": 1, "login": "releaser", "type": "User"},
        "target_commitish": "main",
    }


def _make_raw_deployment(*, id: int = 1, sha: str = "abc123", created_at: datetime = NOW):
    return {
        "id": id,
        "sha": sha,
        "ref": "main",
        "environment": "production",
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": created_at.isoformat().replace("+00:00", "Z"),
        "creator": {"id": 2, "login": "deployer", "type": "User"},
        "description": "Deploy to prod",
    }


def _make_raw_ci_run(*, id: int = 1, sha: str = "def456", created_at: datetime = NOW, pr_number: int | None = None):
    run = {
        "id": id,
        "name": "CI",
        "head_sha": sha,
        "event": "push",
        "status": "completed",
        "conclusion": "success",
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": (created_at + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "run_started_at": created_at.isoformat().replace("+00:00", "Z"),
        "pull_requests": [{"number": pr_number}] if pr_number else [],
    }
    return run


# ── FileSystemDumpWriter tests ───────────────────────────────────────

class TestDumpWriter:
    def test_write_repo_data_creates_releases_jsonl(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        records = [_make_raw_release(id=1), _make_raw_release(id=2, tag="v2.0.0")]
        writer.write_repo_data("releases", records, "org/repo")

        path = tmp_path / "canonical" / "releases.jsonl"
        assert path.exists()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        parsed = json.loads(lines[0])
        assert parsed["_repository"] == "org/repo"
        assert parsed["tag_name"] == "v1.0.0"

    def test_write_repo_data_creates_deployments_jsonl(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        records = [_make_raw_deployment()]
        writer.write_repo_data("deployments", records, "org/repo")

        path = tmp_path / "canonical" / "deployments.jsonl"
        assert path.exists()
        parsed = json.loads(path.read_text().strip())
        assert parsed["environment"] == "production"
        assert parsed["_repository"] == "org/repo"

    def test_write_repo_data_creates_ci_runs_jsonl(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        records = [_make_raw_ci_run(pr_number=42)]
        writer.write_repo_data("ci_runs", records, "org/repo")

        path = tmp_path / "canonical" / "ci_runs.jsonl"
        assert path.exists()
        parsed = json.loads(path.read_text().strip())
        assert parsed["head_sha"] == "def456"

    def test_write_repo_data_skips_unknown_kind(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        writer.write_repo_data("unknown", [{"id": 1}], "org/repo")
        # No file created
        assert not (tmp_path / "canonical" / "unknown.jsonl").exists()

    def test_write_repo_data_skips_empty_records(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        writer.write_repo_data("releases", [], "org/repo")
        assert not (tmp_path / "canonical" / "releases.jsonl").exists()

    def test_stale_dora_files_cleaned_on_init(self, tmp_path):
        """New writer init should remove stale releases/deployments/ci_runs files."""
        canonical = tmp_path / "canonical"
        canonical.mkdir(parents=True)
        for fname in ("releases.jsonl", "deployments.jsonl", "ci_runs.jsonl"):
            (canonical / fname).write_text("stale\n")

        FileSystemDumpWriter(tmp_path)

        for fname in ("releases.jsonl", "deployments.jsonl", "ci_runs.jsonl"):
            assert not (canonical / fname).exists()


# ── Adapter parsing tests ────────────────────────────────────────────

class TestAdapterParsing:
    def _setup_dump(self, tmp_path, *, releases=None, deployments=None, ci_runs=None):
        """Write manifest + JSONL files for adapter parsing."""
        _write_manifest(tmp_path)
        canonical = tmp_path / "canonical"
        canonical.mkdir(parents=True, exist_ok=True)

        if releases:
            with (canonical / "releases.jsonl").open("w") as f:
                for r in releases:
                    f.write(json.dumps(r) + "\n")
        if deployments:
            with (canonical / "deployments.jsonl").open("w") as f:
                for d in deployments:
                    f.write(json.dumps(d) + "\n")
        if ci_runs:
            with (canonical / "ci_runs.jsonl").open("w") as f:
                for c in ci_runs:
                    f.write(json.dumps(c) + "\n")

    def test_parse_releases(self, tmp_path):
        self._setup_dump(tmp_path, releases=[
            _make_raw_release(id=1, tag="v1.0.0"),
            _make_raw_release(id=2, tag="v2.0.0"),
        ])
        bundle = GitHubAdapter().parse_dump(str(tmp_path))
        assert len(bundle.releases) == 2
        assert bundle.releases[0].tag_name == "v1.0.0"
        assert isinstance(bundle.releases[0], ReleaseRecord)
        assert bundle.releases[0].author is not None
        assert bundle.releases[0].author.login == "releaser"

    def test_parse_deployments(self, tmp_path):
        self._setup_dump(tmp_path, deployments=[
            _make_raw_deployment(id=1, sha="aaa"),
            _make_raw_deployment(id=2, sha="bbb"),
        ])
        bundle = GitHubAdapter().parse_dump(str(tmp_path))
        assert len(bundle.deployments) == 2
        assert bundle.deployments[0].environment == "production"
        assert isinstance(bundle.deployments[0], DeploymentRecord)

    def test_parse_ci_runs(self, tmp_path):
        self._setup_dump(tmp_path, ci_runs=[
            _make_raw_ci_run(id=1, pr_number=10),
        ])
        bundle = GitHubAdapter().parse_dump(str(tmp_path))
        assert len(bundle.ci_runs) == 1
        run = bundle.ci_runs[0]
        assert isinstance(run, CIRunRecord)
        assert run.pull_request_number == 10
        assert run.conclusion == "success"
        # Duration computed from run_started_at to updated_at (5 min)
        assert run.duration_seconds == 300

    def test_parse_filters_by_date_window(self, tmp_path):
        """Records outside the manifest date window should be excluded."""
        before_window = datetime(2026, 2, 1, tzinfo=UTC)
        after_window = datetime(2026, 4, 15, tzinfo=UTC)
        in_window = NOW

        self._setup_dump(tmp_path, releases=[
            _make_raw_release(id=1, created_at=before_window),
            _make_raw_release(id=2, created_at=in_window),
            _make_raw_release(id=3, created_at=after_window),
        ])
        bundle = GitHubAdapter().parse_dump(str(tmp_path))
        assert len(bundle.releases) == 1
        assert bundle.releases[0].id == 2

    def test_empty_dump_has_empty_lists(self, tmp_path):
        """Adapter gracefully handles missing JSONL files."""
        _write_manifest(tmp_path)
        (tmp_path / "canonical").mkdir(parents=True, exist_ok=True)
        bundle = GitHubAdapter().parse_dump(str(tmp_path))
        assert bundle.releases == []
        assert bundle.deployments == []
        assert bundle.ci_runs == []


# ── Ledger indexing tests ────────────────────────────────────────────

class TestLedgerDORA:
    def test_releases_indexed_and_sorted(self):
        r1 = ReleaseRecord(id=1, tag_name="v1", created_at=NOW + timedelta(days=1))
        r2 = ReleaseRecord(id=2, tag_name="v2", created_at=NOW)
        bundle = make_bundle(releases=[r1, r2])
        ledger = Ledger(bundle)

        assert len(ledger.releases) == 2
        assert ledger.releases[0].tag_name == "v2"  # earlier first
        assert ledger.releases[1].tag_name == "v1"

    def test_deployments_indexed_and_sorted(self):
        d1 = DeploymentRecord(id=1, sha="a", ref="main", environment="production", created_at=NOW + timedelta(hours=1))
        d2 = DeploymentRecord(id=2, sha="b", ref="main", environment="staging", created_at=NOW)
        bundle = make_bundle(deployments=[d1, d2])
        ledger = Ledger(bundle)

        assert len(ledger.deployments) == 2
        assert ledger.deployments[0].environment == "staging"

    def test_ci_runs_indexed_by_pr(self):
        ci1 = CIRunRecord(id=1, head_sha="a", created_at=NOW, pull_request_number=10)
        ci2 = CIRunRecord(id=2, head_sha="b", created_at=NOW, pull_request_number=10)
        ci3 = CIRunRecord(id=3, head_sha="c", created_at=NOW, pull_request_number=20)
        bundle = make_bundle(ci_runs=[ci1, ci2, ci3])
        ledger = Ledger(bundle)

        assert len(ledger.get_ci_runs_for_pr(10)) == 2
        assert len(ledger.get_ci_runs_for_pr(20)) == 1
        assert ledger.get_ci_runs_for_pr(99) == []

    def test_get_releases_date_filter(self):
        r1 = ReleaseRecord(id=1, tag_name="v1", created_at=START + timedelta(days=1))
        r2 = ReleaseRecord(id=2, tag_name="v2", created_at=START + timedelta(days=15))
        bundle = make_bundle(releases=[r1, r2])
        ledger = Ledger(bundle)

        mid = START + timedelta(days=10)
        filtered = ledger.get_releases(start_date=mid)
        assert len(filtered) == 1
        assert filtered[0].tag_name == "v2"

    def test_get_deployments_environment_filter(self):
        d1 = DeploymentRecord(id=1, sha="a", ref="main", environment="production", created_at=NOW)
        d2 = DeploymentRecord(id=2, sha="b", ref="main", environment="staging", created_at=NOW)
        bundle = make_bundle(deployments=[d1, d2])
        ledger = Ledger(bundle)

        prod = ledger.get_deployments(environment="production")
        assert len(prod) == 1
        assert prod[0].environment == "production"

    def test_empty_bundle_has_empty_dora_indexes(self):
        bundle = make_bundle()
        ledger = Ledger(bundle)
        assert ledger.releases == []
        assert ledger.deployments == []
        assert ledger.ci_runs == []
        assert ledger.get_ci_runs_for_pr(1) == []
