"""Regression tests: timeline event enrichment and ingestion.

Verifies that:
1. The persistence layer enriches timeline events with pull_request_number.
2. The adapter correctly ingests enriched timeline events.
3. Timeline-dependent metrics (review_demand, review_turnaround_time) produce
   real data when timeline events are present.
"""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from impact.adapters.github import GitHubAdapter
from impact.persistence.filesystem import FileSystemDumpWriter


# ── Persistence: PR number enrichment ────────────────────────────────────

class TestTimelineEnrichment:
    """FileSystemDumpWriter must enrich timeline events with pull_request_number."""

    def test_timeline_events_get_pr_number(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        pr_number = 42

        bundle = {
            "pull_request": {"number": pr_number, "title": "test"},
            "timeline": [
                {"event": "labeled", "id": 1, "created_at": "2026-01-15T00:00:00Z",
                 "actor": {"id": 1, "login": "alice", "type": "User"},
                 "label": {"name": "bug"}},
                {"event": "review_requested", "id": 2, "created_at": "2026-01-15T01:00:00Z",
                 "actor": {"id": 1, "login": "alice", "type": "User"},
                 "requested_reviewer": {"id": 2, "login": "bob", "type": "User"}},
            ],
        }
        writer.write_pr_bundle(bundle)

        tl_file = tmp_path / "canonical" / "timeline.jsonl"
        assert tl_file.exists()
        lines = tl_file.read_text().strip().split("\n")
        assert len(lines) == 2

        for line in lines:
            event = json.loads(line)
            assert event["pull_request_number"] == pr_number, \
                f"Timeline event missing pull_request_number enrichment: {event}"

    def test_commits_get_pr_number(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {"number": 99, "title": "test"},
            "commits": [
                {"sha": "abc123", "commit": {"author": {"date": "2026-01-15T00:00:00Z"},
                 "message": "fix"}, "author": {"id": 1, "login": "alice"}},
            ],
        }
        writer.write_pr_bundle(bundle)

        commit_file = tmp_path / "canonical" / "commits.jsonl"
        commit = json.loads(commit_file.read_text().strip())
        assert commit["pull_request_number"] == 99

    def test_files_get_pr_number(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {"number": 55, "title": "test"},
            "files": [
                {"sha": "def456", "filename": "src/app.py", "additions": 10,
                 "deletions": 2, "changes": 12, "status": "modified"},
            ],
        }
        writer.write_pr_bundle(bundle)

        files_file = tmp_path / "canonical" / "files.jsonl"
        file_rec = json.loads(files_file.read_text().strip())
        assert file_rec["pull_request_number"] == 55


# ── Adapter: timeline ingestion ──────────────────────────────────────────

def _make_minimal_dump(tmp_path: Path, pr_number: int = 100,
                       user_login: str = "alice",
                       timeline_events: list[dict] | None = None) -> Path:
    """Create a minimal valid dump directory for adapter testing."""
    canonical = tmp_path / "canonical"
    canonical.mkdir(parents=True, exist_ok=True)

    manifest = {
        "provider": "github",
        "api_version": "2022-11-28",
        "user": user_login,
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-01-31T23:59:59Z",
        "repositories": ["test/repo"],
        "generated_at": "2026-01-31T00:00:00Z",
    }
    (tmp_path / "dump_manifest.json").write_text(json.dumps(manifest))

    # Minimal PR
    pr = {
        "id": 1, "number": pr_number, "title": "Test PR", "state": "closed",
        "user": {"id": 10, "login": user_login, "type": "User"},
        "created_at": "2026-01-15T10:00:00Z",
        "updated_at": "2026-01-15T12:00:00Z",
        "closed_at": "2026-01-15T14:00:00Z",
        "merged_at": "2026-01-15T14:00:00Z",
        "draft": False, "merged": True, "merge_commit_sha": "abc",
        "commits": 1, "additions": 10, "deletions": 2, "changed_files": 1,
        "comments": 0, "review_comments": 0,
        "base": {
            "label": "main", "ref": "main", "sha": "base123",
            "user": {"id": 10, "login": user_login, "type": "User"},
            "repo": {"id": 1, "name": "repo", "full_name": "test/repo",
                     "owner": {"id": 20, "login": "test", "type": "Organization"}},
        },
        "head": {
            "label": "feature", "ref": "feature", "sha": "head456",
            "user": {"id": 10, "login": user_login, "type": "User"},
            "repo": {"id": 1, "name": "repo", "full_name": "test/repo",
                     "owner": {"id": 20, "login": "test", "type": "Organization"}},
        },
    }
    with (canonical / "pull_requests.jsonl").open("w") as f:
        f.write(json.dumps(pr) + "\n")

    # Timeline events
    if timeline_events:
        with (canonical / "timeline.jsonl").open("w") as f:
            for evt in timeline_events:
                f.write(json.dumps(evt) + "\n")

    return tmp_path


class TestTimelineIngestion:
    """GitHubAdapter must parse enriched timeline events into the bundle."""

    def test_enriched_events_are_ingested(self, tmp_path):
        events = [
            {"event": "labeled", "id": 1001, "created_at": "2026-01-15T10:05:00Z",
             "actor": {"id": 10, "login": "alice", "type": "User"},
             "pull_request_number": 100, "node_id": "n1"},
            {"event": "review_requested", "id": 1002, "created_at": "2026-01-15T10:10:00Z",
             "actor": {"id": 10, "login": "alice", "type": "User"},
             "requested_reviewer": {"id": 30, "login": "bob", "type": "User"},
             "pull_request_number": 100, "node_id": "n2"},
        ]
        dump_path = _make_minimal_dump(tmp_path, timeline_events=events)
        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_path))

        assert len(bundle.timeline) == 2
        assert bundle.timeline[0].event == "labeled"
        assert bundle.timeline[1].event == "review_requested"
        assert bundle.timeline[1].requested_reviewer.login == "bob"

    def test_events_without_pr_number_are_skipped(self, tmp_path):
        """Events without pull_request_number AND unparseable URL are dropped."""
        events = [
            {"event": "labeled", "id": 2001, "created_at": "2026-01-15T10:05:00Z",
             "actor": {"id": 10, "login": "alice", "type": "User"},
             # No pull_request_number, URL doesn't contain PR number
             "url": "https://api.github.com/repos/test/repo/issues/events/2001"},
        ]
        dump_path = _make_minimal_dump(tmp_path, timeline_events=events)
        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_path))

        # Event should be dropped (no PR number, URL fallback yields "events" not a number)
        assert len(bundle.timeline) == 0

    def test_events_outside_date_range_are_filtered(self, tmp_path):
        events = [
            {"event": "labeled", "id": 3001, "created_at": "2025-06-01T10:00:00Z",
             "actor": {"id": 10, "login": "alice", "type": "User"},
             "pull_request_number": 100},
        ]
        dump_path = _make_minimal_dump(tmp_path, timeline_events=events)
        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_path))

        assert len(bundle.timeline) == 0

    def test_committed_entries_are_skipped(self, tmp_path):
        """Entries with event='committed' but no 'event' key structure are skipped."""
        events = [
            # This is a raw committed entry (no 'event' key in the way _normalize expects)
            {"sha": "abc", "author": {"name": "Alice"}, "message": "fix",
             "pull_request_number": 100},
        ]
        dump_path = _make_minimal_dump(tmp_path, timeline_events=events)
        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_path))

        assert len(bundle.timeline) == 0


# ── Sample dump sanity ───────────────────────────────────────────────────

SAMPLE_DUMP = "impact/samples/github_live_dump"


class TestSampleDumpTimeline:
    """The sample dump must have usable timeline data after re-fetch."""

    @pytest.fixture
    def bundle(self):
        from impact.ingestion.dump import DumpIngestion
        return DumpIngestion(SAMPLE_DUMP).ingest()

    def test_timeline_events_present(self, bundle):
        assert len(bundle.timeline) > 0, \
            "Sample dump has 0 timeline events — was it re-fetched with PR number enrichment?"

    def test_timeline_events_have_pr_numbers(self, bundle):
        for evt in bundle.timeline:
            assert evt.pull_request_number is not None, \
                f"Timeline event {evt.id} ({evt.event}) has no pull_request_number"

    def test_review_requested_events_have_reviewer(self, bundle):
        rr_events = [e for e in bundle.timeline if e.event == "review_requested"]
        # Not all dumps will have review_requested events, but if they do, they
        # should have a requested_reviewer
        for evt in rr_events:
            assert evt.requested_reviewer is not None, \
                f"review_requested event {evt.id} missing requested_reviewer"

    def test_review_demand_metric_has_timeline_data(self, bundle):
        """review_demand should see timeline events (not report 'no timeline data')."""
        import json
        from impact.domain.models import MetricContext
        from impact.ledger.ledger import Ledger
        from impact.metrics import get_metrics

        manifest = json.loads(
            Path(SAMPLE_DUMP, "dump_manifest.json").read_text()
        )
        ledger = Ledger(bundle)
        ctx = MetricContext(
            ledger=ledger,
            user_login=manifest["user"],
            start_date=datetime.fromisoformat(manifest["from"].replace("Z", "+00:00")),
            end_date=datetime.fromisoformat(manifest["to"].replace("Z", "+00:00")),
        )
        metric_cls = get_metrics()["review_demand"]
        result = metric_cls().run(ctx)
        assert not result.details.get("no_data", False), \
            f"review_demand flagged no_data — timeline enrichment broken? details={result.details}"
        assert result.details.get("demand_count", 0) > 0, \
            f"review_demand found 0 requests — expected >0 with timeline data"
