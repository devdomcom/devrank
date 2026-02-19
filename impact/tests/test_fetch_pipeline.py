"""Tests for fetch pipeline fixes: reviewed-by search, date filtering, dedup, JSONL cleanup."""
import json
import textwrap
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from impact.adapters.github import GitHubAdapter
from impact.persistence.filesystem import FileSystemDumpWriter
from impact.providers.github.client import GitHubClient


# ---------------------------------------------------------------------------
# Helper: build a minimal PR dict for the adapter
# ---------------------------------------------------------------------------
def _pr_dict(
    number: int,
    login: str,
    created_at: str,
    closed_at: str | None = None,
    merged_at: str | None = None,
    updated_at: str | None = None,
) -> dict:
    """Minimal PR dict matching the shape the adapter expects."""
    return {
        "number": number,
        "id": number * 100,
        "title": f"PR #{number}",
        "body": "",
        "state": "closed" if closed_at else "open",
        "draft": False,
        "merged": merged_at is not None,
        "merge_commit_sha": None,
        "user": {"id": 1, "login": login, "type": "User"},
        "created_at": created_at,
        "updated_at": updated_at or closed_at or created_at,
        "closed_at": closed_at,
        "merged_at": merged_at,
        "merged_by": None,
        "commits": 1,
        "additions": 10,
        "deletions": 2,
        "changed_files": 1,
        "comments": 0,
        "review_comments": 0,
        "base": {
            "label": "main",
            "ref": "main",
            "sha": "aaa",
            "user": {"id": 1, "login": login, "type": "User"},
            "repo": {
                "id": 999,
                "name": "superset",
                "full_name": "apache/superset",
                "owner": {"id": 10, "login": "apache", "type": "Organization"},
            },
        },
        "head": {
            "label": "feature",
            "ref": "feature",
            "sha": "bbb",
            "user": {"id": 1, "login": login, "type": "User"},
            "repo": {
                "id": 999,
                "name": "superset",
                "full_name": "apache/superset",
                "owner": {"id": 10, "login": "apache", "type": "Organization"},
            },
        },
        "requested_reviewers": [],
        "assignees": [],
    }


def _write_manifest(dump_dir: Path, user: str, from_dt: str, to_dt: str):
    dump_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "provider": "github",
        "api_version": "2022-11-28",
        "user": user,
        "from": from_dt,
        "to": to_dt,
        "repositories": ["apache/superset"],
    }
    (dump_dir / "dump_manifest.json").write_text(json.dumps(manifest))


def _write_jsonl(path: Path, records: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


# =========================================================================
# 1. GitHubClient.search_issues
# =========================================================================
class TestClientSearchIssues:
    def test_search_issues_single_page(self):
        """search_issues unwraps {items: [...]} envelope."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "total_count": 2,
            "items": [{"number": 100}, {"number": 200}],
        }
        mock_resp.headers = {}

        client = GitHubClient.__new__(GitHubClient)
        client.base_url = "https://api.github.com"
        client.get = MagicMock(return_value=mock_resp)

        results = client.search_issues("is:pr reviewed-by:alice repo:org/repo")
        assert len(results) == 2
        assert results[0]["number"] == 100
        assert results[1]["number"] == 200

    def test_search_issues_multi_page(self):
        """search_issues follows Link pagination."""
        page1_resp = MagicMock()
        page1_resp.json.return_value = {"total_count": 3, "items": [{"number": 1}]}
        page1_resp.headers = {
            "Link": '<https://api.github.com/search/issues?q=x&page=2>; rel="next"'
        }

        page2_resp = MagicMock()
        page2_resp.json.return_value = {"total_count": 3, "items": [{"number": 2}, {"number": 3}]}
        page2_resp.headers = {}

        client = GitHubClient.__new__(GitHubClient)
        client.base_url = "https://api.github.com"
        client.get = MagicMock(side_effect=[page1_resp, page2_resp])

        results = client.search_issues("is:pr reviewed-by:bob")
        assert len(results) == 3
        assert [r["number"] for r in results] == [1, 2, 3]

    def test_search_issues_empty(self):
        """search_issues returns [] when no results."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"total_count": 0, "items": []}
        mock_resp.headers = {}

        client = GitHubClient.__new__(GitHubClient)
        client.base_url = "https://api.github.com"
        client.get = MagicMock(return_value=mock_resp)

        results = client.search_issues("is:pr reviewed-by:nobody")
        assert results == []


# =========================================================================
# 2. GitHubLiveFetcher reviewed-by search + dedup
# =========================================================================
class TestLiveFetcherReviewedBy:
    def test_reviewed_prs_added_to_queue(self):
        """PRs found via reviewed-by search are added to the fetch queue."""
        from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig

        cfg = LiveFetchConfig(
            user_login="alice",
            repos=["org/repo"],
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 16, tzinfo=UTC),
            token="fake",
            out_dir=Path("/tmp/test_fetch_reviewed"),
        )
        fetcher_obj = GitHubLiveFetcher(cfg)

        # Mock the client and fetcher
        mock_client = MagicMock()
        mock_gh_fetcher = MagicMock()

        # list_prs returns 1 authored PR
        mock_gh_fetcher.list_prs.return_value = [
            {"number": 100, "user": {"login": "alice"}, "requested_reviewers": [], "assignees": []},
        ]

        # search_issues returns 2 PRs: one already known (100), one new (200)
        mock_client.search_issues.return_value = [
            {"number": 100},
            {"number": 200},
        ]
        mock_client.get.return_value = MagicMock(
            json=MagicMock(return_value={"resources": {"core": {"remaining": 5000, "reset": 0}}})
        )

        # Mock fetch_pr_bundle to track what gets fetched
        mock_gh_fetcher.fetch_pr_bundle.return_value = {
            "pull_request": {"number": 0},
        }

        with patch("impact.providers.github_live.GitHubClient", return_value=mock_client), \
             patch("impact.providers.github_live.GitHubFetcher", return_value=mock_gh_fetcher), \
             patch("impact.providers.github_live.FileSystemDumpWriter") as mock_writer_cls, \
             patch("impact.providers.github_live.GitHubAdapter") as mock_adapter_cls:

            mock_writer = MagicMock()
            mock_writer_cls.return_value = mock_writer
            mock_adapter_cls.return_value.parse_dump.return_value = MagicMock()

            fetcher_obj.run()

            # Should fetch 2 unique PRs: 100 (from list_prs) and 200 (from search)
            assert mock_gh_fetcher.fetch_pr_bundle.call_count == 2
            fetched_numbers = {
                call.args[1] for call in mock_gh_fetcher.fetch_pr_bundle.call_args_list
            }
            assert fetched_numbers == {100, 200}

    def test_search_failure_is_non_fatal(self):
        """If search_issues fails, fetch continues with authored PRs only."""
        from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig

        cfg = LiveFetchConfig(
            user_login="alice",
            repos=["org/repo"],
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 16, tzinfo=UTC),
            token="fake",
            out_dir=Path("/tmp/test_fetch_fallback"),
        )
        fetcher_obj = GitHubLiveFetcher(cfg)

        mock_client = MagicMock()
        mock_gh_fetcher = MagicMock()

        mock_gh_fetcher.list_prs.return_value = [
            {"number": 100, "user": {"login": "alice"}, "requested_reviewers": [], "assignees": []},
        ]
        mock_client.search_issues.side_effect = Exception("API down")
        mock_client.get.return_value = MagicMock(
            json=MagicMock(return_value={"resources": {"core": {"remaining": 5000, "reset": 0}}})
        )
        mock_gh_fetcher.fetch_pr_bundle.return_value = {"pull_request": {"number": 100}}

        with patch("impact.providers.github_live.GitHubClient", return_value=mock_client), \
             patch("impact.providers.github_live.GitHubFetcher", return_value=mock_gh_fetcher), \
             patch("impact.providers.github_live.FileSystemDumpWriter") as mock_writer_cls, \
             patch("impact.providers.github_live.GitHubAdapter") as mock_adapter_cls:

            mock_writer_cls.return_value = MagicMock()
            mock_adapter_cls.return_value.parse_dump.return_value = MagicMock()

            # Should NOT raise — search failure is non-fatal
            fetcher_obj.run()
            assert mock_gh_fetcher.fetch_pr_bundle.call_count == 1


# =========================================================================
# 3. Adapter date-range filter: active-during-window logic
# =========================================================================
class TestAdapterDateFilter:
    def test_pr_created_and_merged_before_window_excluded(self, tmp_path):
        """PR fully resolved before window start is excluded."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                100, "alice",
                created_at="2025-11-25T00:00:00Z",
                closed_at="2025-12-08T00:00:00Z",
                merged_at="2025-12-08T00:00:00Z",
                updated_at="2025-12-08T00:00:00Z",
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 0

    def test_pr_created_before_window_merged_during_window_included(self, tmp_path):
        """PR created pre-window but merged in-window is included."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                200, "alice",
                created_at="2025-12-10T00:00:00Z",
                closed_at="2026-01-20T00:00:00Z",
                merged_at="2026-01-20T00:00:00Z",
                updated_at="2026-01-20T00:00:00Z",
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 1
        assert bundle.pull_requests[0].number == 200

    def test_pr_created_before_window_still_open_included(self, tmp_path):
        """PR created pre-window but still open is included."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                300, "alice",
                created_at="2025-12-17T00:00:00Z",
                updated_at="2026-02-11T00:00:00Z",
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 1
        assert bundle.pull_requests[0].number == 300

    def test_pr_created_after_window_excluded(self, tmp_path):
        """PR created after window end is excluded."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                400, "alice",
                created_at="2026-03-01T00:00:00Z",
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 0

    def test_pr_created_in_window_included(self, tmp_path):
        """Standard case: PR created within window is included."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                500, "alice",
                created_at="2026-01-15T00:00:00Z",
                closed_at="2026-02-01T00:00:00Z",
                merged_at="2026-02-01T00:00:00Z",
                updated_at="2026-02-01T00:00:00Z",
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 1
        assert bundle.pull_requests[0].number == 500

    def test_pr_closed_before_window_but_updated_during_included(self, tmp_path):
        """PR closed before window but with activity during window (comment/bot update)."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(
                600, "alice",
                created_at="2025-12-24T00:00:00Z",
                closed_at="2025-12-31T00:00:00Z",
                merged_at="2025-12-31T00:00:00Z",
                updated_at="2026-01-29T00:00:00Z",  # bot updated it in-window
            ),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 1
        assert bundle.pull_requests[0].number == 600

    def test_mixed_prs_correct_filtering(self, tmp_path):
        """Mix of in-window, straddling, and fully-out-of-window PRs."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"
        _write_jsonl(canonical / "pull_requests.jsonl", [
            # Fully pre-window (closed+updated before start) → excluded
            _pr_dict(1, "alice", "2025-11-01T00:00:00Z", "2025-11-15T00:00:00Z", "2025-11-15T00:00:00Z", "2025-11-15T00:00:00Z"),
            # Straddled: created before, merged during → included
            _pr_dict(2, "alice", "2025-12-10T00:00:00Z", "2026-01-20T00:00:00Z", "2026-01-20T00:00:00Z", "2026-01-20T00:00:00Z"),
            # In-window → included
            _pr_dict(3, "alice", "2026-01-08T00:00:00Z", "2026-01-30T00:00:00Z", "2026-01-30T00:00:00Z", "2026-01-30T00:00:00Z"),
            # Created after window → excluded
            _pr_dict(4, "alice", "2026-03-01T00:00:00Z"),
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        numbers = {pr.number for pr in bundle.pull_requests}
        assert numbers == {2, 3}


# =========================================================================
# 4. FileSystemDumpWriter clears stale JSONL on init
# =========================================================================
class TestDumpWriterCleanup:
    def test_init_clears_existing_jsonl(self, tmp_path):
        """Re-creating a writer for the same dir removes old JSONL files."""
        base = tmp_path / "dump"
        canonical = base / "canonical"
        canonical.mkdir(parents=True)

        # Simulate a prior fetch
        (canonical / "pull_requests.jsonl").write_text('{"old": true}\n')
        (canonical / "reviews.jsonl").write_text('{"old": true}\n')
        assert (canonical / "pull_requests.jsonl").exists()

        # Creating a new writer should clear the old files
        FileSystemDumpWriter(base)
        assert not (canonical / "pull_requests.jsonl").exists()
        assert not (canonical / "reviews.jsonl").exists()

    def test_init_works_on_fresh_dir(self, tmp_path):
        """Writer works fine when no prior data exists."""
        base = tmp_path / "fresh_dump"
        writer = FileSystemDumpWriter(base)
        assert writer.canonical_dir.exists()
        # No files to clean — should not raise
        assert list(writer.canonical_dir.iterdir()) == []

    def test_write_after_cleanup_only_has_new_data(self, tmp_path):
        """After cleanup, writing new data doesn't include old records."""
        base = tmp_path / "dump"
        canonical = base / "canonical"
        canonical.mkdir(parents=True)

        # Simulate old data
        (canonical / "pull_requests.jsonl").write_text('{"number": 1, "old": true}\n')

        # New writer clears old, then writes new
        writer = FileSystemDumpWriter(base)
        writer.write_pr_bundle({
            "pull_request": {"number": 99, "new": True},
            "reviews": [{"id": 1}],
        })

        lines = (canonical / "pull_requests.jsonl").read_text().strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["number"] == 99
        # No "old" record
        assert "old" not in lines[0]


# =========================================================================
# 5. Adapter: reviewed PRs (non-authored) kept via acted_pr_numbers
# =========================================================================
class TestAdapterReviewedPRs:
    def test_non_authored_pr_with_user_review_included(self, tmp_path):
        """PR authored by someone else is kept if the assessed user submitted a review."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"

        # PR authored by bob
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(100, "bob", "2026-01-10T00:00:00Z", "2026-01-20T00:00:00Z", "2026-01-20T00:00:00Z"),
        ])
        # Alice submitted a review on bob's PR
        _write_jsonl(canonical / "reviews.jsonl", [
            {
                "id": 1,
                "user": {"id": 2, "login": "alice", "type": "User"},
                "body": "LGTM",
                "state": "APPROVED",
                "submitted_at": "2026-01-12T00:00:00Z",
                "pull_request_url": "https://api.github.com/repos/apache/superset/pulls/100",
            },
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))

        # PR should be included because alice acted on it
        assert len(bundle.pull_requests) == 1
        assert bundle.pull_requests[0].number == 100
        # And the review should be present
        assert len(bundle.reviews) == 1
        assert bundle.reviews[0].user.login == "alice"

    def test_non_authored_pr_without_user_action_excluded(self, tmp_path):
        """PR authored by someone else is excluded if the assessed user never acted."""
        dump_dir = tmp_path / "dump"
        _write_manifest(dump_dir, "alice", "2026-01-01T00:00:00Z", "2026-02-16T23:59:59Z")
        canonical = dump_dir / "canonical"

        # PR authored by bob, no action by alice
        _write_jsonl(canonical / "pull_requests.jsonl", [
            _pr_dict(100, "bob", "2026-01-10T00:00:00Z", "2026-01-20T00:00:00Z", "2026-01-20T00:00:00Z"),
        ])
        # Review by charlie (not alice)
        _write_jsonl(canonical / "reviews.jsonl", [
            {
                "id": 1,
                "user": {"id": 3, "login": "charlie", "type": "User"},
                "body": "Needs changes",
                "state": "CHANGES_REQUESTED",
                "submitted_at": "2026-01-12T00:00:00Z",
                "pull_request_url": "https://api.github.com/repos/apache/superset/pulls/100",
            },
        ])

        adapter = GitHubAdapter()
        bundle = adapter.parse_dump(str(dump_dir))
        assert len(bundle.pull_requests) == 0
