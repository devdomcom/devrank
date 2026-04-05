"""Tests for secret redaction in the persistence layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from impact.persistence.redact import redact_secrets, scrub_record, _PLACEHOLDER
from impact.persistence.filesystem import FileSystemDumpWriter


class TestRedactSecrets:
    """Unit tests for individual secret patterns."""

    def test_mapbox_public_key(self):
        text = "key: 'pk.eyJ1Ijoia3Jpc3R3IiwiYSI6ImNqbGg1bjI2NTFlczczdnBhazViMjgzZ2sifQ.lUneM-o3NucXN1cG';"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "pk.eyJ" not in redact_secrets(text)

    def test_mapbox_secret_key(self):
        text = "token = 'sk.eyJ1IjoiZm9vIiwiYSI6ImFiYyJ9.abcDEFghiJKL-123_456'"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "sk.eyJ" not in redact_secrets(text)

    def test_aws_access_key(self):
        text = "aws_key = AKIAIOSFODNN7EXAMPLE"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "AKIA" not in redact_secrets(text)

    def test_github_token(self):
        text = "token: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "ghp_" not in redact_secrets(text)

    def test_gitlab_token(self):
        text = "GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "glpat-" not in redact_secrets(text)

    def test_slack_token(self):
        text = "SLACK_TOKEN=xoxb-1234-5678-abcdefghijklmnop"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "xoxb-" not in redact_secrets(text)

    def test_stripe_secret_key(self):
        # Construct token dynamically to avoid GitHub push protection
        token = "sk" + "_live_" + "1234567890abcdefghijklmn"
        text = f"key={token}"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "sk_live_" not in redact_secrets(text)

    def test_stripe_test_key(self):
        token = "sk" + "_test_" + "1234567890abcdefghijklmn"
        assert _PLACEHOLDER in redact_secrets(token)

    def test_google_api_key(self):
        text = "key=AIzaSyA1234567890abcdefghijklmnopqrstuv"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "AIza" not in redact_secrets(text)

    def test_sendgrid_key(self):
        # Construct dynamically to avoid GitHub push protection
        token = "SG." + "abcdefghijklmnopqrstuv" + "." + "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
        assert _PLACEHOLDER in redact_secrets(token)
        assert "SG." not in redact_secrets(token)

    def test_private_key_header(self):
        text = "-----BEGIN RSA PRIVATE KEY-----\nMIIEow..."
        assert _PLACEHOLDER in redact_secrets(text)
        assert "PRIVATE KEY" not in redact_secrets(text)

    def test_openssh_private_key(self):
        text = "-----BEGIN OPENSSH PRIVATE KEY-----"
        assert _PLACEHOLDER in redact_secrets(text)

    def test_connection_string(self):
        text = "postgres://admin:s3cret@db.host.com/mydb"
        result = redact_secrets(text)
        assert "s3cret" not in result
        assert "admin" not in result

    def test_inline_password(self):
        text = "password = 'SuperSecretPassword123'"
        assert _PLACEHOLDER in redact_secrets(text)
        assert "SuperSecret" not in redact_secrets(text)

    def test_inline_api_key(self):
        text = 'api_key: "abcdefghijklmnopqrstuvwxyz"'
        assert _PLACEHOLDER in redact_secrets(text)

    def test_no_false_positive_on_normal_code(self):
        text = "const x = 42; function foo() { return bar; }"
        assert redact_secrets(text) == text

    def test_no_false_positive_on_short_values(self):
        text = "password = 'short'"
        assert redact_secrets(text) == text  # too short to match

    def test_multiple_secrets_in_one_string(self):
        text = "key1=ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij and key2=AKIAIOSFODNN7EXAMPLE"
        result = redact_secrets(text)
        assert "ghp_" not in result
        assert "AKIA" not in result
        assert result.count(_PLACEHOLDER) == 2

    def test_preserves_surrounding_text(self):
        text = "before ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij after"
        result = redact_secrets(text)
        assert result.startswith("before ")
        assert result.endswith(" after")


class TestScrubRecord:
    """Tests for scrub_record on raw API dicts."""

    def test_scrubs_patch_field(self):
        record = {"sha": "abc", "patch": "+  token = 'pk.eyJ1IjoiZm9vIiwiYSI6ImJhciJ9.xyz123'"}
        result = scrub_record(record)
        assert "pk.eyJ" not in result["patch"]
        assert result["sha"] == "abc"  # non-text field untouched

    def test_scrubs_body_field(self):
        record = {"id": 1, "body": "Set AKIAIOSFODNN7EXAMPLE in env"}
        result = scrub_record(record)
        assert "AKIA" not in result["body"]

    def test_scrubs_content_field(self):
        record = {"filename": "config.py", "content": "SECRET = 'ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij'"}
        result = scrub_record(record)
        assert "ghp_" not in result["content"]

    def test_scrubs_message_field(self):
        token = "sk" + "_live_" + "1234567890abcdefghijklmn"
        record = {"sha": "x", "message": f"fix: remove {token} from config"}
        result = scrub_record(record)
        assert "sk_live_" not in result["message"]

    def test_scrubs_description_field(self):
        record = {"id": 1, "description": "Deploy with token glpat-xxxxxxxxxxxxxxxxxxxx"}
        result = scrub_record(record)
        assert "glpat-" not in result["description"]

    def test_ignores_non_text_fields(self):
        record = {"sha": "AKIAIOSFODNN7EXAMPLE", "number": 42}
        result = scrub_record(record)
        assert result["sha"] == "AKIAIOSFODNN7EXAMPLE"  # sha is not a _TEXT_FIELD

    def test_no_copy_when_clean(self):
        record = {"body": "normal text", "id": 1}
        result = scrub_record(record)
        assert result is record  # no copy needed

    def test_copy_on_write(self):
        record = {"body": "token = ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"}
        result = scrub_record(record)
        assert result is not record
        assert "ghp_" in record["body"]  # original untouched
        assert "ghp_" not in result["body"]

    def test_handles_none_values(self):
        record = {"body": None, "patch": None}
        result = scrub_record(record)
        assert result is record


class TestWriterRedaction:
    """Integration: secrets are scrubbed when writing through FileSystemDumpWriter."""

    def test_pr_bundle_patch_redacted(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {"number": 1, "body": "normal"},
            "files": [
                {
                    "sha": "abc",
                    "filename": "map.js",
                    "patch": "+  mapboxToken: 'pk.eyJ1IjoiZm9vIiwiYSI6ImJhciJ9.secretxyz'",
                },
            ],
        }
        writer.write_pr_bundle(bundle)

        files_jsonl = tmp_path / "canonical" / "files.jsonl"
        line = json.loads(files_jsonl.read_text().strip())
        assert "pk.eyJ" not in line["patch"]
        assert _PLACEHOLDER in line["patch"]

    def test_pr_body_redacted(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {
                "number": 1,
                "body": "Use token AKIAIOSFODNN7EXAMPLE for S3",
            },
        }
        writer.write_pr_bundle(bundle)

        pr_jsonl = tmp_path / "canonical" / "pull_requests.jsonl"
        line = json.loads(pr_jsonl.read_text().strip())
        assert "AKIA" not in line["body"]

    def test_commit_message_redacted(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {"number": 1},
            "commits": [
                {"sha": "x", "message": "add ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"},
            ],
        }
        writer.write_pr_bundle(bundle)

        commits_jsonl = tmp_path / "canonical" / "commits.jsonl"
        line = json.loads(commits_jsonl.read_text().strip())
        assert "ghp_" not in line["message"]

    def test_repo_data_description_redacted(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        writer.write_repo_data("deployments", [
            {"id": 1, "description": "deploy with glpat-xxxxxxxxxxxxxxxxxxxx"},
        ], "org/repo")

        dep_jsonl = tmp_path / "canonical" / "deployments.jsonl"
        line = json.loads(dep_jsonl.read_text().strip())
        assert "glpat-" not in line["description"]

    def test_clean_data_unchanged(self, tmp_path):
        writer = FileSystemDumpWriter(tmp_path)
        bundle = {
            "pull_request": {"number": 1, "body": "Fix button alignment"},
            "reviews": [{"id": 1, "body": "LGTM"}],
        }
        writer.write_pr_bundle(bundle)

        pr_jsonl = tmp_path / "canonical" / "pull_requests.jsonl"
        line = json.loads(pr_jsonl.read_text().strip())
        assert line["body"] == "Fix button alignment"
