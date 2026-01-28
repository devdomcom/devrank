import pytest
from impact.ingestion.dump import DumpIngestion
from impact.domain.models import CanonicalBundle


def test_dump_ingestion():
    dump_path = "samples/github_test/dump_2024-12"
    ingestion = DumpIngestion(dump_path)
    bundle = ingestion.ingest()

    assert isinstance(bundle, CanonicalBundle)
    assert len(bundle.users) == 4  # alice, acme, bob, carol
    assert len(bundle.repositories) == 1
    assert len(bundle.pull_requests) == 2  # alice's PRs
    assert len(bundle.commits) == 5  # alice's commits
    assert len(bundle.reviews) == 5  # alice's reviews + received on her PRs
    assert len(bundle.comments) == 5  # alice's comments + on her PRs

    # Check specific PR
    pr = next(pr for pr in bundle.pull_requests if pr.number == 42)
    assert pr.title == "Improve logging"
    assert pr.state.value == "closed"
    assert pr.merged is True

    # Check user
    alice = next(u for u in bundle.users if u.login == "alice")
    assert alice.id == 10101
    acme = next(u for u in bundle.users if u.login == "acme")
    assert acme.type.value == "Organization"