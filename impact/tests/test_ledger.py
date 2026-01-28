import pytest
from datetime import datetime
from impact.ledger.ledger import Ledger
from impact.ingestion.dump import DumpIngestion


def test_ledger_indexes():
    dump_path = "impact/samples/github_test/dump_2024-12"
    ingestion = DumpIngestion(dump_path)
    bundle = ingestion.ingest()
    ledger = Ledger(bundle)

    # Test user_prs
    alice_prs = ledger.user_prs.get('alice', [])
    assert len(alice_prs) == 2
    assert all(pr.user.login == 'alice' for pr in alice_prs)

    # Test pr_reviews
    reviews_42 = ledger.pr_reviews.get(42, [])
    assert len(reviews_42) == 2  # bob's reviews

    # Test user_reviews
    alice_reviews = ledger.user_reviews.get('alice', [])
    assert len(alice_reviews) == 3  # her reviews

    # Test get_reviews_for_user
    reviews = ledger.get_reviews_for_user('alice')
    assert len(reviews) == 3
    assert all(r.user.login == 'alice' for r in reviews)


def test_ledger_time_filtering():
    dump_path = "impact/samples/github_test/dump_2024-12"
    ingestion = DumpIngestion(dump_path)
    bundle = ingestion.ingest()
    ledger = Ledger(bundle)

    # Assuming manifest dates
    start = datetime(2024, 12, 1)
    end = datetime(2024, 12, 31)

    prs = ledger.get_prs_for_user('alice', start, end)
    assert len(prs) == 2

    reviews = ledger.get_reviews_for_user('alice', start, end)
    assert len(reviews) == 3
