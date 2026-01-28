import pytest
from impact.metrics.plugins.review_leverage import ReviewLeverage
from impact.ingestion.dump import DumpIngestion
from impact.ledger.ledger import Ledger
from impact.domain.models import MetricContext


def test_review_leverage_metric():
    dump_path = "samples/github_test/dump_2024-12"
    ingestion = DumpIngestion(dump_path)
    bundle = ingestion.ingest()
    ledger = Ledger(bundle)

    # Mock context
    context = MetricContext(
        ledger=ledger,
        user_login='alice'
    )

    metric = ReviewLeverage()
    result = metric.run(context)

    assert result.metric_slug == "review_leverage"
    assert "PRs reviewed" in result.summary
    assert isinstance(result.details, dict)
    assert "total_reviews" in result.details
    assert "effective_changes" in result.details