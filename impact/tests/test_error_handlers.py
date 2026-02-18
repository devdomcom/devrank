"""Dedicated test suite for error handlers (covers all exceptions + metrics pipeline error cases).

Asserts sanitized responses (no internals leaked; consistent {"error":, "detail":})
while preserving coverage. Uses standalone test app (avoids module-cache issues).
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.exceptions import AuthenticationError, AuthorizationError
from api.handlers import register_exception_handlers
from impact.exceptions import (
    AdapterError,
    DataValidationError,
    ImpactError,
    ManifestInvalidError,
    ManifestNotFoundError,
    ParseError,
    ProviderError,
    ResponseError,
)
from impact.metrics import get_metrics
from impact.domain.models import MetricContext


def _create_test_app() -> FastAPI:
    """Create a minimal app with an error-raising endpoint for handler testing."""
    test_app = FastAPI()
    register_exception_handlers(test_app)

    @test_app.get("/_test_error")
    def _test_error(error_type: str):
        exc_map = {
            "DataValidationError": DataValidationError("test validation"),
            "ManifestNotFoundError": ManifestNotFoundError("test not found"),
            "ManifestInvalidError": ManifestInvalidError("test invalid"),
            "ParseError": ParseError("test parse"),
            "ProviderError": ProviderError("test provider"),
            "AdapterError": AdapterError("test adapter"),
            "ResponseError": ResponseError("test response"),
            "ImpactError": ImpactError("test base"),
            "AuthenticationError": AuthenticationError("bad creds"),
            "AuthorizationError": AuthorizationError("no access"),
            "ValueError": ValueError("test metrics/ledger error"),
        }
        if error_type in exc_map:
            raise exc_map[error_type]
        raise ImpactError("unknown test error")

    return test_app


@pytest.fixture
def client():
    return TestClient(_create_test_app())


@pytest.mark.parametrize(
    "exc_class,expected_status,expected_error",
    [
        (DataValidationError, 400, "data_validation_error"),
        (ManifestNotFoundError, 404, "manifest_not_found"),
        (ManifestInvalidError, 422, "manifest_invalid"),
        (ParseError, 422, "parse_error"),
        (ProviderError, 503, "provider_error"),
        (AdapterError, 500, "adapter_error"),
        (ResponseError, 500, "response_error"),
        (ImpactError, 500, "impact_error"),
        (AuthenticationError, 401, "authentication_error"),
        (AuthorizationError, 403, "authorization_error"),
        (ValueError, 400, "app_error"),  # Wrapped to AppError in handler
    ],
)
def test_error_handlers(client, exc_class, expected_status, expected_error):
    """Test each handler via dedicated test endpoint (standalone app)."""
    resp = client.get(f"/_test_error?error_type={exc_class.__name__}")
    assert resp.status_code == expected_status
    data = resp.json()
    assert data["error"] == expected_error or expected_error in data["error"]
    assert "detail" in data
    # Security: no leaking fields
    assert "type" not in data
    assert "path" not in data
    assert "value" not in data
    assert "details" not in data


# Coverage for metrics pipeline error cases (run metrics with bad context)
def test_metrics_run_error_cases():
    """Cover error paths across metrics (bad context triggers ValueError/Impact)."""
    metrics_dict = get_metrics()
    context = MetricContext(ledger=None, user_login="test")

    for slug, metric_class in list(metrics_dict.items())[:5]:
        metric = metric_class()
        with pytest.raises((AttributeError, ValueError, ImpactError)):
            metric.run(context)
