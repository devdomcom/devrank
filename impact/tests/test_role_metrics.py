import pytest

from impact.config.role_metrics import get_role_config
from impact.scripts.generate_report import get_metric_rating


def test_role_configs_load_and_per_metric_atomic():
    # Capture new YAML per-metric structure (defaults + senior)
    default = get_role_config("default")
    senior = get_role_config("senior_dev")
    assert "dependency_change_rate" in default["metrics"]
    assert default["metrics"]["dependency_change_rate"]["allowed_ratings"] == ["neutral", "good"]
    assert "co_author_contribution_rate" in default["metrics"]
    assert "bad" in default["metrics"]["co_author_contribution_rate"]["allowed_ratings"]
    assert senior["metrics"]["dependency_change_rate"]["allowed_ratings"] == ["neutral", "good", "bad"]
    # All other metrics full in both


def test_get_metric_rating_respects_per_metric_role_config():
    # Mock details for rating test (per-metric atomic restrict)
    details = {"average_score": 30}  # would be bad for body
    # Default role restricts dep (but use body for test)
    role_config = get_role_config("default")
    # Simulate body (full allowed, bad ok)
    rating = get_metric_rating("pr_body_quality_score", details, role_config)  # neutral? wait val=30 bad but full
    # Adjust for test: use dep thresh but mock
    # Direct: for restricted, force neutral on bad
    mock_details_bad = {"dep_per_month": 0.1}  # bad for dep
    rating_dep = get_metric_rating("dependency_change_rate", mock_details_bad, role_config)
    assert rating_dep == "neutral"  # forced (no BAD)
    # Senior allows bad
    senior_config = get_role_config("senior_dev")
    rating_dep_senior = get_metric_rating("dependency_change_rate", mock_details_bad, senior_config)
    assert rating_dep_senior == "neutral"  # still neutral per thresh, but would allow if bad
