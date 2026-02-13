import pytest

from impact.config.role_metrics import get_role_config, get_available_roles
from impact.scripts.generate_report import get_metric_rating


def test_senior_dev_config_loads_all_metrics():
    config = get_role_config("senior_dev")
    assert "metrics" in config
    metrics = config["metrics"]
    # Should have all 45 metrics
    assert len(metrics) >= 45


def test_senior_dev_yaml_structure():
    config = get_role_config("senior_dev")
    metrics = config["metrics"]
    # Each metric (except net_code_contribution) should have key, direction, scores, allowed_ratings
    for slug, mcfg in metrics.items():
        if mcfg.get("no_rating"):
            continue
        assert "key" in mcfg, f"{slug} missing 'key'"
        assert "direction" in mcfg, f"{slug} missing 'direction'"
        assert "scores" in mcfg, f"{slug} missing 'scores'"
        assert "allowed_ratings" in mcfg, f"{slug} missing 'allowed_ratings'"
        assert mcfg["direction"] in ("lower_is_better", "higher_is_better", "band"), \
            f"{slug} invalid direction: {mcfg['direction']}"


def test_unknown_role_raises():
    with pytest.raises(FileNotFoundError, match="not found"):
        get_role_config("nonexistent_role")


def test_default_role_removed():
    """default.yaml should no longer exist."""
    with pytest.raises(FileNotFoundError):
        get_role_config("default")


def test_available_roles_includes_senior_dev():
    roles = get_available_roles()
    assert "senior_dev" in roles
    assert "default" not in roles


def test_allowed_ratings_filtering():
    config = get_role_config("senior_dev")
    # dependency_change_rate restricts to [neutral, good, bad] — no excellent
    dep_cfg = config["metrics"]["dependency_change_rate"]
    assert "excellent" not in dep_cfg["allowed_ratings"]
    # inline_comment_density allows [excellent, good, neutral] — no bad
    inline_cfg = config["metrics"]["inline_comment_density"]
    assert "bad" not in inline_cfg["allowed_ratings"]


def test_get_metric_rating_yaml_thresholds():
    config = get_role_config("senior_dev")
    # cycle_time: lower_is_better, excellent <= 4
    assert get_metric_rating("cycle_time", {"median_hours": 2}, config) == "excellent"
    assert get_metric_rating("cycle_time", {"median_hours": 8}, config) == "good"
    assert get_metric_rating("cycle_time", {"median_hours": 18}, config) == "neutral"
    assert get_metric_rating("cycle_time", {"median_hours": 30}, config) == "bad"


def test_get_metric_rating_higher_is_better():
    config = get_role_config("senior_dev")
    # reviews_given: higher_is_better, excellent >= 7.0
    assert get_metric_rating("reviews_given", {"reviews_per_week": 8.0}, config) == "excellent"
    assert get_metric_rating("reviews_given", {"reviews_per_week": 5.0}, config) == "good"
    assert get_metric_rating("reviews_given", {"reviews_per_week": 3.0}, config) == "neutral"
    assert get_metric_rating("reviews_given", {"reviews_per_week": 1.0}, config) == "bad"


def test_get_metric_rating_band():
    config = get_role_config("senior_dev")
    # bug_fix_focus_rate: band, excellent [20, 40]
    assert get_metric_rating("bug_fix_focus_rate", {"overall_rate": 30}, config) == "excellent"
    assert get_metric_rating("bug_fix_focus_rate", {"overall_rate": 15}, config) == "good"
    assert get_metric_rating("bug_fix_focus_rate", {"overall_rate": 90}, config) == "bad"


def test_get_metric_rating_no_data():
    config = get_role_config("senior_dev")
    assert get_metric_rating("cycle_time", {"no_data": True}, config) == "INSUFFICIENT_DATA"


def test_get_metric_rating_descriptive():
    config = get_role_config("senior_dev")
    assert get_metric_rating("net_code_contribution", {"add_to_del_ratio": 1.5}, config) == "descriptive"


def test_get_metric_rating_allowed_ratings_restricts():
    config = get_role_config("senior_dev")
    # dependency_change_rate: allowed [neutral, good, bad], direction higher_is_better
    # value 1.0 >= good threshold 0.5 → good
    assert get_metric_rating("dependency_change_rate", {"dep_per_month": 1.0}, config) == "good"


def test_custom_path_loading(tmp_path):
    """Test loading config from a custom path."""
    custom = tmp_path / "custom_role.yaml"
    custom.write_text("metrics:\n  cycle_time:\n    key: median_hours\n    direction: lower_is_better\n    excellent: 2\n    good: 6\n    neutral: 12\n    scores: [[0, 100], [2, 75], [6, 50], [12, 25], [24, 0]]\n    allowed_ratings: [excellent, good, neutral, bad]\n")
    config = get_role_config("ignored", custom_path=str(custom))
    assert "cycle_time" in config["metrics"]
    assert config["metrics"]["cycle_time"]["excellent"] == 2
