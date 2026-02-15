"""Tests for metric category system."""
from impact.config.categories import (
    CATEGORIES,
    CATEGORY_SLUGS,
    compute_group_scores,
    get_category_name,
    get_category_order,
)
from impact.metrics import get_metrics, validate_metrics


def test_validate_metrics_all_valid_categories():
    """Every registered metric must declare a category from CATEGORY_SLUGS."""
    validate_metrics()


def test_every_category_has_at_least_one_metric():
    """Every category slug should have at least one metric assigned."""
    metrics = get_metrics()
    used_categories = {cls().category for cls in metrics.values()}
    for cat in CATEGORY_SLUGS:
        assert cat in used_categories, f"Category '{cat}' has no metrics"


def test_category_order_returns_all_slugs():
    order = get_category_order()
    assert set(order) == CATEGORY_SLUGS
    assert len(order) == len(CATEGORIES)


def test_get_category_name_returns_display_names():
    assert get_category_name("delivery_velocity") == "Delivery & Velocity"
    assert get_category_name("review_impact") == "Review Impact & Mentorship"
    assert get_category_name("contextual") == "Contextual Signals"


def test_metric_category_property_returns_string():
    """Each metric's category property returns a plain string slug."""
    for slug, cls in get_metrics().items():
        m = cls()
        assert isinstance(m.category, str), f"{slug}.category is not str"
        assert m.category in CATEGORY_SLUGS, f"{slug}.category '{m.category}' not in CATEGORY_SLUGS"


# ---------------------------------------------------------------------------
# Group-averaged scoring tests
# ---------------------------------------------------------------------------


def test_compute_group_scores_basic():
    """Two groups with different scores produce correct group averages + overall."""
    pairs = [
        (80.0, "delivery_velocity"),
        (60.0, "delivery_velocity"),
        (40.0, "code_quality"),
    ]
    overall, groups = compute_group_scores(pairs)
    assert groups["delivery_velocity"] == 70.0
    assert groups["code_quality"] == 40.0
    # Overall = avg of group scores: (70 + 40) / 2
    assert overall == 55.0


def test_compute_group_scores_excludes_contextual():
    """Contextual category metrics must not affect group scores or overall."""
    pairs = [
        (90.0, "delivery_velocity"),
        (10.0, "contextual"),
    ]
    overall, groups = compute_group_scores(pairs)
    assert "contextual" not in groups
    assert overall == 90.0
    assert len(groups) == 1


def test_compute_group_scores_empty():
    """Empty input returns None overall and empty groups."""
    overall, groups = compute_group_scores([])
    assert overall is None
    assert groups == {}


def test_compute_group_scores_only_contextual():
    """All-contextual input returns None overall."""
    pairs = [(50.0, "contextual"), (30.0, "contextual")]
    overall, groups = compute_group_scores(pairs)
    assert overall is None
    assert groups == {}


def test_compute_group_scores_single_group():
    """Single group: overall == group average."""
    pairs = [(100.0, "responsiveness"), (50.0, "responsiveness")]
    overall, groups = compute_group_scores(pairs)
    assert overall == 75.0
    assert groups["responsiveness"] == 75.0


def test_compute_group_scores_equal_weight_per_group():
    """Groups get equal weight regardless of metric count (the key invariant)."""
    # group A: 1 metric scoring 100
    # group B: 3 metrics scoring 0
    # Flat avg would be 25; group avg should be (100+0)/2 = 50
    pairs = [
        (100.0, "review_effectiveness"),
        (0.0, "review_impact"),
        (0.0, "review_impact"),
        (0.0, "review_impact"),
    ]
    overall, groups = compute_group_scores(pairs)
    assert groups["review_effectiveness"] == 100.0
    assert groups["review_impact"] == 0.0
    assert overall == 50.0
