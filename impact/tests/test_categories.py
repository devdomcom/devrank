"""Tests for metric category system."""
from impact.config.categories import CATEGORIES, CATEGORY_SLUGS, get_category_name, get_category_order
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
    assert get_category_name("productivity_throughput") == "Productivity & Throughput"
    assert get_category_name("influence_review") == "Influence & Review"
    assert get_category_name("descriptive") == "Descriptive"


def test_metric_category_property_returns_string():
    """Each metric's category property returns a plain string slug."""
    for slug, cls in get_metrics().items():
        m = cls()
        assert isinstance(m.category, str), f"{slug}.category is not str"
        assert m.category in CATEGORY_SLUGS, f"{slug}.category '{m.category}' not in CATEGORY_SLUGS"
