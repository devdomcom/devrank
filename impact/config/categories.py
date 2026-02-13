"""Central registry of metric categories.

Categories are the top-level grouping for metrics (e.g. "Productivity & Throughput").
Each category has a slug (used in code/API) and a human-readable display name.
Ordering in CATEGORIES defines the display order in reports and UI.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Category:
    slug: str
    name: str


CATEGORIES: tuple[Category, ...] = (
    Category("productivity_throughput", "Productivity & Throughput"),
    Category("code_quality_size", "Code Quality & Size"),
    Category("pr_hygiene_process", "PR Hygiene & Process"),
    Category("scope_collaboration", "Scope & Collaboration"),
    Category("risk_sustainability", "Risk & Sustainability"),
    Category("influence_review", "Influence & Review"),
    Category("descriptive", "Descriptive"),
)

CATEGORY_SLUGS: frozenset[str] = frozenset(c.slug for c in CATEGORIES)

CATEGORY_BY_SLUG: dict[str, Category] = {c.slug: c for c in CATEGORIES}


def get_category_name(slug: str) -> str:
    """Get human-readable name for a category slug."""
    return CATEGORY_BY_SLUG[slug].name


def get_category_order() -> list[str]:
    """Return category slugs in display order."""
    return [c.slug for c in CATEGORIES]
