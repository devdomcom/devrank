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
    Category("delivery_velocity", "Delivery & Velocity"),
    Category("code_quality", "Code Quality"),
    Category("process_discipline", "Process & Discipline"),
    Category("responsiveness", "Responsiveness & Follow-through"),
    Category("review_impact", "Review Impact & Mentorship"),
    Category("review_effectiveness", "Review Effectiveness"),
    Category("contextual", "Contextual Signals"),
)

# Contextual category is excluded from group-averaged scoring
UNSCORED_CATEGORIES: frozenset[str] = frozenset({"contextual"})

CATEGORY_SLUGS: frozenset[str] = frozenset(c.slug for c in CATEGORIES)

CATEGORY_BY_SLUG: dict[str, Category] = {c.slug: c for c in CATEGORIES}


def get_category_name(slug: str) -> str:
    """Get human-readable name for a category slug."""
    return CATEGORY_BY_SLUG[slug].name


def get_category_order() -> list[str]:
    """Return category slugs in display order."""
    return [c.slug for c in CATEGORIES]


def compute_group_scores(
    score_category_pairs: list[tuple[float, str]],
) -> tuple[float | None, dict[str, float]]:
    """Compute group-averaged overall score from (score, category) pairs.

    Each scored category gets one equal-weight vote in the overall score,
    regardless of how many metrics it contains.

    Returns (overall_score, {category_slug: group_avg}).
    """
    from collections import defaultdict

    groups: dict[str, list[float]] = defaultdict(list)
    for score, cat in score_category_pairs:
        if cat in UNSCORED_CATEGORIES:
            continue
        groups[cat].append(score)

    group_scores: dict[str, float] = {}
    for cat, scores in groups.items():
        if scores:
            group_scores[cat] = sum(scores) / len(scores)

    if not group_scores:
        return None, {}

    overall = sum(group_scores.values()) / len(group_scores)
    return overall, group_scores
