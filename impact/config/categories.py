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
    category_weights: dict[str, float] | None = None,
) -> tuple[float | None, dict[str, float]]:
    """Compute group-averaged overall score from (score, category) pairs.

    Each scored category gets one vote in the overall score. If category_weights
    is provided, the overall score is a weighted average; otherwise equal weight.
    Weights must be >= 0. Categories in UNSCORED_CATEGORIES are always excluded.

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

    if category_weights:
        # Weighted average: sum(w_i * s_i) / sum(w_i)
        # Ignore weights for unscored categories and categories not present
        total_weight = 0.0
        weighted_sum = 0.0
        for cat, score in group_scores.items():
            w = category_weights.get(cat, 1.0)  # default weight 1.0 if not specified
            if w < 0:
                w = 0.0
            total_weight += w
            weighted_sum += w * score
        overall = weighted_sum / total_weight if total_weight > 0 else None
    else:
        overall = sum(group_scores.values()) / len(group_scores)

    return overall, group_scores
