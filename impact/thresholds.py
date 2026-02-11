# Thresholds for rating metrics (using best judgment for defaults)
# These thresholds determine the qualitative rating (excellent/good/neutral/bad) for each metric.
# Ratings are based on the specified key in the metric's details.
#
# Each threshold entry also includes a "scores" list of (value, score) breakpoints for continuous
# 0-100 scoring with linear interpolation. This eliminates hard cliff effects where a tiny change
# in the metric value (e.g. 1.00h vs 1.01h) causes a discrete rating jump.
#
# Time Window Rationale:
# - review_led_to_commit: 24h (tight correlation -- immediate response)
# - review_led_to_merge: 48h (merge can take longer due to CI/approval)
# - _is_effective_change_request: 72h (allows for complex changes)
# - approval_was_final: 48h (same as merge window)

from typing import Any

# Central no-data util (DRY for guards across rating/score/metrics)
from impact.metrics.utils import is_no_data

METRIC_THRESHOLDS = {
    "pr_throughput": {
        "key": "merge_ratio",
        "excellent": lambda x: x >= 0.9,
        "good": lambda x: 0.7 <= x < 0.9,
        "neutral": lambda x: 0.5 <= x < 0.7,
        "bad": lambda x: x < 0.5,
        "scores": [(0, 0), (0.5, 25), (0.7, 50), (0.9, 75), (1.0, 100)],
    },
    "cycle_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 3,
        "neutral": lambda x: 3 < x <= 7,
        "bad": lambda x: x > 7,
        "scores": [(0, 100), (1, 75), (3, 50), (7, 25), (14, 0)],
    },
    "pr_merge_effectiveness": {
        "key": "average_back_and_forth",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
        "scores": [(0, 100), (1, 75), (2, 50), (4, 25), (8, 0)],
    },
    "review_leverage": {
        "key": "effectiveness_rate",
        "excellent": lambda x: x >= 0.8,
        "good": lambda x: 0.6 <= x < 0.8,
        "neutral": lambda x: 0.3 <= x < 0.6,
        "bad": lambda x: x < 0.3,
        "scores": [(0, 0), (0.3, 25), (0.6, 50), (0.8, 75), (1.0, 100)],
    },
    "pr_size_distribution": {
        "key": "large_pr_percent",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
        "scores": [(0, 100), (5, 75), (15, 50), (30, 25), (60, 0)],
    },
    "trivial_contribution_rate": {
        "key": "trivial_prs_per_day",
        "excellent": lambda x: x <= 0.05,  # <=1 trivial PR per 20 days
        "good": lambda x: 0.05 < x <= 0.15,  # 1-3 per 20 days
        "neutral": lambda x: 0.15 < x <= 0.3,  # 3-6 per 20 days
        "bad": lambda x: x > 0.3,  # >6 per 20 days
        "scores": [(0, 100), (0.05, 75), (0.15, 50), (0.3, 25), (0.6, 0)],
    },
    "module_area_breadth": {
        "key": "areas_per_pr",
        "excellent": lambda x: x >= 2.0,  # >=2 areas per PR shows good breadth
        "good": lambda x: 1.0 <= x < 2.0,
        "neutral": lambda x: 0.5 <= x < 1.0,
        "bad": lambda x: x < 0.5,
        "scores": [(0, 0), (0.5, 25), (1.0, 50), (2.0, 75), (4.0, 100)],
    },
    "review_iterations": {
        "key": "average_iterations",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
        "scores": [(0, 100), (1, 75), (2, 50), (4, 25), (8, 0)],
    },
    "time_to_first_review": {
        "key": "median_hours",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 6,
        "neutral": lambda x: 6 < x <= 24,
        "bad": lambda x: x > 24,
        "scores": [(0, 100), (1, 75), (6, 50), (24, 25), (48, 0)],
    },
    "slow_review_response": {
        "key": "median_hours",
        "excellent": lambda x: x <= 2,
        "good": lambda x: 2 < x <= 12,
        "neutral": lambda x: 12 < x <= 48,
        "bad": lambda x: x > 48,
        "scores": [(0, 100), (2, 75), (12, 50), (48, 25), (96, 0)],
    },
    # Active weeks: lower max gap = fewer absences/disengagement
    "active_weeks": {
        "key": "max_gap_weeks",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
        "scores": [(0, 100), (1, 75), (2, 50), (4, 25), (8, 0)],
    },
    # Burstiness: lower ratio = steadier/sustainable (vs big bursts)
    "burstiness": {
        "key": "burst_ratio",
        "excellent": lambda x: x <= 1.5,
        "good": lambda x: 1.5 < x <= 2.5,
        "neutral": lambda x: 2.5 < x <= 4.0,
        "bad": lambda x: x > 4.0,
        "scores": [(0, 100), (1.5, 75), (2.5, 50), (4.0, 25), (8.0, 0)],
    },
    # Reviews Given: opinionated on collab volume (industry ~2-5/week avg for active)
    "reviews_given": {
        "key": "reviews_per_week",
        "excellent": lambda x: x >= 5.0,
        "good": lambda x: 3.0 <= x < 5.0,
        "neutral": lambda x: 1.0 <= x < 3.0,
        "bad": lambda x: x < 1.0,
        "scores": [(0, 0), (1.0, 25), (3.0, 50), (5.0, 75), (10.0, 100)],
    },
    # PR Merge Rate: high % of reviews leading to merge = strong influence
    "pr_merge_rate": {
        "key": "merge_rate",
        "excellent": lambda x: x >= 0.8,
        "good": lambda x: 0.6 <= x < 0.8,
        "neutral": lambda x: 0.4 <= x < 0.6,
        "bad": lambda x: x < 0.4,
        "scores": [(0, 0), (0.4, 25), (0.6, 50), (0.8, 75), (1.0, 100)],
    },
    # Change-Inducing Review Rate: high % inducing commits = clear impact
    "change_inducing_review_rate": {
        "key": "inducing_rate",
        "excellent": lambda x: x >= 0.7,
        "good": lambda x: 0.5 <= x < 0.7,
        "neutral": lambda x: 0.3 <= x < 0.5,
        "bad": lambda x: x < 0.3,
        "scores": [(0, 0), (0.3, 25), (0.5, 50), (0.7, 75), (1.0, 100)],
    },
    # Approval To Merge Ratio: high = accurate approvals on ready code
    "approval_to_merge_ratio": {
        "key": "ratio",
        "excellent": lambda x: x >= 0.8,
        "good": lambda x: 0.6 <= x < 0.8,
        "neutral": lambda x: 0.4 <= x < 0.6,
        "bad": lambda x: x < 0.4,
        "scores": [(0, 0), (0.4, 25), (0.6, 50), (0.8, 75), (1.0, 100)],
    },
    # Review Turnaround: lower time = fast action (aligned closer to time_to_first_review;
    # previously 12x gap between author-facing 1h and reviewer-facing 12h)
    "review_turnaround_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 4,
        "good": lambda x: 4 < x <= 8,
        "neutral": lambda x: 8 < x <= 16,
        "bad": lambda x: x > 16,
        "scores": [(0, 100), (4, 75), (8, 50), (16, 25), (32, 0)],
    },
    # Blocking Comment Rate: moderate per day = ownership (balanced by period; band metric)
    "blocking_comment_rate": {
        "key": "blocking_per_day",
        "excellent": lambda x: 0.5 <= x <= 2.0,
        "good": lambda x: 0.2 <= x < 0.5 or 2.0 < x <= 3.0,
        "neutral": lambda x: 0.05 <= x < 0.2 or 3.0 < x <= 5.0,
        "bad": lambda x: x < 0.05 or x > 5.0,
        "scores": [(0, 0), (0.05, 25), (0.2, 50), (0.5, 75), (1.25, 100), (2.0, 75), (3.0, 50), (5.0, 25), (10.0, 0)],
    },
    # Unblock Time: lower = fast re-review after blocks (industry <48h)
    "unblock_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 12,
        "good": lambda x: 12 < x <= 24,
        "neutral": lambda x: 24 < x <= 48,
        "bad": lambda x: x > 48,
        "scores": [(0, 100), (12, 75), (24, 50), (48, 25), (96, 0)],
    },
    # Bug Fix Focus Rate: moderate % best (band metric; 20-40% balanced)
    "bug_fix_focus_rate": {
        "key": "overall_rate",
        "excellent": lambda x: 20 <= x <= 40,
        "good": lambda x: 10 <= x < 20 or 40 < x <= 60,
        "neutral": lambda x: 5 <= x < 10 or 60 < x <= 75,
        "bad": lambda x: x < 5 or x > 75,
        "scores": [(0, 0), (5, 25), (10, 50), (20, 75), (30, 100), (40, 75), (60, 50), (75, 25), (100, 0)],
    },
    # Revert Introduction Rate: low % = stability (per proposal; <5% best practice)
    "revert_introduction_rate": {
        "key": "rate",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
        "scores": [(0, 100), (5, 75), (15, 50), (30, 25), (60, 0)],
    },
    # Test File Ratio: >=25% test changes = good discipline (industry benchmark; 0% flags weak testing)
    "test_file_ratio": {
        "key": "ratio",
        "excellent": lambda x: x >= 25,
        "good": lambda x: 15 <= x < 25,
        "neutral": lambda x: 5 <= x < 15,
        "bad": lambda x: x < 5,
        "scores": [(0, 0), (5, 25), (15, 50), (25, 75), (50, 100)],
    },
    # PR Body Quality: higher score better (structure/length/refs)
    "pr_body_quality_score": {
        "key": "average_score",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
        "scores": [(0, 0), (40, 25), (60, 50), (80, 75), (100, 100)],
    },
    # Co-Author Rate: normalized events/week (period-balanced; short periods lenient, long expect density)
    "co_author_contribution_rate": {
        "key": "collab_per_week",
        "excellent": lambda x: x >= 2.0,
        "good": lambda x: 1.0 <= x < 2.0,
        "neutral": lambda x: 0.5 <= x < 1.0,
        "bad": lambda x: x < 0.5,
        "scores": [(0, 0), (0.5, 25), (1.0, 50), (2.0, 75), (4.0, 100)],
    },
    # Dep Change Rate: updates/month (neutral if low/0; never BAD by default per role config)
    "dependency_change_rate": {
        "key": "dep_per_month",
        "good": lambda x: x >= 0.5,
        "neutral": lambda x: x < 0.5,
        "scores": [(0, 50), (0.5, 75), (2.0, 100)],
    },
    # Inline Comment Density: avg per PR (higher=depth; neutral low, no dup iterations)
    "inline_comment_density": {
        "key": "avg_inline_per_pr",
        "excellent": lambda x: x >= 5,
        "good": lambda x: 2 <= x < 5,
        "neutral": lambda x: x < 2,
        "scores": [(0, 50), (2, 75), (5, 100)],
    },
    # Conventional Commit Rate: % conventional (industry best practices; higher better)
    "conventional_commit_rate": {
        "key": "conventional_commit_rate",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
        "scores": [(0, 0), (40, 25), (60, 50), (80, 75), (100, 100)],
    },
    # Code Churn Rate: low max-weekly % = stable (spikes/instability bad; period-len aware)
    "code_churn_rate": {
        "key": "max_weekly_churn",
        "excellent": lambda x: x <= 10,
        "good": lambda x: 10 < x <= 25,
        "neutral": lambda x: 25 < x <= 40,
        "bad": lambda x: x > 40,
        "scores": [(0, 100), (10, 75), (25, 50), (40, 25), (80, 0)],
    },
    # Self-Merge Rate: low % = good (no approval skips); worst if repo low but engineer high
    "self_merge_rate": {
        "key": "self_merge_rate",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 20,
        "neutral": lambda x: 20 < x <= 40,
        "bad": lambda x: x > 40,
        "scores": [(0, 100), (5, 75), (20, 50), (40, 25), (80, 0)],
    },
    # Abandoned PR Rate: low % = good; worsens w/ longer window (weighted)
    "abandoned_pr_rate": {
        "key": "weighted_score",
        "excellent": lambda x: x <= 10,
        "good": lambda x: 10 < x <= 30,
        "neutral": lambda x: 30 < x <= 60,
        "bad": lambda x: x > 60,
        "scores": [(0, 100), (10, 75), (30, 50), (60, 25), (100, 0)],
    },
    # Doc Touch Rate: touches/month (neutral/low ok; no BAD default per role config)
    "documentation_touch_rate": {
        "key": "doc_per_month",
        "good": lambda x: x >= 0.5,
        "neutral": lambda x: x < 0.5,
        "scores": [(0, 50), (0.5, 75), (2.0, 100)],
    },
    # Net Code Contribution: descriptive only (no rating; ratio for assessment; role knobs ignored)
    "net_code_contribution": {
        "key": "add_to_del_ratio",
        "no_rating": True,  # new pattern: disable rating
    },
    # Follow-Up Commit Rate: moderate = healthy iteration (band metric; very high = poor initial quality)
    "follow_up_commit_rate": {
        "key": "follow_up_rate",
        "excellent": lambda x: 30 <= x <= 60,
        "good": lambda x: 15 <= x < 30 or 60 < x <= 75,
        "neutral": lambda x: 5 <= x < 15 or 75 < x <= 85,
        "bad": lambda x: x < 5 or x > 85,
        "scores": [(0, 25), (15, 50), (30, 75), (45, 100), (60, 75), (75, 50), (85, 25), (100, 0)],
    },
    # PR Category Diversity: more distinct types = well-rounded engineer
    "pr_category_diversity": {
        "key": "distinct_categories",
        "excellent": lambda x: x >= 5,
        "good": lambda x: 3 <= x < 5,
        "neutral": lambda x: 2 <= x < 3,
        "bad": lambda x: x < 2,
        "scores": [(1, 20), (2, 40), (3, 60), (4, 80), (5, 100)],
    },
    # Review Breadth: more unique authors = broader influence
    "review_breadth": {
        "key": "unique_authors",
        "excellent": lambda x: x >= 8,
        "good": lambda x: 5 <= x < 8,
        "neutral": lambda x: 2 <= x < 5,
        "bad": lambda x: x < 2,
        "scores": [(0, 0), (2, 25), (5, 50), (8, 75), (12, 100)],
    },
    # Review Comment Substance: higher score = more actionable/detailed feedback
    "review_comment_substance": {
        "key": "avg_substance_score",
        "excellent": lambda x: x >= 60,
        "good": lambda x: 40 <= x < 60,
        "neutral": lambda x: 20 <= x < 40,
        "bad": lambda x: x < 20,
        "scores": [(0, 0), (20, 25), (40, 50), (60, 75), (80, 100)],
    },
    # Mentorship Signal: higher % reviewing junior authors = more investment in team growth
    "mentorship_signal": {
        "key": "mentorship_rate",
        "excellent": lambda x: x >= 40,
        "good": lambda x: 20 <= x < 40,
        "neutral": lambda x: 10 <= x < 20,
        "bad": lambda x: x < 10,
        "scores": [(0, 0), (10, 25), (20, 50), (40, 75), (60, 100)],
    },
    # Off-Hours Activity Rate: low % = good sustainability (high burnout risk)
    "off_hours_activity_rate": {
        "key": "off_hours_rate",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
        "scores": [(0, 100), (5, 75), (15, 50), (30, 25), (60, 0)],
    },
}


def score_metric(slug: str, value: float) -> float | None:
    """Return a continuous 0-100 score for a metric value using linear interpolation.

    Uses the "scores" breakpoints defined in METRIC_THRESHOLDS to interpolate
    smoothly between rating bands, eliminating hard cliff effects.

    Returns None if the metric has no scoring breakpoints (e.g. no_rating metrics).
    """
    thresh = METRIC_THRESHOLDS.get(slug)
    if not thresh or "scores" not in thresh:
        return None

    breakpoints = thresh["scores"]
    # Clamp to the range of defined breakpoints
    if value <= breakpoints[0][0]:
        return float(breakpoints[0][1])
    if value >= breakpoints[-1][0]:
        return float(breakpoints[-1][1])

    # Find the two surrounding breakpoints and interpolate linearly
    for i in range(len(breakpoints) - 1):
        x0, y0 = breakpoints[i]
        x1, y1 = breakpoints[i + 1]
        if x0 <= value <= x1:
            t = (value - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)

    return float(breakpoints[-1][1])


def get_continuous_score(metric_slug: str, details: dict[str, Any]) -> float | None:
    """Compute continuous 0-100 score (DRY helper)."""
    if is_no_data(details):
        return None
    key = METRIC_THRESHOLDS.get(metric_slug, {}).get("key")
    if key and key in details:
        try:
            return score_metric(metric_slug, float(details[key]))
        except (TypeError, ValueError):
            return None
    return None
