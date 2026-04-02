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
    "delivery_volume": {
        "key": "merged_per_week",
        "excellent": lambda x: x >= 2.0,
        "good": lambda x: 1.0 <= x < 2.0,
        "neutral": lambda x: 0.5 <= x < 1.0,
        "bad": lambda x: x < 0.5,
        "scores": [(0, 0), (0.5, 25), (1.0, 50), (2.0, 75), (4.0, 100)],
    },
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
    # Coding Time To PR: lower = fast pre-PR coding (elite <8h per benchmarks)
    "coding_time_to_pr": {
        "key": "median_hours",
        "excellent": lambda x: x <= 8,
        "good": lambda x: 8 < x <= 24,
        "neutral": lambda x: 24 < x <= 48,
        "bad": lambda x: x > 48,
        "scores": [(0, 100), (8, 75), (24, 50), (48, 25), (72, 0)],
    },
    # Merge Delay: lower = fast post-approval to merge (isolates CI/gates; elite <4h)
    "merge_delay": {
        "key": "median_hours",
        "excellent": lambda x: x <= 4,
        "good": lambda x: 4 < x <= 12,
        "neutral": lambda x: 12 < x <= 24,
        "bad": lambda x: x > 24,
        "scores": [(0, 100), (4, 75), (12, 50), (24, 25), (48, 0)],
    },
    "pr_merge_effectiveness": {
        "key": "merge_effectiveness",  # aligned with detail key
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
    # Coding Days: high ratio = consistent daily work (elite ~60-80% per benchmarks)
    "coding_days": {
        "key": "ratio",
        "excellent": lambda x: x >= 0.7,
        "good": lambda x: 0.5 <= x < 0.7,
        "neutral": lambda x: 0.3 <= x < 0.5,
        "bad": lambda x: x < 0.3,
        "scores": [(0, 0), (0.3, 25), (0.5, 50), (0.7, 75), (1.0, 100)],
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
    # Review Demand: high requests/week = sought-after reviewer (demand signal)
    "review_demand": {
        "key": "demand_per_week",
        "excellent": lambda x: x >= 10.0,
        "good": lambda x: 5.0 <= x < 10.0,
        "neutral": lambda x: 2.0 <= x < 5.0,
        "bad": lambda x: x < 2.0,
        "scores": [(0, 0), (2.0, 25), (5.0, 50), (10.0, 75), (20.0, 100)],
    },
    # First Reviewer Rate: high = driving reviews as first (position/initiative)
    "first_reviewer_rate": {
        "key": "rate",
        "excellent": lambda x: x >= 0.6,
        "good": lambda x: 0.4 <= x < 0.6,
        "neutral": lambda x: 0.2 <= x < 0.4,
        "bad": lambda x: x < 0.2,
        "scores": [(0, 0), (0.2, 25), (0.4, 50), (0.6, 75), (1.0, 100)],
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
    # First-Time Approval Rate: high = PRs approved immediately (elite >80%)
    "first_time_approval_rate": {
        "key": "rate",
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
    # Inline Comment Density: avg per PR (higher=depth; 0 with 0 PRs is bad signal)
    "inline_comment_density": {
        "key": "avg_inline_per_pr",
        "excellent": lambda x: x >= 5,
        "good": lambda x: 2 <= x < 5,
        "neutral": lambda x: 0.5 <= x < 2,
        "bad": lambda x: x < 0.5,
        "scores": [(0, 0), (0.5, 25), (2, 50), (5, 75), (8, 100)],
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
    # Rework Rate: low % = minimal self-rework on recent code (DORA elite <2%; high=bad)
    "rework_rate": {
        "key": "rework_rate",
        "excellent": lambda x: x <= 2,
        "good": lambda x: 2 < x <= 8,
        "neutral": lambda x: 8 < x <= 16,
        "bad": lambda x: x > 16,
        "scores": [(0, 100), (2, 75), (8, 50), (16, 25), (30, 0)],
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
    # Abandoned PR Rate: low % stale = good (0-100 scale, same as abandoned_rate)
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
    # Hotspot Detection: lower max score = more stable codebase (fewer high-churn files)
    "hotspot_detection": {
        "key": "max_hotspot_score",
        "excellent": lambda x: x <= 50,
        "good": lambda x: 50 < x <= 150,
        "neutral": lambda x: 150 < x <= 300,
        "bad": lambda x: x > 300,
        "scores": [(0, 100), (50, 75), (150, 50), (300, 25), (600, 0)],
    },
    # Bus Factor: higher = more resilient (Tornhill CodeScene benchmarks: >=3 healthy)
    "bus_factor": {
        "key": "bus_factor",
        "excellent": lambda x: x >= 4,
        "good": lambda x: 3 <= x < 4,
        "neutral": lambda x: 2 <= x < 3,
        "bad": lambda x: x < 2,
        "scores": [(0, 0), (1, 20), (2, 50), (3, 75), (4, 100)],
    },
    # Knowledge Islands: descriptive risk metric (no qualitative rating)
    # High ownership concentration = maintenance risk; informative only
    "knowledge_islands": {
        "key": "island_count",
        "no_rating": True,
    },
    # Knowledge Loss: descriptive risk metric (no qualitative rating)
    # Code written by departed/inactive contributors = knowledge gap risk; informative only
    "knowledge_loss": {
        "key": "loss_count",
        "no_rating": True,
    },
    # Knowledge Sharing Index: descriptive team metric (no qualitative rating)
    # Measures how evenly reviews distribute across team (0-1 scale); informative only
    "knowledge_sharing_index": {
        "key": "sharing_index",
        "no_rating": True,
    },
    # Main Developer (by revisions): descriptive per-file ownership metric
    # Primary author per file by commit count; informative only
    "main_developer_by_revisions": {
        "key": "ownership_pct",
        "no_rating": True,
    },
    # Main Developer (by lines): descriptive per-file ownership metric
    # Primary author per file by line changes; informative only
    "main_developer_by_lines": {
        "key": "ownership_pct",
        "no_rating": True,
    },
    # Code Familiarity: % of codebase known by current active team
    # Higher is better — measures diffusion of knowledge across the active team
    "code_familiarity": {
        "key": "familiarity_pct",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
        "scores": [(0, 0), (40, 25), (60, 50), (80, 75), (100, 100)],
    },
    # Temporal/Logical Coupling: lower max ratio = fewer hidden dependencies
    # Note: coupling_ratio = shared_revisions / avg_revisions × 100, max is 100
    "temporal_logical_coupling": {
        "key": "max_coupling_ratio",
        "excellent": lambda x: x <= 30,
        "good": lambda x: 30 < x <= 60,
        "neutral": lambda x: 60 < x <= 85,
        "bad": lambda x: x > 85,
        "scores": [(0, 100), (30, 75), (60, 50), (85, 25), (100, 0)],
    },
    # Entity Fragmentation: lower index = more concentrated work
    # fragmentation_index = 1 - sum(share^2), range 0..(1-1/n)
    "entity_fragmentation": {
        "key": "fragmentation_index",
        "excellent": lambda x: x <= 0.35,
        "good": lambda x: 0.35 < x <= 0.55,
        "neutral": lambda x: 0.55 < x <= 0.75,
        "bad": lambda x: x > 0.75,
        "scores": [(0.0, 100), (0.35, 75), (0.55, 50), (0.75, 25), (0.9, 0)],
    },
    # Entity Ownership: avg top-owner pct across files; lower = more distributed
    # (i.e., good for bus factor / knowledge sharing). Higher = concentrated risk.
    "entity_ownership": {
        "key": "avg_top_owner_pct",
        "excellent": lambda x: x <= 50,
        "good": lambda x: 50 < x <= 65,
        "neutral": lambda x: 65 < x <= 80,
        "bad": lambda x: x > 80,
        "scores": [(0, 100), (50, 75), (65, 50), (80, 25), (100, 0)],
    },
    # Contributor Experience: relative share of codebase activity by the target
    # developer. Higher = deeper familiarity with the codebase.
    "contributor_experience": {
        "key": "experience_pct",
        "excellent": lambda x: x >= 30,
        "good": lambda x: 15 <= x < 30,
        "neutral": lambda x: 5 <= x < 15,
        "bad": lambda x: x < 5,
        "scores": [(0, 0), (5, 25), (15, 50), (30, 75), (50, 100)],
    },
    # Complexity Trend: lower std dev implies stable complexity
    "complexity_trend": {
        "key": "max_std_dev",
        "excellent": lambda x: x <= 0.4,
        "good": lambda x: 0.4 < x <= 0.8,
        "neutral": lambda x: 0.8 < x <= 1.2,
        "bad": lambda x: x > 1.2,
        "scores": [(0.0, 100), (0.4, 75), (0.8, 50), (1.2, 25), (2.0, 0)],
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
    # Change Proximity: lower avg distance = concentrated/focused changes (safer)
    # High avg distance = scattered changes across files (riskier)
    "change_proximity": {
        "key": "avg_proximity_per_change",
        "excellent": lambda x: x <= 5,    # Changes within 5 lines of each other
        "good": lambda x: 5 < x <= 15,   # Moderately concentrated
        "neutral": lambda x: 15 < x <= 30,  # Somewhat scattered
        "bad": lambda x: x > 30,         # Highly scattered changes
        "scores": [(0, 100), (5, 75), (15, 50), (30, 25), (60, 0)],
    },
    # Sum of Coupling: lower max coupling = fewer entanglements
    "sum_of_coupling": {
        "key": "max_coupling_score",
        "excellent": lambda x: x <= 2,
        "good": lambda x: 2 < x <= 6,
        "neutral": lambda x: 6 < x <= 12,
        "bad": lambda x: x > 12,
        "scores": [(0, 100), (2, 75), (6, 50), (12, 25), (24, 0)],
    },
    # Absolute Churn Trend: lower daily spikes indicate smoother integration
    "absolute_churn_trend": {
        "key": "max_daily_churn",
        "excellent": lambda x: x <= 50,
        "good": lambda x: 50 < x <= 150,
        "neutral": lambda x: 150 < x <= 300,
        "bad": lambda x: x > 300,
        "scores": [(0, 100), (50, 75), (150, 50), (300, 25), (600, 0)],
    },
    # Commit Message Mining: lower defect-rate indicates fewer bug fixes
    "commit_message_mining": {
        "key": "defect_commit_rate",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
        "scores": [(0, 100), (5, 75), (15, 50), (30, 25), (60, 0)],
    },
    # Code Survival: higher % still present indicates higher durability/impact
    "code_survival": {
        "key": "survival_rate",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
        "scores": [(0, 0), (40, 25), (60, 50), (80, 75), (100, 100)],
    },
    # Review Coverage: higher % of files with inline review comments = better quality gate
    "review_coverage": {
        "key": "coverage_pct",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
        "scores": [(0, 0), (40, 25), (60, 50), (80, 75), (100, 100)],
    },
    # AI Adoption Rate: per-engineer indicator of AI tool usage (descriptive only; no qualitative rating)
    "ai_adoption_rate": {
        "key": "has_adopted_ai",
        "no_rating": True,  # informative/descriptive only (org policy, not skill)
    },
    # AI-Assisted PR Rate: measures AI adoption (transparency metric; higher = more AI usage)
    "ai_assisted_pr_rate": {
        "key": "ai_rate",
        "excellent": lambda x: x >= 70,
        "good": lambda x: 40 <= x < 70,
        "neutral": lambda x: 10 <= x < 40,
        "bad": lambda x: x < 10,
        "scores": [(0, 0), (10, 25), (40, 50), (70, 75), (100, 100)],
    },
    # AI Code Quality: rework ratio (AI iterations / human iterations); lower = AI code is higher quality
    "ai_code_quality": {
        "key": "quality_ratio",
        "excellent": lambda x: x <= 0.8,  # AI needs less rework than human
        "good": lambda x: 0.8 < x <= 1.2,  # Comparable quality
        "neutral": lambda x: 1.2 < x <= 2.0,  # AI needs somewhat more rework
        "bad": lambda x: x > 2.0,  # AI needs significantly more rework
        "scores": [(0, 100), (0.8, 75), (1.2, 50), (2.0, 25), (4.0, 0)],
    },
    # AI Phantom Ownership: descriptive/contextual — risk signal, not quality outcome.
    # Whether unreviewed AI code is "bad" depends on team size, CI maturity,
    # and code criticality.  Data is fully available for teams to interpret.
    "ai_phantom_ownership": {
        "key": "phantom_rate",
        "no_rating": True,  # descriptive only — team-culture dependent risk signal
    },
    # AI Suggestion Acceptance: % of AI suggestions accepted (higher = better)
    "ai_suggestion_acceptance": {
        "key": "acceptance_rate",
        "excellent": lambda x: x >= 70,  # High trust in AI suggestions
        "good": lambda x: 50 <= x < 70,
        "neutral": lambda x: 30 <= x < 50,
        "bad": lambda x: x < 30,  # Low trust / poor suggestions
        "scores": [(0, 0), (30, 25), (50, 50), (70, 75), (100, 100)],
    },
}


def _interpolate_breakpoints(value: float, breakpoints: list) -> float:
    """Linear interpolation over (value, score) breakpoints. Shared by code + YAML paths."""
    if value <= breakpoints[0][0]:
        return float(breakpoints[0][1])
    if value >= breakpoints[-1][0]:
        return float(breakpoints[-1][1])
    for i in range(len(breakpoints) - 1):
        x0, y0 = breakpoints[i]
        x1, y1 = breakpoints[i + 1]
        if x0 <= value <= x1:
            t = (value - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return float(breakpoints[-1][1])


def score_metric(slug: str, value: float) -> float | None:
    """Return a continuous 0-100 score using METRIC_THRESHOLDS breakpoints (code fallback)."""
    thresh = METRIC_THRESHOLDS.get(slug)
    if not thresh or "scores" not in thresh:
        return None
    return _interpolate_breakpoints(value, thresh["scores"])


# --- YAML role threshold resolution ---


def resolve_role_metric_cfg(slug: str, role_config: dict | None) -> dict | None:
    """Return the YAML metric config for *slug* if it defines thresholds, else None."""
    if not role_config or "metrics" not in role_config:
        return None
    mcfg = role_config["metrics"].get(slug)
    if not mcfg or "key" not in mcfg or "direction" not in mcfg:
        return None
    return mcfg


def rate_from_yaml(value: float, metric_cfg: dict) -> str:
    """Compute discrete rating from YAML threshold config (direction-aware)."""
    direction = metric_cfg.get("direction", "higher_is_better")
    if direction == "lower_is_better":
        for level in ["excellent", "good", "neutral"]:
            if level in metric_cfg and value <= metric_cfg[level]:
                return level
        return "bad"
    elif direction == "higher_is_better":
        for level in ["excellent", "good", "neutral"]:
            if level in metric_cfg and value >= metric_cfg[level]:
                return level
        return "bad"
    elif direction == "band":
        for level in ["excellent", "good", "neutral"]:
            bounds = metric_cfg.get(level)
            if bounds and bounds[0] <= value <= bounds[1]:
                return level
        return "bad"
    return "unknown"


def score_from_yaml(value: float, metric_cfg: dict) -> float | None:
    """Continuous 0-100 score from YAML breakpoints."""
    breakpoints = metric_cfg.get("scores")
    if not breakpoints:
        return None
    return _interpolate_breakpoints(value, breakpoints)


def get_continuous_score(metric_slug: str, details: dict[str, Any], role_config: dict | None = None) -> float | None:
    """Compute continuous 0-100 score. Tries YAML role config first, falls back to code thresholds."""
    if is_no_data(details):
        return None
    # Try YAML thresholds
    mcfg = resolve_role_metric_cfg(metric_slug, role_config)
    if mcfg:
        key = mcfg["key"]
        if key in details:
            try:
                return score_from_yaml(float(details[key]), mcfg)
            except (TypeError, ValueError):
                return None
    # Fallback to code thresholds
    key = METRIC_THRESHOLDS.get(metric_slug, {}).get("key")
    if key and key in details:
        try:
            return score_metric(metric_slug, float(details[key]))
        except (TypeError, ValueError):
            return None
    return None
