# Thresholds for rating metrics (using best judgment for defaults)
# These thresholds determine the qualitative rating (excellent/good/neutral/bad) for each metric.
# Ratings are based on the specified key in the metric's details.

METRIC_THRESHOLDS = {
    "pr_throughput": {
        "key": "merge_ratio",
        "excellent": lambda x: x >= 0.9,
        "good": lambda x: 0.7 <= x < 0.9,
        "neutral": lambda x: 0.5 <= x < 0.7,
        "bad": lambda x: x < 0.5,
    },
    "cycle_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 3,
        "neutral": lambda x: 3 < x <= 7,
        "bad": lambda x: x > 7,
    },
    "pr_merge_effectiveness": {
        "key": "average_back_and_forth",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
    },
    "review_leverage": {
        "key": "effectiveness_percentage",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 30 <= x < 60,
        "bad": lambda x: x < 30,
    },
    "pr_size_distribution": {
        "key": "large_pr_percent",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
    },
    "trivial_contribution_rate": {
        "key": "trivial_prs_per_day",
        "excellent": lambda x: x <= 0.05,  # <=1 trivial PR per 20 days
        "good": lambda x: 0.05 < x <= 0.15,  # 1-3 per 20 days
        "neutral": lambda x: 0.15 < x <= 0.3,  # 3-6 per 20 days
        "bad": lambda x: x > 0.3,  # >6 per 20 days
    },
    "module_area_breadth": {
        "key": "areas_per_pr",
        "excellent": lambda x: x >= 2.0,  # >=2 areas per PR shows good breadth
        "good": lambda x: 1.0 <= x < 2.0,
        "neutral": lambda x: 0.5 <= x < 1.0,
        "bad": lambda x: x < 0.5,
    },
    "review_iterations": {
        "key": "average_iterations",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
    },
    "time_to_first_review": {
        "key": "median_hours",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 6,
        "neutral": lambda x: 6 < x <= 24,
        "bad": lambda x: x > 24,
    },
    "slow_review_response": {
        "key": "median_hours",
        "excellent": lambda x: x <= 2,
        "good": lambda x: 2 < x <= 12,
        "neutral": lambda x: 12 < x <= 48,
        "bad": lambda x: x > 48,
    },
    # Active weeks: lower max gap = fewer absences/disengagement
    "active_weeks": {
        "key": "max_gap_weeks",
        "excellent": lambda x: x <= 1,
        "good": lambda x: 1 < x <= 2,
        "neutral": lambda x: 2 < x <= 4,
        "bad": lambda x: x > 4,
    },
    # Burstiness: lower ratio = steadier/sustainable (vs big bursts)
    "burstiness": {
        "key": "burst_ratio",
        "excellent": lambda x: x <= 1.5,
        "good": lambda x: 1.5 < x <= 2.5,
        "neutral": lambda x: 2.5 < x <= 4.0,
        "bad": lambda x: x > 4.0,
    },
    # Reviews Given: opinionated on collab volume (industry ~2-5/week avg for active)
    "reviews_given": {
        "key": "reviews_per_week",
        "excellent": lambda x: x >= 5.0,
        "good": lambda x: 3.0 <= x < 5.0,
        "neutral": lambda x: 1.0 <= x < 3.0,
        "bad": lambda x: x < 1.0,
    },
    # PR Merge Rate: high % of reviews leading to merge = strong influence
    "pr_merge_rate": {
        "key": "merge_rate",
        "excellent": lambda x: x >= 0.8,
        "good": lambda x: 0.6 <= x < 0.8,
        "neutral": lambda x: 0.4 <= x < 0.6,
        "bad": lambda x: x < 0.4,
    },
    # Change-Inducing Review Rate: high % inducing commits = clear impact
    "change_inducing_review_rate": {
        "key": "inducing_rate",
        "excellent": lambda x: x >= 0.7,
        "good": lambda x: 0.5 <= x < 0.7,
        "neutral": lambda x: 0.3 <= x < 0.5,
        "bad": lambda x: x < 0.3,
    },
    # Approval To Merge Ratio: high = accurate approvals on ready code
    "approval_to_merge_ratio": {
        "key": "ratio",
        "excellent": lambda x: x >= 0.8,
        "good": lambda x: 0.6 <= x < 0.8,
        "neutral": lambda x: 0.4 <= x < 0.6,
        "bad": lambda x: x < 0.4,
    },
    # Review Turnaround: lower time = fast action (industry avg <48h good; period-balanced)
    "review_turnaround_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 12,
        "good": lambda x: 12 < x <= 24,
        "neutral": lambda x: 24 < x <= 48,
        "bad": lambda x: x > 48,
    },
    # Blocking Comment Rate: moderate per day = ownership (balanced by period)
    "blocking_comment_rate": {
        "key": "blocking_per_day",
        "excellent": lambda x: 0.5 <= x <= 2.0,
        "good": lambda x: 0.2 <= x < 0.5 or 2.0 < x <= 3.0,
        "neutral": lambda x: 0.05 <= x < 0.2 or 3.0 < x <= 5.0,
        "bad": lambda x: x < 0.05 or x > 5.0,
    },
    # Unblock Time: lower = fast re-review after blocks (industry <48h)
    "unblock_time": {
        "key": "median_hours",
        "excellent": lambda x: x <= 12,
        "good": lambda x: 12 < x <= 24,
        "neutral": lambda x: 24 < x <= 48,
        "bad": lambda x: x > 48,
    },
    # Bug Fix Focus Rate: moderate % best (industry ~20-40% balanced; high may indicate debt/firefighting, low=feature-heavy)
    "bug_fix_focus_rate": {
        "key": "overall_rate",
        "excellent": lambda x: 20 <= x <= 40,
        "good": lambda x: 10 <= x < 20 or 40 < x <= 60,
        "neutral": lambda x: 5 <= x < 10 or 60 < x <= 75,
        "bad": lambda x: x < 5 or x > 75,
    },
    # Revert Introduction Rate: low % = stability (per proposal; <5% best practice)
    "revert_introduction_rate": {
        "key": "rate",
        "excellent": lambda x: x <= 5,
        "good": lambda x: 5 < x <= 15,
        "neutral": lambda x: 15 < x <= 30,
        "bad": lambda x: x > 30,
    },
    # Test File Ratio: >=25% test changes = good discipline (industry benchmark; 0% flags weak testing)
    "test_file_ratio": {
        "key": "ratio",
        "excellent": lambda x: x >= 25,
        "good": lambda x: 15 <= x < 25,
        "neutral": lambda x: 5 <= x < 15,
        "bad": lambda x: x < 5,
    },
    # PR Body Quality: higher score better (structure/length/refs)
    "pr_body_quality_score": {
        "key": "average_score",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
    },
    # Co-Author Rate: normalized events/week (period-balanced; short periods lenient, long expect density)
    "co_author_contribution_rate": {
        "key": "collab_per_week",
        "excellent": lambda x: x >= 2.0,
        "good": lambda x: 1.0 <= x < 2.0,
        "neutral": lambda x: 0.5 <= x < 1.0,
        "bad": lambda x: x < 0.5,
    },
    # Dep Change Rate: updates/month (neutral if low/0; never BAD by default per role config)
    "dependency_change_rate": {
        "key": "dep_per_month",
        "good": lambda x: x >= 0.5,
        "neutral": lambda x: x < 0.5,
    },
    # Inline Comment Density: avg per PR (higher=depth; neutral low, no dup iterations)
    "inline_comment_density": {
        "key": "avg_inline_per_pr",
        "excellent": lambda x: x >= 5,
        "good": lambda x: 2 <= x < 5,
        "neutral": lambda x: x < 2,
    },
    # Commit Message Clarity: % conventional (industry best practices; higher better)
    "commit_message_clarity": {
        "key": "clarity_rate",
        "excellent": lambda x: x >= 80,
        "good": lambda x: 60 <= x < 80,
        "neutral": lambda x: 40 <= x < 60,
        "bad": lambda x: x < 40,
    },
}
