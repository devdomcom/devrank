# Thresholds for rating metrics (using best judgment for defaults)
# These thresholds determine the qualitative rating (excellent/good/neutral/bad) for each metric.
# Ratings are based on the specified key in the metric's details.

METRIC_THRESHOLDS = {
    'pr_throughput': {
        'key': 'merge_ratio',
        'excellent': lambda x: x >= 0.9,
        'good': lambda x: 0.7 <= x < 0.9,
        'neutral': lambda x: 0.5 <= x < 0.7,
        'bad': lambda x: x < 0.5,
    },
    'cycle_time': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 3,
        'neutral': lambda x: 3 < x <= 7,
        'bad': lambda x: x > 7,
    },
    'pr_merge_effectiveness': {
        'key': 'average_back_and_forth',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 2,
        'neutral': lambda x: 2 < x <= 4,
        'bad': lambda x: x > 4,
    },
    'review_leverage': {
        'key': 'effectiveness_percentage',
        'excellent': lambda x: x >= 80,
        'good': lambda x: 60 <= x < 80,
        'neutral': lambda x: 30 <= x < 60,
        'bad': lambda x: x < 30,
    },
    'pr_size_distribution': {
        'key': 'large_pr_percent',
        'excellent': lambda x: x <= 5,
        'good': lambda x: 5 < x <= 15,
        'neutral': lambda x: 15 < x <= 30,
        'bad': lambda x: x > 30,
    },
    'trivial_contribution_rate': {
        'key': 'trivial_prs_per_day',
        'excellent': lambda x: x <= 0.05,  # <=1 trivial PR per 20 days
        'good': lambda x: 0.05 < x <= 0.15,  # 1-3 per 20 days
        'neutral': lambda x: 0.15 < x <= 0.3,  # 3-6 per 20 days
        'bad': lambda x: x > 0.3,  # >6 per 20 days
    },
    'module_area_breadth': {
        'key': 'areas_per_pr',
        'excellent': lambda x: x >= 2.0,  # >=2 areas per PR shows good breadth
        'good': lambda x: 1.0 <= x < 2.0,
        'neutral': lambda x: 0.5 <= x < 1.0,
        'bad': lambda x: x < 0.5,
    },
    'review_iterations': {
        'key': 'average_iterations',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 2,
        'neutral': lambda x: 2 < x <= 4,
        'bad': lambda x: x > 4,
    },
    'time_to_first_review': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 1,
        'good': lambda x: 1 < x <= 6,
        'neutral': lambda x: 6 < x <= 24,
        'bad': lambda x: x > 24,
    },
    'slow_review_response': {
        'key': 'median_hours',
        'excellent': lambda x: x <= 2,
        'good': lambda x: 2 < x <= 12,
        'neutral': lambda x: 12 < x <= 48,
        'bad': lambda x: x > 48,
    },
}