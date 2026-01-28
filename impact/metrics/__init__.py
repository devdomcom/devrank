from impact.metrics.plugins.pr_merge_effectiveness import PRMergeEffectiveness
from impact.metrics.plugins.review_leverage import ReviewLeverage

def get_metrics():
    return {
        'pr_merge_effectiveness': PRMergeEffectiveness,
        'review_leverage': ReviewLeverage,
    }