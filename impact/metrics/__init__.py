from impact.metrics.plugins.pr_merge_effectiveness import PRMergeEffectiveness
from impact.metrics.plugins.review_leverage import ReviewLeverage
from impact.metrics.plugins.pr_throughput import PRThroughput
from impact.metrics.plugins.cycle_time import CycleTime
from impact.metrics.plugins.review_quality import ReviewIterations, TimeToFirstReview, SlowReviewResponse

def get_metrics():
    return {
        'pr_merge_effectiveness': PRMergeEffectiveness,
        'review_leverage': ReviewLeverage,
        'pr_throughput': PRThroughput,
        'cycle_time': CycleTime,
        'review_iterations': ReviewIterations,
        'time_to_first_review': TimeToFirstReview,
        'slow_review_response': SlowReviewResponse,
    }
