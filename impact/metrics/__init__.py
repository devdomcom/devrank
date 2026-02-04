from impact.metrics.plugins.pr_merge_effectiveness import PRMergeEffectiveness
from impact.metrics.plugins.review_leverage import ReviewLeverage
from impact.metrics.plugins.pr_throughput import PRThroughput
from impact.metrics.plugins.cycle_time import CycleTime
from impact.metrics.plugins.pr_size_distribution import PRSizeDistribution
from impact.metrics.plugins.trivial_contribution_rate import TrivialContributionRate
from impact.metrics.plugins.module_area_breadth import ModuleAreaBreadth
from impact.metrics.plugins.review_quality import ReviewIterations, TimeToFirstReview, SlowReviewResponse

def get_metrics():
    return {
        'pr_merge_effectiveness': PRMergeEffectiveness,
        'review_leverage': ReviewLeverage,
        'pr_throughput': PRThroughput,
        'cycle_time': CycleTime,
        'pr_size_distribution': PRSizeDistribution,
        'trivial_contribution_rate': TrivialContributionRate,
        'module_area_breadth': ModuleAreaBreadth,
        'review_iterations': ReviewIterations,
        'time_to_first_review': TimeToFirstReview,
        'slow_review_response': SlowReviewResponse,
    }
