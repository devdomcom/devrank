# Authored work metrics (engineer-owned PRs/activity)
from impact.metrics.plugins.authored.active_weeks import ActiveWeeks
from impact.metrics.plugins.authored.bug_fix_focus_rate import BugFixFocusRate
from impact.metrics.plugins.authored.burstiness import Burstiness
from impact.metrics.plugins.authored.cycle_time import CycleTime
from impact.metrics.plugins.authored.module_area_breadth import ModuleAreaBreadth
from impact.metrics.plugins.authored.pr_merge_effectiveness import PRMergeEffectiveness
from impact.metrics.plugins.authored.pr_size_distribution import PRSizeDistribution
from impact.metrics.plugins.authored.pr_throughput import PRThroughput
from impact.metrics.plugins.authored.revert_introduction_rate import RevertIntroductionRate
from impact.metrics.plugins.authored.review_quality import (
    ReviewIterations,
    SlowReviewResponse,
    TimeToFirstReview,
)
from impact.metrics.plugins.authored.test_file_ratio import TestFileRatio
from impact.metrics.plugins.authored.trivial_contribution_rate import TrivialContributionRate
from impact.metrics.plugins.influence.approval_to_merge_ratio import ApprovalToMergeRatio
from impact.metrics.plugins.influence.blocking_comment_rate import BlockingCommentRate
from impact.metrics.plugins.influence.change_inducing_review_rate import ChangeInducingReviewRate
from impact.metrics.plugins.influence.pr_merge_rate import PRMergeRate

# Influence metrics (impact on others' work)
from impact.metrics.plugins.influence.review_leverage import ReviewLeverage
from impact.metrics.plugins.influence.review_turnaround_time import ReviewTurnaroundTime

# Reviews Given for collaboration (DRY count pattern)
from impact.metrics.plugins.influence.reviews_given import ReviewsGiven
from impact.metrics.plugins.influence.unblock_time import UnblockTime


def get_metrics():
    return {
        "pr_merge_effectiveness": PRMergeEffectiveness,
        "review_leverage": ReviewLeverage,
        "pr_throughput": PRThroughput,
        "cycle_time": CycleTime,
        "pr_size_distribution": PRSizeDistribution,
        "trivial_contribution_rate": TrivialContributionRate,
        "module_area_breadth": ModuleAreaBreadth,
        "review_iterations": ReviewIterations,
        "time_to_first_review": TimeToFirstReview,
        "slow_review_response": SlowReviewResponse,
        "active_weeks": ActiveWeeks,
        "burstiness": Burstiness,
        "bug_fix_focus_rate": BugFixFocusRate,
        "revert_introduction_rate": RevertIntroductionRate,
        "test_file_ratio": TestFileRatio,
        "reviews_given": ReviewsGiven,
        "pr_merge_rate": PRMergeRate,
        "change_inducing_review_rate": ChangeInducingReviewRate,
        "approval_to_merge_ratio": ApprovalToMergeRatio,
        "review_turnaround_time": ReviewTurnaroundTime,
        "blocking_comment_rate": BlockingCommentRate,
        "unblock_time": UnblockTime,
    }
