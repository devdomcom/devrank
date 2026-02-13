# Authored work metrics (engineer-owned PRs/activity)
from impact.metrics.plugins.authored.active_weeks import ActiveWeeks
from impact.metrics.plugins.authored.bug_fix_focus_rate import BugFixFocusRate
from impact.metrics.plugins.authored.burstiness import Burstiness
from impact.metrics.plugins.authored.cycle_time import CycleTime
from impact.metrics.plugins.authored.coding_time_to_pr import CodingTimeToPR
from impact.metrics.plugins.authored.coding_days import CodingDays
from impact.metrics.plugins.authored.merge_delay import MergeDelay
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
from impact.metrics.plugins.authored.pr_body_quality import PRBodyQualityScore
from impact.metrics.plugins.authored.co_author_contribution_rate import CoAuthorContributionRate
from impact.metrics.plugins.authored.dependency_change_rate import DependencyChangeRate
from impact.metrics.plugins.authored.inline_comment_density import InlineCommentDensity
from impact.metrics.plugins.authored.commit_message_clarity import ConventionalCommitRate
from impact.metrics.plugins.authored.code_churn_rate import CodeChurnRate
from impact.metrics.plugins.authored.rework_rate import ReworkRate
from impact.metrics.plugins.authored.self_merge_rate import SelfMergeRate
from impact.metrics.plugins.authored.abandoned_pr_rate import AbandonedPRRate
from impact.metrics.plugins.authored.documentation_touch_rate import DocumentationTouchRate
from impact.metrics.plugins.authored.net_code_contribution import NetCodeContribution
from impact.metrics.plugins.authored.off_hours_activity_rate import OffHoursActivityRate
from impact.metrics.plugins.authored.follow_up_commit_rate import FollowUpCommitRate
from impact.metrics.plugins.authored.pr_category_diversity import PRCategoryDiversity
from impact.metrics.plugins.authored.first_time_approval_rate import FirstTimeApprovalRate
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
from impact.metrics.plugins.influence.review_breadth import ReviewBreadth
from impact.metrics.plugins.influence.review_comment_substance import ReviewCommentSubstance
from impact.metrics.plugins.influence.mentorship_signal import MentorshipSignal
from impact.metrics.plugins.influence.review_demand import ReviewDemand
from impact.metrics.plugins.influence.first_reviewer_rate import FirstReviewerRate


def validate_metrics() -> None:
    """Validate all registered metrics have valid category slugs."""
    from impact.config.categories import CATEGORY_SLUGS

    errors = []
    for slug, cls in get_metrics().items():
        m = cls()
        if m.category not in CATEGORY_SLUGS:
            errors.append(f"{slug}: unknown category '{m.category}'")
    if errors:
        raise ValueError(f"Invalid metric categories: {'; '.join(errors)}")


def get_metrics():
    return {
        "pr_merge_effectiveness": PRMergeEffectiveness,
        "review_leverage": ReviewLeverage,
        "pr_throughput": PRThroughput,
        "cycle_time": CycleTime,
        "coding_time_to_pr": CodingTimeToPR,
        "coding_days": CodingDays,
        "merge_delay": MergeDelay,
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
        "pr_body_quality_score": PRBodyQualityScore,
        "co_author_contribution_rate": CoAuthorContributionRate,
        "dependency_change_rate": DependencyChangeRate,
        "inline_comment_density": InlineCommentDensity,
        "conventional_commit_rate": ConventionalCommitRate,
        "code_churn_rate": CodeChurnRate,
        "rework_rate": ReworkRate,
        "self_merge_rate": SelfMergeRate,
        "abandoned_pr_rate": AbandonedPRRate,
        "documentation_touch_rate": DocumentationTouchRate,
        "net_code_contribution": NetCodeContribution,
        "off_hours_activity_rate": OffHoursActivityRate,
        "reviews_given": ReviewsGiven,
        "pr_merge_rate": PRMergeRate,
        "change_inducing_review_rate": ChangeInducingReviewRate,
        "approval_to_merge_ratio": ApprovalToMergeRatio,
        "review_turnaround_time": ReviewTurnaroundTime,
        "blocking_comment_rate": BlockingCommentRate,
        "unblock_time": UnblockTime,
        "follow_up_commit_rate": FollowUpCommitRate,
        "pr_category_diversity": PRCategoryDiversity,
        "first_time_approval_rate": FirstTimeApprovalRate,
        "review_breadth": ReviewBreadth,
        "review_comment_substance": ReviewCommentSubstance,
        "mentorship_signal": MentorshipSignal,
        "review_demand": ReviewDemand,
        "first_reviewer_rate": FirstReviewerRate,
    }
