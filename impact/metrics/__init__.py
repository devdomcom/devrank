# Authored work metrics (engineer-owned PRs/activity)
from impact.metrics.plugins.authored.active_weeks import ActiveWeeks
from impact.metrics.plugins.authored.ai_adoption_rate import AIAdoptionRate
from impact.metrics.plugins.authored.ai_assisted_pr_rate import AIAssistedPRRate
from impact.metrics.plugins.authored.ai_phantom_ownership import AIPhantomOwnership
from impact.metrics.plugins.authored.ai_code_quality import AICodeQuality
from impact.metrics.plugins.authored.ai_suggestion_acceptance import AISuggestionAcceptance
from impact.metrics.plugins.authored.bug_fix_focus_rate import BugFixFocusRate
from impact.metrics.plugins.authored.bus_factor import BusFactor
from impact.metrics.plugins.authored.knowledge_islands import KnowledgeIslands
from impact.metrics.plugins.authored.knowledge_loss import KnowledgeLoss
from impact.metrics.plugins.authored.burstiness import Burstiness
from impact.metrics.plugins.authored.cycle_time import CycleTime
from impact.metrics.plugins.authored.coding_time_to_pr import CodingTimeToPR
from impact.metrics.plugins.authored.coding_days import CodingDays
from impact.metrics.plugins.authored.merge_delay import MergeDelay
from impact.metrics.plugins.authored.module_area_breadth import ModuleAreaBreadth
from impact.metrics.plugins.authored.pr_merge_effectiveness import PRMergeEffectiveness
from impact.metrics.plugins.authored.pr_size_distribution import PRSizeDistribution
from impact.metrics.plugins.authored.delivery_volume import DeliveryVolume
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
from impact.metrics.plugins.authored.dependency_change_rate import DependencyChangeRate
from impact.metrics.plugins.authored.conventional_commit_rate import ConventionalCommitRate
from impact.metrics.plugins.authored.code_churn_rate import CodeChurnRate
from impact.metrics.plugins.authored.code_familiarity import CodeFamiliarity
from impact.metrics.plugins.authored.rework_rate import ReworkRate
from impact.metrics.plugins.authored.self_merge_rate import SelfMergeRate
from impact.metrics.plugins.authored.abandoned_pr_rate import AbandonedPRRate
from impact.metrics.plugins.authored.documentation_touch_rate import DocumentationTouchRate
from impact.metrics.plugins.authored.net_code_contribution import NetCodeContribution
from impact.metrics.plugins.authored.off_hours_activity_rate import OffHoursActivityRate
from impact.metrics.plugins.authored.follow_up_commit_rate import FollowUpCommitRate
from impact.metrics.plugins.authored.hotspot_detection import HotspotDetection
from impact.metrics.plugins.authored.main_developer import MainDeveloperByRevisions, MainDeveloperByLines
from impact.metrics.plugins.authored.contributor_experience import ContributorExperience
from impact.metrics.plugins.authored.entity_ownership import EntityOwnership
from impact.metrics.plugins.authored.temporal_logical_coupling import TemporalLogicalCoupling
from impact.metrics.plugins.authored.pr_category_diversity import PRCategoryDiversity
from impact.metrics.plugins.authored.first_time_approval_rate import FirstTimeApprovalRate
from impact.metrics.plugins.authored.entity_fragmentation import EntityFragmentation
from impact.metrics.plugins.authored.complexity_trend import ComplexityTrend
from impact.metrics.plugins.authored.change_proximity import ChangeProximity
from impact.metrics.plugins.authored.sum_of_coupling import SumOfCoupling
from impact.metrics.plugins.authored.absolute_churn_trend import AbsoluteChurnTrend
from impact.metrics.plugins.authored.commit_message_mining import CommitMessageMining
from impact.metrics.plugins.authored.code_survival import CodeSurvival
from impact.metrics.plugins.authored.review_coverage import ReviewCoverage
from impact.metrics.plugins.authored.wip_load import WIPLoad
from impact.metrics.plugins.authored.flow_efficiency import FlowEfficiency
from impact.metrics.plugins.authored.time_to_restore import TimeToRestore
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
from impact.metrics.plugins.influence.inline_comment_density import InlineCommentDensity
from impact.metrics.plugins.influence.knowledge_sharing_index import KnowledgeSharingIndex

# Mixed metrics (both authored and influence signals)
from impact.metrics.plugins.mixed.co_author_contribution_rate import CoAuthorContributionRate


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
        "ai_adoption_rate": AIAdoptionRate,
        "ai_assisted_pr_rate": AIAssistedPRRate,
        "ai_phantom_ownership": AIPhantomOwnership,
        "ai_code_quality": AICodeQuality,
        "ai_suggestion_acceptance": AISuggestionAcceptance,
        "bus_factor": BusFactor,
        "knowledge_islands": KnowledgeIslands,
        "knowledge_loss": KnowledgeLoss,
        "pr_merge_effectiveness": PRMergeEffectiveness,
        "review_leverage": ReviewLeverage,
        "pr_throughput": PRThroughput,
        "delivery_volume": DeliveryVolume,
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
        "code_familiarity": CodeFamiliarity,
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
        "hotspot_detection": HotspotDetection,
        "main_developer_by_revisions": MainDeveloperByRevisions,
        "main_developer_by_lines": MainDeveloperByLines,
        "temporal_logical_coupling": TemporalLogicalCoupling,
        "entity_fragmentation": EntityFragmentation,
        "contributor_experience": ContributorExperience,
        "entity_ownership": EntityOwnership,
        "complexity_trend": ComplexityTrend,
        "change_proximity": ChangeProximity,
        "sum_of_coupling": SumOfCoupling,
        "absolute_churn_trend": AbsoluteChurnTrend,
        "commit_message_mining": CommitMessageMining,
        "code_survival": CodeSurvival,
        "review_coverage": ReviewCoverage,
        "wip_load": WIPLoad,
        "flow_efficiency": FlowEfficiency,
        "time_to_restore": TimeToRestore,
        "knowledge_sharing_index": KnowledgeSharingIndex,
    }
