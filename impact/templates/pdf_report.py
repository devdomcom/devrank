"""Modern executive PDF report for engineering impact metrics.

Produces a polished, boardroom-ready document with:
- Executive summary page (overall score, rating distribution, strengths/growth)
- Category sections with color-coded metric cards
- Properly formatted values (percentages, hours, counts, clickable PR links)

All user-facing strings are loaded from a locale YAML file
(``impact/templates/locales/<locale>.yaml``).  Pass ``locale="de"`` (or
``--locale de`` on the CLI) to generate reports in other languages.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from impact.config.categories import UNSCORED_CATEGORIES, compute_group_scores, get_category_name, get_category_order

# ---------------------------------------------------------------------------
# Page size lookup
# ---------------------------------------------------------------------------
PAGE_SIZES: dict[str, tuple[float, float]] = {
    "letter": letter,
    "a4": A4,
}

# ---------------------------------------------------------------------------
# Locale loading
# ---------------------------------------------------------------------------
_LOCALES_DIR = Path(__file__).resolve().parent / "locales"


def _load_locale(locale: str = "en") -> dict[str, str]:
    """Load locale strings from ``locales/<locale>.yaml``.

    Falls back to English if the requested locale file is missing.
    Returns a flat ``{key: string}`` dict.
    """
    path = _LOCALES_DIR / f"{locale}.yaml"
    if not path.exists():
        path = _LOCALES_DIR / "en.yaml"
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return {k: str(v) for k, v in data.items()}
    except (OSError, yaml.YAMLError):
        # Ultimate fallback: return empty dict; callers use English defaults.
        return {}


def _t(strings: dict[str, str], key: str, default: str, **kwargs: object) -> str:
    """Translate helper: look up *key* in *strings*, format with *kwargs*."""
    template = strings.get(key, default)
    try:
        return template.format(**kwargs) if kwargs else template
    except (KeyError, IndexError):
        return default

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
RATING_COLORS: dict[str, colors.Color] = {
    "excellent": colors.HexColor("#16A34A"),
    "good": colors.HexColor("#2563EB"),
    "neutral": colors.HexColor("#6B7280"),
    "bad": colors.HexColor("#DC2626"),
    "descriptive": colors.HexColor("#8B5CF6"),
    "unknown": colors.HexColor("#9CA3AF"),
    "INSUFFICIENT_DATA": colors.HexColor("#D1D5DB"),
}

RATING_BG: dict[str, colors.Color] = {
    "excellent": colors.HexColor("#DCFCE7"),
    "good": colors.HexColor("#DBEAFE"),
    "neutral": colors.HexColor("#F3F4F6"),
    "bad": colors.HexColor("#FEE2E2"),
    "descriptive": colors.HexColor("#EDE9FE"),
    "unknown": colors.HexColor("#F3F4F6"),
    "INSUFFICIENT_DATA": colors.HexColor("#F9FAFB"),
}

PRIMARY = colors.HexColor("#1E3A8A")
DARK_TEXT = colors.HexColor("#1F2937")
MED_TEXT = colors.HexColor("#4B5563")
LIGHT_TEXT = colors.HexColor("#6B7280")
BORDER = colors.HexColor("#E5E7EB")
SECTION_BG = colors.HexColor("#F8FAFC")

# Category ordering is defined centrally in impact.config.categories

# ---------------------------------------------------------------------------
# Detail keys to never display in the PDF
# ---------------------------------------------------------------------------
HIDDEN_KEYS = frozenset({
    "no_data", "no_cr_activity", "no_data_reason",
    "per_pr", "per_review", "per_cr", "per_comment", "per_author",
    "per_pr_hours", "per_pr_co", "per_week", "per_pr_co",
    "analyzed_pr_numbers", "change_request_details", "off_activities",
    "commit_messages_sample", "weekly_rates", "per_author",
    "small_pr_numbers", "medium_pr_numbers", "large_pr_numbers", "trivial_pr_numbers",
    "bug_pr_numbers", "non_bug_pr_numbers", "test_pr_numbers", "non_test_pr_numbers",
    "opened_pr_numbers", "merged_pr_numbers", "review_pr_numbers",
    "revert_shas", "distinct_areas", "distribution",
    "activity_sources", "active_weeks", "inactive_weeks",
    "trivial_pr_numbers", "pr_details", "top_hotspots", "pr_numbers",
    "top_coupled_pairs",
    "entity_shares",
    "file_trends",
    "per_file",
    "per_entity",
    "per_day",
    "defect_commit_samples",
    "per_cohort",
    "phantom_files",
})

# ---------------------------------------------------------------------------
# Metric display config: slug -> {name, category, stats: [(key, label, fmt)]}
# ---------------------------------------------------------------------------
METRIC_DISPLAY_CONFIG: dict[str, dict[str, Any]] = {
    # ---- AI-Assisted Development ----
    "ai_assisted_pr_rate": {
        "name": "AI-Assisted PR Rate",
        "stats": [
            ("ai_rate", "AI Rate", "pct"),
            ("ai_pr_count", "AI PRs", "count"),
            ("human_pr_count", "Human PRs", "count"),
            ("total_pr_count", "Total", "count"),
        ],
    },
    "ai_phantom_ownership": {
        "name": "AI Phantom Ownership",
        "stats": [
            ("phantom_rate", "Phantom Rate", "pct"),
            ("phantom_file_count", "Phantom Files", "count"),
            ("total_ai_files", "AI-Touched Files", "count"),
            ("ai_pr_count", "AI PRs", "count"),
            ("phantom_line_rate", "Phantom Line Rate", "pct"),
        ],
    },
    # ---- Code Quality & Risk ----
    "bus_factor": {
        "name": "Bus Factor",
        "stats": [
            ("bus_factor", "Bus Factor", "count"),
            ("unique_contributors", "Contributors", "count"),
            ("total_files", "Files", "count"),
            ("single_contributor_files_count", "Single-Contrib Files", "count"),
        ],
    },
    "knowledge_loss": {
        "name": "Knowledge Loss",
        "stats": [
            ("loss_count", "At-Risk Files", "count"),
            ("overall_loss_pct", "Overall Loss", "pct"),
            ("total_files", "Files Analyzed", "count"),
            ("active_contributors_count", "Active Contributors", "count"),
        ],
    },
    "knowledge_sharing_index": {
        "name": "Knowledge Sharing Index",
        "stats": [
            ("sharing_index", "Sharing Index", "ratio"),
            ("reviewer_count", "Reviewers", "count"),
            ("total_reviews", "Total Reviews", "count"),
            ("entropy", "Entropy", "ratio"),
        ],
    },
    # ---- Productivity & Throughput ----
    "pr_throughput": {
        "name": "PR Throughput",
        "stats": [
            ("merge_ratio", "Merge Rate", "pct"),
            ("opened_count", "Opened", "count"),
            ("merged_count", "Merged", "count"),
        ],
    },
    "cycle_time": {
        "name": "Cycle Time",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("merged_count", "PRs Merged", "count"),
        ],
    },
    "coding_time_to_pr": {
        "name": "Coding Time to PR",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "merge_delay": {
        "name": "Merge Delay",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("merged_count", "PRs", "count"),
        ],
    },
    "coding_days": {
        "name": "Coding Days",
        "stats": [
            ("ratio_pct", "Activity Ratio", "pct"),
            ("commit_count", "Commits", "count"),
        ],
    },
    "active_weeks": {
        "name": "Active Weeks",
        "stats": [
            ("active_count", "Active", "count"),
            ("total_weeks", "Total", "count"),
            ("active_ratio", "Ratio", "pct"),
            ("max_gap_weeks", "Max Gap", "count"),
        ],
    },
    "burstiness": {
        "name": "Burstiness",
        "stats": [
            ("burst_ratio", "Burst Ratio", "ratio"),
            ("active_weeks", "Active Weeks", "count"),
            ("total_weeks", "Total Weeks", "count"),
        ],
    },
    # ---- Code Quality & Size ----
    "pr_size_distribution": {
        "name": "PR Size Distribution",
        "stats": [
            ("small_pr_percent", "Small PRs", "pct"),
            ("medium_pr_percent", "Medium PRs", "pct"),
            ("large_pr_percent", "Large PRs", "pct"),
        ],
    },
    "test_file_ratio": {
        "name": "Test File Ratio",
        "stats": [
            ("ratio", "Test Ratio", "pct"),
            ("test_prs", "Test PRs", "count"),
            ("total_prs", "Total PRs", "count"),
        ],
    },
    "inline_comment_density": {
        "name": "Inline Comment Density",
        "stats": [
            ("avg_inline_per_pr", "Avg per PR", "ratio"),
            ("reviewed_pr_count", "PRs Reviewed", "count"),
            ("total_inline_comments", "Total Comments", "count"),
        ],
    },
    "conventional_commit_rate": {
        "name": "Conventional Commit Rate",
        "stats": [
            ("conventional_commit_rate", "Rate", "pct"),
            ("conventional_count", "Conventional", "count"),
            ("total_commits", "Total", "count"),
        ],
    },
    "pr_body_quality_score": {
        "name": "PR Body Quality",
        "stats": [
            ("average_score", "Avg Score", "score"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "code_churn_rate": {
        "name": "Code Churn Rate",
        "stats": [
            ("churn_rate", "Churn Rate", "pct"),
            ("churned_lines", "Churned Lines", "count"),
            ("total_lines", "Total Lines", "count"),
            ("pr_count", "PRs", "count"),
        ],
    },
    # ---- PR Hygiene & Process ----
    "pr_merge_effectiveness": {
        "name": "PR Merge Effectiveness",
        "stats": [
            ("average_merge_time_hours", "Avg Merge Time", "hours"),
            ("average_back_and_forth", "Avg Iterations", "ratio"),
            ("merged_pr_count", "Merged", "count"),
        ],
    },
    "review_iterations": {
        "name": "Review Iterations",
        "stats": [
            ("average_iterations", "Avg Iterations", "ratio"),
            ("merged_prs", "PRs", "count"),
        ],
    },
    "time_to_first_review": {
        "name": "Time to First Review",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("reviewed_prs", "PRs", "count"),
        ],
    },
    "slow_review_response": {
        "name": "Slow Review Response",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("samples", "Reviews", "count"),
        ],
    },
    "first_time_approval_rate": {
        "name": "First-Time Approval Rate",
        "stats": [
            ("rate", "Rate", "pct"),
            ("immediate_count", "First-Time", "count"),
            ("merged_pr_count", "Total", "count"),
        ],
    },
    "follow_up_commit_rate": {
        "name": "Follow-Up Commit Rate",
        "stats": [
            ("follow_up_rate", "Rate", "pct"),
            ("follow_up_count", "Follow-ups", "count"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "trivial_contribution_rate": {
        "name": "Trivial Contribution Rate",
        "stats": [
            ("trivial_rate", "Rate", "pct"),
            ("trivial_pr_count", "Trivial", "count"),
            ("total_pr_count", "Total", "count"),
        ],
    },
    "self_merge_rate": {
        "name": "Self-Merge Rate",
        "stats": [
            ("self_merge_rate", "Rate", "pct"),
            ("no_approval_count", "No Approval", "count"),
            ("engineer_merged_count", "Total Merged", "count"),
        ],
    },
    "abandoned_pr_rate": {
        "name": "Abandoned PR Rate",
        "stats": [
            ("abandoned_rate", "Rate", "pct"),
            ("stale_count", "Stale", "count"),
            ("open_pr_count", "Open PRs", "count"),
        ],
    },
    # ---- Scope & Collaboration ----
    "module_area_breadth": {
        "name": "Module Area Breadth",
        "stats": [
            ("distinct_areas_count", "Areas", "count"),
            ("areas_per_pr", "Areas per PR", "ratio"),
            ("total_files_touched", "Files Touched", "count"),
        ],
    },
    "co_author_contribution_rate": {
        "name": "Co-Author Contribution",
        "stats": [
            ("inbound_rate", "Inbound Rate", "pct"),
            ("outbound_rate", "Outbound Rate", "pct"),
            ("total_co_events", "Co-author Events", "count"),
        ],
    },
    "pr_category_diversity": {
        "name": "PR Category Diversity",
        "stats": [
            ("distinct_categories", "Categories", "count"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "dependency_change_rate": {
        "name": "Dependency Change Rate",
        "stats": [
            ("dep_rate", "Rate", "pct"),
            ("dep_pr_count", "Dep PRs", "count"),
            ("total_prs", "Total PRs", "count"),
        ],
    },
    "documentation_touch_rate": {
        "name": "Documentation Touch Rate",
        "stats": [
            ("doc_rate", "Rate", "pct"),
            ("doc_pr_count", "Doc PRs", "count"),
            ("total_prs", "Total PRs", "count"),
        ],
    },
    # ---- Risk & Sustainability ----
    "bug_fix_focus_rate": {
        "name": "Bug Fix Focus Rate",
        "stats": [
            ("overall_rate", "Rate", "pct"),
            ("total_bug", "Bug Fixes", "count"),
            ("total_items", "Total", "count"),
        ],
    },
    "revert_introduction_rate": {
        "name": "Revert Rate",
        "stats": [
            ("rate", "Rate", "pct"),
            ("revert_count", "Reverts", "count"),
            ("total_commits", "Total Commits", "count"),
        ],
    },
    "off_hours_activity_rate": {
        "name": "Off-Hours Activity",
        "stats": [
            ("off_hours_rate", "Rate", "pct"),
            ("off_count", "Off-Hours", "count"),
            ("total_activities", "Total", "count"),
        ],
    },
    # ---- Influence & Review ----
    "reviews_given": {
        "name": "Reviews Given",
        "stats": [
            ("review_count", "Reviews", "count"),
            ("reviews_per_week", "Per Week", "ratio"),
            ("period_days", "Period (days)", "count"),
        ],
    },
    "review_turnaround_time": {
        "name": "Review Turnaround",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("reviewed_prs", "PRs", "count"),
        ],
    },
    "review_leverage": {
        "name": "Review Leverage",
        "stats": [
            ("effectiveness_rate", "Effectiveness", "pct"),
            ("total_reviews", "Reviews", "count"),
            ("effective_changes", "Changes Induced", "count"),
        ],
    },
    "pr_merge_rate": {
        "name": "PR Merge Rate",
        "stats": [
            ("merge_rate", "Rate", "pct"),
            ("effective_merges", "Merges", "count"),
            ("total_reviews", "Reviews", "count"),
        ],
    },
    "change_inducing_review_rate": {
        "name": "Change-Inducing Review",
        "stats": [
            ("inducing_rate", "Rate", "pct"),
            ("inducing_count", "Inducing", "count"),
            ("total_reviews", "Total", "count"),
        ],
    },
    "approval_to_merge_ratio": {
        "name": "Approval to Merge",
        "stats": [
            ("ratio", "Ratio", "ratio"),
            ("final_approvals", "Final Approvals", "count"),
            ("total_approvals", "Total", "count"),
        ],
    },
    "blocking_comment_rate": {
        "name": "Blocking Comment Rate",
        "stats": [
            ("blocking_rate", "Rate", "pct"),
            ("blocking_count", "Blocking", "count"),
            ("total_reviews", "Total", "count"),
        ],
    },
    "unblock_time": {
        "name": "Unblock Time",
        "stats": [
            ("median_hours", "Median", "hours"),
            ("p75_hours", "P75", "hours"),
            ("cr_count", "Change Requests", "count"),
        ],
    },
    "review_breadth": {
        "name": "Review Breadth",
        "stats": [
            ("unique_authors", "Authors", "count"),
            ("total_prs_reviewed", "PRs Reviewed", "count"),
        ],
    },
    "review_comment_substance": {
        "name": "Review Substance",
        "stats": [
            ("avg_substance_score", "Avg Score", "score"),
            ("total_comments", "Comments", "count"),
            ("reviewed_pr_count", "PRs", "count"),
        ],
    },
    "mentorship_signal": {
        "name": "Mentorship Signal",
        "stats": [
            ("mentorship_rate", "Rate", "pct"),
            ("junior_review_count", "Junior Reviews", "count"),
            ("total_reviewed_prs", "Total", "count"),
        ],
    },
    "review_demand": {
        "name": "Review Demand",
        "stats": [
            ("demand_per_week", "Per Week", "ratio"),
            ("demand_count", "Requests", "count"),
            ("affected_prs", "PRs", "count"),
        ],
    },
    "first_reviewer_rate": {
        "name": "First Reviewer Rate",
        "stats": [
            ("rate", "Rate", "pct"),
            ("first_count", "First Reviews", "count"),
            ("total_reviews", "Total", "count"),
        ],
    },
    # ---- Descriptive ----
    "net_code_contribution": {
        "name": "Net Code Contribution",
        "stats": [
            ("net_lines", "Net Lines", "count"),
            ("add_to_del_ratio", "Add/Del Ratio", "ratio"),
            ("total_additions", "Additions", "count"),
            ("total_deletions", "Deletions", "count"),
        ],
    },
    "rework_rate": {
        "name": "Rework Rate",
        "stats": [
            ("rework_rate", "Rate", "pct"),
            ("reworked_lines", "Reworked Lines", "count"),
            ("total_changed", "Total Changed", "count"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "hotspot_detection": {
        "name": "Hotspot Detection",
        "stats": [
            ("max_hotspot_score", "Max Score", "ratio"),
            ("hotspot_count", "Hotspots", "count"),
        ],
    },
    "temporal_logical_coupling": {
        "name": "Temporal / Logical Coupling",
        "stats": [
            ("max_coupling_ratio", "Max Ratio", "pct"),
            ("pair_count", "Pairs", "count"),
        ],
    },
    "entity_fragmentation": {
        "name": "Entity Fragmentation",
        "stats": [
            ("fragmentation_index", "Fragmentation", "ratio"),
            ("entity_count", "Entities", "count"),
            ("total_changes", "Changes", "count"),
        ],
    },
    "complexity_trend": {
        "name": "Complexity Trend",
        "stats": [
            ("max_std_dev", "Max Std Dev", "ratio"),
            ("avg_std_dev", "Avg Std Dev", "ratio"),
            ("file_count", "Files", "count"),
            ("sample_count", "Samples", "count"),
        ],
    },
    "change_proximity": {
        "name": "Change Proximity",
        "stats": [
            ("avg_proximity_per_change", "Avg Distance", "ratio"),
            ("total_proximity", "Total Distance", "count"),
            ("total_changes", "Changes", "count"),
            ("total_files", "Files", "count"),
        ],
    },
    "sum_of_coupling": {
        "name": "Sum of Coupling",
        "stats": [
            ("max_coupling_score", "Max SoC", "count"),
            ("total_coupling", "Total SoC", "count"),
            ("total_entities", "Entities", "count"),
            ("pr_count", "PRs", "count"),
        ],
    },
    "absolute_churn_trend": {
        "name": "Absolute Churn Trend",
        "stats": [
            ("max_daily_churn", "Max Daily Churn", "count"),
            ("total_churn", "Total Churn", "count"),
            ("total_additions", "Additions", "count"),
            ("total_deletions", "Deletions", "count"),
        ],
    },
    "commit_message_mining": {
        "name": "Commit Message Mining",
        "stats": [
            ("defect_commit_rate", "Defect Rate", "pct"),
            ("defect_commit_count", "Defect Commits", "count"),
            ("total_commits", "Total Commits", "count"),
        ],
    },
    "code_survival": {
        "name": "Code Survival",
        "stats": [
            ("survival_rate", "Survival Rate", "pct"),
            ("total_survived", "Survived Lines", "count"),
            ("total_contributed", "Total Lines", "count"),
            ("total_churned", "Churned Lines", "count"),
        ],
    },
    # ---- Review Coverage ----
    "review_coverage": {
        "name": "Review Coverage",
        "stats": [
            ("coverage_pct", "Coverage", "pct"),
            ("reviewed_files", "Reviewed Files", "count"),
            ("total_files", "Total Files", "count"),
            ("fully_reviewed_prs", "Fully Reviewed PRs", "count"),
            ("unreviewed_prs", "Unreviewed PRs", "count"),
        ],
    },
}


# ---------------------------------------------------------------------------
# Value formatters
# ---------------------------------------------------------------------------

def _fmt_pct(v: Any) -> str:
    """Format as percentage: 0.87 -> '87.0%', 87.3 -> '87.3%'."""
    if v is None:
        return "N/A"
    f = float(v)
    if f <= 1.0 and f >= 0.0:
        f *= 100
    return f"{f:.1f}%"


def _fmt_hours(v: Any) -> str:
    if v is None:
        return "N/A"
    return f"{float(v):.1f}h"


def _fmt_days(v: Any) -> str:
    if v is None:
        return "N/A"
    f = float(v)
    if f > 24:
        return f"{f / 24:.1f}d"
    return f"{f:.0f} days"


def _fmt_ratio(v: Any) -> str:
    if v is None:
        return "N/A"
    return f"{float(v):.2f}"


def _fmt_count(v: Any) -> str:
    if v is None:
        return "N/A"
    n = int(round(float(v)))
    return f"{n:,}"


def _fmt_score(v: Any) -> str:
    if v is None:
        return "N/A"
    return f"{float(v):.1f}/100"


FORMATTERS = {
    "pct": _fmt_pct,
    "hours": _fmt_hours,
    "days": _fmt_days,
    "ratio": _fmt_ratio,
    "count": _fmt_count,
    "score": _fmt_score,
}


def _fmt_value(v: Any, fmt: str) -> str:
    return FORMATTERS.get(fmt, str)(v)


def _rating_label(rating: str, short: bool = False, strings: dict[str, str] | None = None) -> str:
    """Human-readable rating label.  Uses locale *strings* when provided."""
    s = strings or {}
    suffix = "_short" if short else ""
    mapping = {
        "excellent": _t(s, f"rating_excellent{suffix}", "Excellent" if not short else "Excellent"),
        "good": _t(s, f"rating_good{suffix}", "Good"),
        "neutral": _t(s, f"rating_neutral{suffix}", "Neutral"),
        "bad": _t(s, f"rating_bad{suffix}", "Needs Work"),
        "descriptive": _t(s, f"rating_descriptive{suffix}", "Informational" if not short else "Info"),
        "unknown": _t(s, f"rating_unknown{suffix}", "Unknown"),
        "INSUFFICIENT_DATA": _t(s, f"rating_insufficient_data{suffix}", "Insufficient Data" if not short else "No Data"),
    }
    return mapping.get(rating, rating)


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

def _build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "RptTitle", parent=base["Title"],
            fontName="Helvetica-Bold", fontSize=28, textColor=PRIMARY,
            alignment=TA_CENTER, spaceAfter=6, leading=34,
        ),
        "subtitle": ParagraphStyle(
            "RptSubtitle", parent=base["Normal"],
            fontName="Helvetica", fontSize=14, textColor=LIGHT_TEXT,
            alignment=TA_CENTER, spaceBefore=4, spaceAfter=28, leading=18,
        ),
        "section": ParagraphStyle(
            "RptSection", parent=base["Heading2"],
            fontName="Helvetica-Bold", fontSize=16, textColor=PRIMARY,
            spaceBefore=28, spaceAfter=12, leading=20,
        ),
        "metric_name": ParagraphStyle(
            "RptMetricName", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=11, textColor=DARK_TEXT,
            spaceAfter=4, leading=14,
        ),
        "metric_desc": ParagraphStyle(
            "RptMetricDesc", parent=base["Normal"],
            fontName="Helvetica-Oblique", fontSize=8, textColor=LIGHT_TEXT,
            spaceAfter=3, leading=10,
        ),
        "metric_summary": ParagraphStyle(
            "RptMetricSummary", parent=base["Normal"],
            fontName="Helvetica", fontSize=9, textColor=MED_TEXT,
            spaceAfter=6, leading=12,
        ),
        "stat_label": ParagraphStyle(
            "RptStatLabel", parent=base["Normal"],
            fontName="Helvetica", fontSize=8, textColor=LIGHT_TEXT,
            leading=11, spaceAfter=2,
        ),
        "stat_value": ParagraphStyle(
            "RptStatValue", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=10, textColor=DARK_TEXT,
            leading=13,
        ),
        "badge": ParagraphStyle(
            "RptBadge", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=9, alignment=TA_CENTER,
        ),
        "score_big": ParagraphStyle(
            "RptScoreBig", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=42, textColor=PRIMARY,
            alignment=TA_CENTER, leading=50, spaceAfter=10,
        ),
        "score_label": ParagraphStyle(
            "RptScoreLabel", parent=base["Normal"],
            fontName="Helvetica", fontSize=11, textColor=LIGHT_TEXT,
            alignment=TA_CENTER, spaceBefore=6, spaceAfter=20,
        ),
        "strength_name": ParagraphStyle(
            "RptStrengthName", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=10, textColor=DARK_TEXT,
            leading=14, spaceAfter=2,
        ),
        "strength_detail": ParagraphStyle(
            "RptStrengthDetail", parent=base["Normal"],
            fontName="Helvetica", fontSize=9, textColor=MED_TEXT,
            leading=12,
        ),
        "footer": ParagraphStyle(
            "RptFooter", parent=base["Normal"],
            fontName="Helvetica", fontSize=8, textColor=LIGHT_TEXT,
            alignment=TA_CENTER,
        ),
        "insuf_line": ParagraphStyle(
            "RptInsufLine", parent=base["Normal"],
            fontName="Helvetica", fontSize=9, textColor=colors.HexColor("#9CA3AF"),
            leading=14, spaceBefore=2, spaceAfter=2,
        ),
        "fallback_key": ParagraphStyle(
            "RptFallbackKey", parent=base["Normal"],
            fontName="Helvetica", fontSize=8, textColor=LIGHT_TEXT,
            leading=11,
        ),
        "fallback_val": ParagraphStyle(
            "RptFallbackVal", parent=base["Normal"],
            fontName="Helvetica", fontSize=9, textColor=MED_TEXT,
            leading=12,
        ),
    }


# ---------------------------------------------------------------------------
# Helper: build a rating badge
# ---------------------------------------------------------------------------

def _rating_badge(rating: str, styles: dict) -> Table:
    """Small colored pill with the rating text."""
    fg = RATING_COLORS.get(rating, RATING_COLORS["unknown"])
    bg = RATING_BG.get(rating, RATING_BG["unknown"])
    label = _rating_label(rating)
    p = Paragraph(
        f'<font color="{fg.hexval()}">{label}</font>',
        styles["badge"],
    )
    t = Table([[p]], colWidths=[1.25 * inch], rowHeights=[0.28 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


# ---------------------------------------------------------------------------
# Helper: format a stat row
# ---------------------------------------------------------------------------

def _stat_cells(details: dict, stat_defs: list[tuple], styles: dict) -> list:
    """Build a row of (label, value) stat pairs."""
    cells = []
    for key, label, fmt in stat_defs:
        val = details.get(key)
        if val is None:
            continue
        lbl_p = Paragraph(label, styles["stat_label"])
        val_p = Paragraph(_fmt_value(val, fmt), styles["stat_value"])
        cells.append([lbl_p, val_p])
    return cells


# ---------------------------------------------------------------------------
# Helper: _humanize_key turns snake_case into Title Case
# ---------------------------------------------------------------------------

def _humanize_key(key: str) -> str:
    return key.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Build a metric card
# ---------------------------------------------------------------------------

_SIGNAL_TAG_COLORS: dict[str, tuple[str, str]] = {
    "authored": ("#1D4ED8", "#DBEAFE"),    # blue
    "influence": ("#047857", "#D1FAE5"),   # green
    "mixed": ("#B45309", "#FEF3C7"),       # amber
}

# Framework badge colors (foreground, background)
FRAMEWORK_COLORS: dict[str, tuple[str, str]] = {
    "DORA": ("#7C3AED", "#EDE9FE"),        # purple
    "SPACE": ("#0891B2", "#CFFAFE"),       # cyan
    "CodeScene": ("#C2410C", "#FFEDD5"),   # orange
    "Lean": ("#059669", "#D1FAE5"),        # emerald
    "Traditional": ("#4B5563", "#E5E7EB"), # gray
    "Network": ("#DB2777", "#FCE7F3"),     # pink
    "DevRank": ("#2563EB", "#DBEAFE"),     # blue
}


def _signal_tag(signal_type: str) -> str:
    """Inline XML fragment for a small signal-type label."""
    fg, _bg = _SIGNAL_TAG_COLORS.get(signal_type, ("#6B7280", "#F3F4F6"))
    label = signal_type.capitalize()
    return f'  <font color="{fg}" size="8">[{label}]</font>'


def _framework_tags(frameworks: list[str]) -> str:
    """Inline XML fragment for small framework labels."""
    if not frameworks:
        return ""
    tags = []
    for fw in frameworks[:3]:  # Limit to first 3 frameworks
        fg, _bg = FRAMEWORK_COLORS.get(fw, ("#6B7280", "#F3F4F6"))
        tags.append(f'<font color="{fg}" size="7">{fw}</font>')
    return '  ' + ' • '.join(tags)


def _build_metric_card(
    metric: dict,
    styles: dict,
    page_width: float,
) -> list:
    """Build flowable elements for a single metric card."""
    slug = metric["slug"]
    config = METRIC_DISPLAY_CONFIG.get(slug)
    display_name = config["name"] if config else _humanize_key(slug)
    rating = metric.get("rating", "unknown")
    score = metric.get("score")
    details = metric.get("details", {})
    summary = metric.get("summary", "")
    description = metric.get("description", "")
    signal_type = metric.get("signal_type", "authored")
    frameworks = metric.get("frameworks", [])

    # Truncate summary
    if len(summary) > 140:
        summary = summary[:137] + "..."

    # Score text
    score_text = f"  ({score:.0f}/100)" if score is not None else ""

    # Badge
    badge = _rating_badge(rating, styles)

    # Name + score + signal tag + framework tags line
    tag = _signal_tag(signal_type)
    fw_tags = _framework_tags(frameworks)
    name_p = Paragraph(
        f'{display_name}<font color="{LIGHT_TEXT.hexval()}" size="9">{score_text}</font>{tag}{fw_tags}',
        styles["metric_name"],
    )

    # Description line (purpose of the metric)
    desc_p = Paragraph(
        f'<i><font color="{LIGHT_TEXT.hexval()}">{description}</font></i>',
        styles["metric_desc"],
    ) if description else None

    # Summary line
    summary_p = Paragraph(summary, styles["metric_summary"])

    # Stats row
    stat_defs = config["stats"] if config else []
    stat_pairs = _stat_cells(details, stat_defs, styles)

    # Build stats table (horizontal row of label/value pairs)
    stat_elements = []
    if stat_pairs:
        n_stats = len(stat_pairs)
        stat_col_w = min(1.5 * inch, (page_width - 1.8 * inch) / max(n_stats, 1))
        # Flatten: each cell is a mini-table of [label, value]
        stat_row_top = []
        stat_row_bot = []
        for pair in stat_pairs:
            stat_row_top.append(pair[0])
            stat_row_bot.append(pair[1])
        stats_t = Table(
            [stat_row_top, stat_row_bot],
            colWidths=[stat_col_w] * n_stats,
        )
        stats_t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("TOPPADDING", (0, 0), (-1, -1), 1),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        stat_elements.append(stats_t)

    # Fallback: show non-hidden detail keys not covered by stats
    shown_keys = {s[0] for s in stat_defs} if stat_defs else set()
    fallback_items = []
    if not config:
        # No display config: show all non-hidden keys
        for k, v in details.items():
            if k in HIDDEN_KEYS or isinstance(v, (list, dict)):
                continue
            fallback_items.append((k, v))
    else:
        # Check for important keys not in stats
        for k, v in details.items():
            if k in HIDDEN_KEYS or k in shown_keys or isinstance(v, (list, dict)):
                continue
            # Skip keys that are clearly internal
            if k.startswith("_"):
                continue

    if fallback_items:
        for k, v in fallback_items[:4]:
            formatted = _auto_format(v)
            fb = Paragraph(
                f'<font color="{LIGHT_TEXT.hexval()}">{_humanize_key(k)}:</font> {formatted}',
                styles["fallback_val"],
            )
            stat_elements.append(fb)

    # Combine into card: [badge | content]
    content_parts = [name_p]
    if desc_p:
        content_parts.append(desc_p)
    content_parts.append(summary_p)
    content_parts.extend(stat_elements)
    # Build an inner table for content
    content_rows = [[e] for e in content_parts]
    inner = Table(content_rows, colWidths=[page_width - 1.8 * inch])
    inner.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    card = Table(
        [[badge, inner]],
        colWidths=[1.5 * inch, page_width - 1.8 * inch],
    )
    card.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("LEFTPADDING", (0, 0), (0, -1), 10),
        ("RIGHTPADDING", (-1, 0), (-1, -1), 10),
        ("LINEBELOW", (0, 0), (-1, -1), 0.5, BORDER),
    ]))
    return [card]


def _auto_format(v: Any, strings: dict[str, str] | None = None) -> str:
    """Best-effort formatting for unknown values."""
    if isinstance(v, float):
        if 0 <= v <= 1:
            return f"{v * 100:.1f}%"
        return f"{v:.1f}"
    if isinstance(v, bool):
        s = strings or {}
        return _t(s, "bool_yes", "Yes") if v else _t(s, "bool_no", "No")
    return str(v)


# ---------------------------------------------------------------------------
# Group-averaged scoring (delegates to shared utility)
# ---------------------------------------------------------------------------

def _compute_group_scores(
    metrics: list[dict],
    category_weights: dict[str, float] | None = None,
) -> tuple[float | None, dict[str, float]]:
    """Thin wrapper: extract (score, category) pairs and delegate."""
    pairs = [
        (m["score"], m.get("category", "contextual"))
        for m in metrics
        if m.get("score") is not None
    ]
    return compute_group_scores(pairs, category_weights)


# ---------------------------------------------------------------------------
# Executive summary page
# ---------------------------------------------------------------------------

def _build_executive_summary(
    metrics: list[dict],
    user_login: str,
    period_str: str,
    styles: dict,
    page_width: float,
    strings: dict[str, str] | None = None,
) -> list:
    """Build the first-page executive summary."""
    s = strings or {}
    elements: list = []

    # Title
    elements.append(Paragraph(_t(s, "report_title", "Engineering Impact Report"), styles["title"]))
    elements.append(Paragraph(
        f'{user_login}  |  {period_str}',
        styles["subtitle"],
    ))
    elements.append(Spacer(1, 0.3 * inch))

    # Compute group-averaged overall score
    overall_score, group_scores = _compute_group_scores(metrics)
    if overall_score is not None:
        score_color = (
            RATING_COLORS["excellent"] if overall_score >= 75
            else RATING_COLORS["good"] if overall_score >= 50
            else RATING_COLORS["neutral"] if overall_score >= 25
            else RATING_COLORS["bad"]
        )
        elements.append(Paragraph(
            f'<font color="{score_color.hexval()}">{overall_score:.0f}</font>',
            styles["score_big"],
        ))
        elements.append(Paragraph(
            _t(s, "overall_score", "Overall Score ({count} groups)", count=len(group_scores)),
            styles["score_label"],
        ))

    # Group score breakdown
    if group_scores:
        cat_order = get_category_order()
        gs_rows = []
        for cat in cat_order:
            if cat in UNSCORED_CATEGORIES or cat not in group_scores:
                continue
            gs = group_scores[cat]
            gc = (
                RATING_COLORS["excellent"] if gs >= 75
                else RATING_COLORS["good"] if gs >= 50
                else RATING_COLORS["neutral"] if gs >= 25
                else RATING_COLORS["bad"]
            )
            gs_rows.append([
                Paragraph(get_category_name(cat), styles["stat_label"]),
                Paragraph(
                    f'<font color="{gc.hexval()}"><b>{gs:.0f}</b></font>/100',
                    styles["stat_value"],
                ),
            ])
        if gs_rows:
            n_cols = min(3, len(gs_rows))
            # Arrange in rows of 3
            padded = gs_rows + [["", ""]] * (n_cols - len(gs_rows) % n_cols) if len(gs_rows) % n_cols else gs_rows
            flat_rows = []
            for i in range(0, len(padded), n_cols):
                name_row = []
                score_row = []
                for j in range(n_cols):
                    if i + j < len(gs_rows):
                        name_row.append(gs_rows[i + j][0])
                        score_row.append(gs_rows[i + j][1])
                    else:
                        name_row.append("")
                        score_row.append("")
                flat_rows.append(name_row)
                flat_rows.append(score_row)
            col_w = (page_width - 0.5 * inch) / n_cols
            gs_table = Table(flat_rows, colWidths=[col_w] * n_cols)
            gs_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]))
            elements.append(Spacer(1, 0.15 * inch))
            elements.append(gs_table)

    elements.append(Spacer(1, 0.25 * inch))

    # Rating distribution
    dist: dict[str, int] = {}
    for m in metrics:
        r = m.get("rating", "unknown")
        dist[r] = dist.get(r, 0) + 1

    total = len(metrics) or 1
    bar_order = ["excellent", "good", "neutral", "bad", "INSUFFICIENT_DATA", "descriptive", "unknown"]
    bar_cells = []
    bar_widths = []
    bar_colors_list = []
    for r in bar_order:
        cnt = dist.get(r, 0)
        if cnt == 0:
            continue
        w = max(0.6 * inch, (page_width - 0.5 * inch) * cnt / total)
        bar_widths.append(w)
        bar_cells.append(Paragraph(
            f'<font color="white" size="8"><b>{cnt}</b></font>',
            ParagraphStyle("BarCell", alignment=TA_CENTER, fontSize=8),
        ))
        bar_colors_list.append(RATING_COLORS.get(r, RATING_COLORS["unknown"]))

    if bar_cells:
        bar_table = Table([bar_cells], colWidths=bar_widths, rowHeights=[0.32 * inch])
        bar_style = [
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]
        for i, bg in enumerate(bar_colors_list):
            bar_style.append(("BACKGROUND", (i, 0), (i, 0), bg))
        bar_table.setStyle(TableStyle(bar_style))
        elements.append(bar_table)

        # Legend row (use short labels to prevent truncation)
        legend_cells = []
        for r in bar_order:
            cnt = dist.get(r, 0)
            if cnt == 0:
                continue
            color = RATING_COLORS.get(r, RATING_COLORS["unknown"])
            legend_cells.append(Paragraph(
                f'<font color="{color.hexval()}" size="7">{_rating_label(r, short=True)} ({cnt})</font>',
                ParagraphStyle("Legend", alignment=TA_CENTER, fontSize=7),
            ))
        legend_t = Table([legend_cells], colWidths=bar_widths, rowHeights=[0.24 * inch])
        legend_t.setStyle(TableStyle([
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        elements.append(legend_t)

    elements.append(Spacer(1, 0.4 * inch))

    # Strengths and growth areas side by side
    strengths = sorted(
        [m for m in metrics if m.get("rating") == "excellent" and m.get("score") is not None],
        key=lambda m: m["score"],
        reverse=True,
    )[:5]
    growth = sorted(
        [m for m in metrics if m.get("rating") == "bad" and m.get("score") is not None],
        key=lambda m: m["score"],
    )[:5]

    def _highlight_list(title: str, items: list[dict], color: colors.Color) -> list:
        """Build a mini-list of metric highlights."""
        rows = []
        title_p = Paragraph(
            f'<font color="{color.hexval()}"><b>{title}</b></font>',
            ParagraphStyle("HL_Title", fontSize=12, spaceAfter=10, leading=16),
        )
        rows.append([title_p])
        if not items:
            none_text = _t(s, "none_in_period", "None in this period")
            rows.append([Paragraph(
                f'<font color="#9CA3AF">{none_text}</font>',
                styles["strength_detail"],
            )])
        for m in items:
            cfg = METRIC_DISPLAY_CONFIG.get(m["slug"])
            name = cfg["name"] if cfg else _humanize_key(m["slug"])
            score_val = m.get("score")
            score_s = f" ({score_val:.0f}/100)" if score_val is not None else ""
            rows.append([Paragraph(
                f'{name}<font color="{LIGHT_TEXT.hexval()}" size="9">{score_s}</font>',
                styles["strength_name"],
            )])
        t = Table(rows, colWidths=[(page_width - 0.5 * inch) / 2])
        t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        return t

    strengths_t = _highlight_list(
        _t(s, "top_metrics", "Top Metrics"), strengths, RATING_COLORS["excellent"],
    )
    growth_t = _highlight_list(
        _t(s, "low_metrics", "Low Metrics"), growth, RATING_COLORS["bad"],
    )

    highlights = Table(
        [[strengths_t, growth_t]],
        colWidths=[(page_width - 0.2 * inch) / 2, (page_width - 0.2 * inch) / 2],
    )
    highlights.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    elements.append(highlights)
    elements.append(PageBreak())
    return elements


# ---------------------------------------------------------------------------
# Group metrics by category
# ---------------------------------------------------------------------------

def _group_by_category(metrics: list[dict]) -> dict[str, list[dict]]:
    category_order = get_category_order()
    groups: dict[str, list[dict]] = {cat: [] for cat in category_order}
    for m in metrics:
        cat = m.get("category", "contextual")
        if cat not in groups:
            groups[cat] = []
        groups[cat].append(m)
    return groups


# ---------------------------------------------------------------------------
# Build category section
# ---------------------------------------------------------------------------

def _build_category_section(
    category: str,
    metrics: list[dict],
    styles: dict,
    page_width: float,
    strings: dict[str, str] | None = None,
) -> list:
    s = strings or {}
    elements: list = []
    if not metrics:
        return elements

    # Section header with colored left border
    display_name = get_category_name(category) if category else category
    header_p = Paragraph(display_name, styles["section"])
    count_key = "metric_count_plural" if len(metrics) != 1 else "metric_count"
    count_text = _t(s, count_key, f"{len(metrics)} metric{'s' if len(metrics) != 1 else ''}", count=len(metrics))
    count_p = Paragraph(
        f'<font color="{LIGHT_TEXT.hexval()}" size="9">{count_text}</font>',
        ParagraphStyle("CatCount", fontSize=9, alignment=TA_RIGHT),
    )
    header_t = Table(
        [[header_p, count_p]],
        colWidths=[page_width * 0.7, page_width * 0.3],
    )
    header_t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
        ("LINEBELOW", (0, 0), (-1, -1), 1.5, PRIMARY),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elements.append(header_t)
    elements.append(Spacer(1, 0.15 * inch))

    # All metrics rendered as cards (including INSUFFICIENT_DATA)
    for m in metrics:
        card_elements = _build_metric_card(m, styles, page_width)
        elements.extend(card_elements)

    elements.append(Spacer(1, 0.3 * inch))
    return elements


# ---------------------------------------------------------------------------
# Page header/footer
# ---------------------------------------------------------------------------

def _make_header_footer(pagesize: tuple[float, float], strings: dict[str, str] | None = None):
    """Return a header/footer callback bound to *pagesize* and *strings*."""
    s = strings or {}

    def _header_footer(canvas, doc):
        canvas.saveState()
        w, h = pagesize
        # Footer
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(LIGHT_TEXT)
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        footer_text = _t(s, "footer", "Generated {date}  |  Page {page}", date=now_str, page=doc.page)
        canvas.drawCentredString(w / 2, 0.4 * inch, footer_text)
        # Header line (subtle)
        if doc.page > 1:
            canvas.setStrokeColor(BORDER)
            canvas.setLineWidth(0.5)
            canvas.line(0.75 * inch, h - 0.5 * inch, w - 0.75 * inch, h - 0.5 * inch)
            canvas.setFont("Helvetica", 8)
            canvas.setFillColor(LIGHT_TEXT)
            canvas.drawString(
                0.75 * inch, h - 0.45 * inch,
                _t(s, "header_text", "Engineering Impact Report"),
            )
        canvas.restoreState()

    return _header_footer


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_candidate_pdf(
    metrics_results: list[dict],
    user_login: str,
    period_str: str,
    output_path: str = "candidate_report.pdf",
    repositories: list[str] | None = None,
    locale: str = "en",
    page_size: str = "letter",
) -> None:
    """Generate a modern executive PDF report.

    Parameters
    ----------
    locale : str
        Locale code (e.g. ``"en"``, ``"de"``).  Loads strings from
        ``impact/templates/locales/<locale>.yaml``.
    page_size : str
        Page size name: ``"letter"`` (US, default) or ``"a4"`` (international).
    """
    strings = _load_locale(locale)
    pagesize = PAGE_SIZES.get(page_size.lower(), letter)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=pagesize,
        leftMargin=0.85 * inch,
        rightMargin=0.85 * inch,
        topMargin=0.85 * inch,
        bottomMargin=0.75 * inch,
    )
    page_width = pagesize[0] - 1.7 * inch  # usable width
    styles = _build_styles()

    elements: list = []

    # Page 1: Executive summary
    elements.extend(_build_executive_summary(
        metrics_results, user_login, period_str, styles, page_width,
        strings=strings,
    ))

    # Pages 2+: Category sections
    groups = _group_by_category(metrics_results)
    for category_slug in get_category_order():
        cat_metrics = groups.get(category_slug, [])
        if not cat_metrics:
            continue
        elements.extend(_build_category_section(
            category_slug, cat_metrics, styles, page_width, strings=strings,
        ))

    # Build PDF
    hf = _make_header_footer(pagesize, strings)
    doc.build(elements, onFirstPage=hf, onLaterPages=hf)
    print(f"PDF exported to {output_path}")
