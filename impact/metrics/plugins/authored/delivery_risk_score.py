"""Delivery Risk Score (1-10) metric.

Computes a per-period risk score based on:
- File count (more files = higher risk)
- Code complexity (from patch indentation)
- Diffusion (change proximity / scatteredness)
- Experience (inverse of contributor experience)
- Risk labels on PRs (e.g., "risk:db-migration")

Higher score = higher delivery risk.
"""

from __future__ import annotations

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import (
    build_file_contributors,
    complexity_from_patch,
    compute_change_proximity,
    filter_prs_for_contribution,
    is_generated_file,
)


def _count_risk_labels(prs) -> int:
    """Count PRs that have risk-related labels (e.g., 'risk:db-migration', 'risk:breaking')."""
    risk_keywords = {"risk", "breaking", "migration", "hotfix", "urgent"}
    count = 0
    for pr in prs:
        for label in getattr(pr, "labels", []):
            label_lower = label.lower()
            if any(kw in label_lower for kw in risk_keywords):
                count += 1
                break  # Count each PR once
    return count


def _compute_experience_pct(context: MetricContext) -> float:
    """Compute contributor experience percentage (0-100)."""
    bundle = context.ledger.bundle
    all_files = {
        f.filename
        for f in getattr(bundle, "files", [])
        if f.filename and not is_generated_file(f.filename, getattr(f, "patch", None))
    }
    if not all_files:
        return 0.0
    file_contributors = build_file_contributors(all_files, bundle, context.ledger, by="lines")
    author_totals = {}
    for filename, authors in file_contributors.items():
        for author, score in authors.items():
            author_totals[author] = author_totals.get(author, 0.0) + score
    total_lines = sum(author_totals.values())
    user_lines = author_totals.get(context.user_login, 0.0)
    return (user_lines / total_lines * 100) if total_lines > 0 else 0.0


def _normalize_to_1_10(value: float, low: float, high: float, invert: bool = False) -> float:
    """Normalize a value to 1-10 scale.

    If invert=True, lower values map to higher scores (e.g., low experience = high risk).
    """
    if high <= low:
        return 5.0
    clamped = max(low, min(high, value))
    normalized = (clamped - low) / (high - low)  # 0..1
    if invert:
        normalized = 1.0 - normalized
    return 1.0 + normalized * 9.0  # 1..10


class DeliveryRiskScore(Metric):
    """Delivery Risk Score (1-10): aggregate risk signal per period.

    Combines file count, code complexity, diffusion (scatteredness),
    experience, and risk labels into a single 1-10 score.
    """

    @property
    def slug(self) -> str:
        return "delivery_risk_score"

    @property
    def name(self) -> str:
        return "Delivery Risk Score"

    @property
    def description(self) -> str:
        return "Per-period delivery risk (1-10) from file count, complexity, diffusion, experience, and risk labels."

    @property
    def category(self) -> str:
        return "reliability"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        if not prs:
            return MetricResult(
                metric_slug=self.slug,
                summary="No PRs in analysis period.",
                details={
                    "risk_score": None,
                    "pr_count": 0,
                    "file_count": 0,
                    "no_data": True,
                },
            )

        # Collect per-PR risk signals
        total_files = 0
        total_non_generated_files = 0
        total_complexity = 0.0
        complexity_count = 0
        per_pr_risk = []

        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            pr_files = 0
            pr_non_gen = 0
            pr_complexity = 0.0
            pr_complexity_count = 0

            for f in files:
                pr_files += 1
                if is_generated_file(f.filename, f.patch):
                    continue
                pr_non_gen += 1
                comp = complexity_from_patch(f.patch)
                if comp is not None:
                    pr_complexity += comp
                    pr_complexity_count += 1
                    total_complexity += comp
                    complexity_count += 1

            total_files += pr_files
            total_non_generated_files += pr_non_gen

            per_pr_risk.append({
                "pr_number": pr.number,
                "changed_files": pr_files,
                "non_generated_files": pr_non_gen,
                "avg_complexity": round(pr_complexity / pr_complexity_count, 3) if pr_complexity_count else None,
            })

        # Compute diffusion (change proximity)
        proximity_details = compute_change_proximity(
            context.ledger, prs,
            start_date=context.start_date, end_date=context.end_date,
        )
        avg_proximity = proximity_details.get("avg_proximity_per_change", 0.0)

        # Compute experience (inverse = risk)
        experience_pct = _compute_experience_pct(context)

        # Count risk labels
        risk_label_count = _count_risk_labels(prs)

        # Compute normalized component scores (1-10)
        # File count: 1-50 files scale
        file_score = _normalize_to_1_10(total_non_generated_files, 1, 50)

        # Complexity: 0-4 indent levels scale (higher = more complex = riskier)
        complexity_score = _normalize_to_1_10(
            (total_complexity / complexity_count) if complexity_count else 0.0,
            0.0, 4.0,
        )

        # Diffusion: 0-50 proximity scale (higher = more scattered = riskier)
        diffusion_score = _normalize_to_1_10(avg_proximity, 0.0, 50.0)

        # Experience: 0-100% scale, inverted (low experience = high risk)
        experience_score = _normalize_to_1_10(experience_pct, 0.0, 100.0, invert=True)

        # Risk labels: each risky PR adds ~1 point, capped at +3
        label_score = min(3.0, risk_label_count * 1.0)

        # Weighted combination → 1-10
        # Weights: file 30%, complexity 20%, diffusion 30%, experience 10%, labels 10%
        # (Experience weight reduced because user-centric data always shows 100% experience)
        raw = (
            0.30 * file_score +
            0.20 * complexity_score +
            0.30 * diffusion_score +
            0.10 * experience_score +
            0.10 * (1 + label_score)  # base 1 + label bonus
        )
        risk_score = round(min(10.0, max(1.0, raw)))

        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 30.0
        )

        details = {
            "risk_score": risk_score,
            "pr_count": len(prs),
            "file_count": total_files,
            "non_generated_file_count": total_non_generated_files,
            "avg_complexity": round(total_complexity / complexity_count, 3) if complexity_count else 0.0,
            "avg_proximity": round(avg_proximity, 2),
            "experience_pct": round(experience_pct, 1),
            "risk_label_count": risk_label_count,
            "component_scores": {
                "file_score": round(file_score, 2),
                "complexity_score": round(complexity_score, 2),
                "diffusion_score": round(diffusion_score, 2),
                "experience_score": round(experience_score, 2),
                "label_score": round(label_score, 2),
            },
            "per_pr": per_pr_risk,
            "period_days": round(period_days, 1),
        }

        if len(prs) < 1 or (period_days < 14 and len(prs) < 2):
            details["no_data"] = True

        summary = (
            f"Delivery risk score: {risk_score}/10 "
            f"({len(prs)} PRs, {total_non_generated_files} files, "
            f"experience {experience_pct:.0f}%, {risk_label_count} risk labels)."
        )
        if details.get("no_data"):
            summary = "Insufficient data to compute delivery risk score."

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
