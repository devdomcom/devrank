from __future__ import annotations

from collections import defaultdict
from statistics import mean, pstdev

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution, is_generated_file


def _indent_level(line: str) -> float:
    expanded = line.replace("\t", "    ")
    indent_spaces = len(expanded) - len(expanded.lstrip(" "))
    return indent_spaces / 4


def _complexity_from_patch(patch: str | None) -> float | None:
    if not patch:
        return None
    indent_levels: list[float] = []
    for line in patch.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        content = line[1:]
        if not content.strip():
            continue
        indent_levels.append(_indent_level(content))
    if not indent_levels:
        return None
    return sum(indent_levels) / len(indent_levels)


class ComplexityTrend(Metric):
    """
    Track whitespace-based complexity trends per file over time.

    Complexity is approximated by the average indentation level (whitespace) of
    added lines in diffs. This is language-neutral and relies only on indentation.
    Per-file trends include min/max/std dev across the period.
    """

    @property
    def slug(self) -> str:
        return "complexity_trend"

    @property
    def name(self) -> str:
        return "Complexity Trend"

    @property
    def description(self) -> str:
        return "Whitespace-based per-file complexity trends (min/max/std dev of indentation)."

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["Traditional"]

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        file_samples: dict[str, list[tuple[int, float]]] = defaultdict(list)

        for pr in prs:
            files = context.ledger.get_files_for_pr(pr.number)
            for file in files:
                if is_generated_file(file.filename, file.patch):
                    continue
                complexity = _complexity_from_patch(file.patch)
                if complexity is None:
                    continue
                file_samples[file.filename].append((pr.number, complexity))

        file_trends: list[dict[str, object]] = []
        std_devs: list[float] = []
        max_values: list[float] = []
        min_values: list[float] = []

        for filename, samples in sorted(file_samples.items()):
            values = [value for _, value in samples]
            max_value = max(values)
            min_value = min(values)
            std_dev = pstdev(values) if len(values) > 1 else 0.0
            std_devs.append(std_dev)
            max_values.append(max_value)
            min_values.append(min_value)

            file_trends.append(
                {
                    "filename": filename,
                    "samples": [
                        {"pr_number": pr_number, "complexity": round(value, 3)}
                        for pr_number, value in samples
                    ],
                    "min_complexity": round(min_value, 3),
                    "max_complexity": round(max_value, 3),
                    "mean_complexity": round(mean(values), 3),
                    "std_dev": round(std_dev, 3),
                }
            )

        max_std_dev = max(std_devs) if std_devs else 0.0
        avg_std_dev = mean(std_devs) if std_devs else 0.0
        overall_max = max(max_values) if max_values else 0.0
        overall_min = min(min_values) if min_values else 0.0

        summary = (
            f"Tracked {len(file_trends)} files. Max std dev: {max_std_dev:.2f}, "
            f"range: {overall_min:.2f}-{overall_max:.2f}."
        )

        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        details: dict[str, object] = {
            "file_count": len(file_trends),
            "sample_count": sum(len(values) for values in file_samples.values()),
            "max_std_dev": round(max_std_dev, 3),
            "avg_std_dev": round(avg_std_dev, 3),
            "max_complexity": round(overall_max, 3),
            "min_complexity": round(overall_min, 3),
            "file_trends": file_trends,
            "period_days": period_days,
        }

        if not file_trends or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True

        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
