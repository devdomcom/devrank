import re
from collections import Counter

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import filter_prs_for_contribution

# Conventional commit types (conventionalcommits.org)
_CATEGORY_TYPES = [
    "feat", "fix", "docs", "style", "refactor", "perf",
    "test", "build", "ci", "chore", "revert",
]
_PREFIX_RE = re.compile(
    r"^(" + "|".join(_CATEGORY_TYPES) + r")(?:\(.+?\))?!?:\s",
    re.IGNORECASE,
)


def _classify_pr(title: str, body: str | None) -> str:
    """Classify a PR into a category from its title or body."""
    for text in [title, body or ""]:
        m = _PREFIX_RE.match(text.strip())
        if m:
            return m.group(1).lower()
    # Fallback heuristics from title keywords
    t = title.lower()
    if any(w in t for w in ["fix", "bug", "hotfix", "patch"]):
        return "fix"
    if any(w in t for w in ["feat", "add", "implement", "new"]):
        return "feat"
    if any(w in t for w in ["refactor", "restructure", "clean"]):
        return "refactor"
    if any(w in t for w in ["doc", "readme", "changelog"]):
        return "docs"
    if any(w in t for w in ["test", "spec", "coverage"]):
        return "test"
    if any(w in t for w in ["ci", "pipeline", "workflow", "deploy"]):
        return "ci"
    if any(w in t for w in ["chore", "bump", "upgrade", "dependency"]):
        return "chore"
    if any(w in t for w in ["perf", "optimiz", "speed", "fast"]):
        return "perf"
    if any(w in t for w in ["style", "format", "lint"]):
        return "style"
    return "other"


class PRCategoryDiversity(Metric):
    @property
    def slug(self) -> str:
        return "pr_category_diversity"

    @property
    def name(self) -> str:
        return "PR Category Diversity"

    @property
    def description(self) -> str:
        return "Number of distinct PR categories (feat/fix/refactor/docs/etc.) for work breadth."

    def run(self, context: MetricContext) -> MetricResult:
        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        category_counts: Counter = Counter()
        per_pr: list[dict] = []

        for pr in prs:
            cat = _classify_pr(pr.title, pr.body)
            category_counts[cat] += 1
            per_pr.append({"number": pr.number, "category": cat})

        distinct_categories = len(category_counts)
        distribution = dict(category_counts.most_common())

        summary = (
            f"{distinct_categories} distinct PR categories across {len(prs)} PRs: "
            f"{', '.join(f'{k}={v}' for k, v in category_counts.most_common(5))}."
        )
        details: dict[str, object] = {
            "distinct_categories": distinct_categories,
            "distribution": distribution,
            "pr_count": len(prs),
            "per_pr": per_pr,
        }
        if not prs:
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
