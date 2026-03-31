import re
from collections import Counter

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric
from impact.metrics.utils import (
    _load_customization,
    filter_prs_for_contribution,
    is_test_file,
    is_documentation_file,
    is_dependency_file,
)

# Conventional commit types (conventionalcommits.org)
_CATEGORY_TYPES = [
    "feat", "fix", "docs", "style", "refactor", "perf",
    "test", "build", "ci", "chore", "revert",
]
_PREFIX_RE = re.compile(
    r"^(" + "|".join(_CATEGORY_TYPES) + r")(?:\(.+?\))?!?:\s",
    re.IGNORECASE,
)

# Built-in label → category mapping (language-neutral, team-applied metadata)
_LABEL_TO_CATEGORY_BUILTIN: dict[str, str] = {
    "bug": "fix", "fix": "fix", "hotfix": "fix", "type:bug": "fix",
    "type:fix": "fix", "kind/bug": "fix", "kind/fix": "fix",
    "feature": "feat", "enhancement": "feat", "type:feature": "feat",
    "kind/feature": "feat",
    "documentation": "docs", "docs": "docs", "type:docs": "docs",
    "test": "test", "testing": "test", "type:test": "test",
    "ci": "ci", "cd": "ci", "devops": "ci", "type:ci": "ci",
    "dependencies": "chore", "chore": "chore", "type:chore": "chore",
    "performance": "perf", "type:perf": "perf",
    "refactor": "refactor", "type:refactor": "refactor",
    "style": "style", "type:style": "style",
}


def _get_label_to_category() -> dict[str, str]:
    """Return the full label-to-category mapping (built-in + config extras)."""
    cfg = _load_customization()
    extras = cfg.get("extra_label_to_category") or {}
    if not extras:
        return _LABEL_TO_CATEGORY_BUILTIN
    merged = dict(_LABEL_TO_CATEGORY_BUILTIN)
    for label, cat in extras.items():
        merged[label.lower()] = cat.lower()
    return merged

# CI config paths (cross-provider)
_CI_CONFIG_PATHS: tuple[str, ...] = (
    ".github/workflows/", ".gitlab-ci.yml", "bitbucket-pipelines.yml",
    "azure-pipelines.yml", ".circleci/", "Jenkinsfile", ".drone.yml",
    ".travis.yml", ".buildkite/",
)


def _is_ci_config_file(filename: str) -> bool:
    f_lower = filename.lower()
    return any(f_lower.startswith(p) or ("/" + p) in f_lower or f_lower.endswith(p) for p in _CI_CONFIG_PATHS)


def _classify_by_labels(labels: list[str]) -> str | None:
    """Classify PR by labels (language-neutral, team-applied metadata)."""
    label_map = _get_label_to_category()
    for label in labels:
        cat = label_map.get(label.lower())
        if cat:
            return cat
    return None


def _classify_by_diff(filenames: list[str]) -> str | None:
    """Classify PR by file-path structure (language-neutral)."""
    if not filenames:
        return None
    if all(is_test_file(fn) for fn in filenames):
        return "test"
    if all(is_documentation_file(fn) for fn in filenames):
        return "docs"
    if all(_is_ci_config_file(fn) for fn in filenames):
        return "ci"
    if all(is_dependency_file(fn) for fn in filenames):
        return "chore"
    return None


def _classify_pr(title: str, body: str | None, labels: list[str] | None = None, filenames: list[str] | None = None) -> str:
    """Classify a PR into a category — multi-signal, language-neutral layers.

    Priority:
      1. Conventional commit prefix (spec-defined, universal)
      2. PR labels (team-applied metadata, universal)
      3. Structural diff analysis (file-path-based, universal)
      4. English keyword fallback (backward-compatible)
    """
    # Priority 1: Conventional commit prefix
    for text in [title, body or ""]:
        m = _PREFIX_RE.match(text.strip())
        if m:
            return m.group(1).lower()
    # Priority 2: PR labels
    if labels:
        cat = _classify_by_labels(labels)
        if cat:
            return cat
    # Priority 3: Structural diff analysis
    if filenames:
        cat = _classify_by_diff(filenames)
        if cat:
            return cat
    # Priority 4: English keyword fallback (backward-compatible)
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

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0

        all_prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date
        )
        prs = filter_prs_for_contribution(all_prs, exclude_drafts=True, only_merged=False)

        category_counts: Counter = Counter()
        per_pr: list[dict] = []

        for pr in prs:
            pr_files = context.ledger.get_files_for_pr(pr.number)
            filenames = [f.filename for f in pr_files] if pr_files else None
            cat = _classify_pr(pr.title, pr.body, labels=getattr(pr, 'labels', None), filenames=filenames)
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
            "period_days": period_days,
        }
        if not prs or (period_days < 14 and len(prs) < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)
