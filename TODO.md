# Language-Dependent and Provider-Dependent Metrics

This document catalogues every function, pattern, and metric in `impact/metrics/`
whose output depends on assumptions about:

1. **Natural language** (English) of commit messages, PR titles/bodies, and
   review comments.
2. **Programming-language-specific** constructs that limit coverage to a subset
   of languages.
3. **Provider-specific** features of GitHub that will not exist (or will differ)
   on GitLab, Bitbucket, Azure DevOps, Gitea, and other providers.

The goal: every metric must be computable -- and produce meaningful results --
regardless of the human language used by the team, the programming language of
the repository, **or the hosting provider** (GitHub, GitLab, Bitbucket, etc.).

DevRank already has a provider abstraction (`impact/adapters/base.py` defines
`ProviderAdapter`; `impact/adapters/registry.py` selects an adapter by name).
All provider-specific parsing lives in adapters, and metrics operate solely on
the canonical domain model (`impact/domain/models.py`).  The principle:

> **Metrics MUST only depend on the canonical model.**
> Provider-specific data must be normalised into canonical fields by the adapter.
> Metrics must never reference a specific provider's API, syntax, or convention
> directly.

Each item includes a **Proposed Fix** that keeps the metric sound while removing
or minimising the dependency.  Fixes are classified:

| Tag | Meaning |
|---|---|
| **MODEL** | Requires adding a field to the domain model / adapter |
| **LOGIC** | Pure logic change inside the metric or utility |
| **CONFIG** | Adds an external configuration knob |
| **PROVIDER** | Fix must account for cross-provider differences |

---

## 1. Natural-Language (English) Dependencies in `impact/metrics/utils.py`

These helpers match English words in free-text fields (commit messages, PR
titles/bodies, review comments). They will **silently miss** equivalent signals
written in any other human language.

### 1.1 ✅ FIXED — `is_bug_fix_indicator(text)` -- Bug-Fix Detection

Used by: **`bug_fix_focus_rate`**

| Pattern kind | Examples |
|---|---|
| Conventional commit prefix | `fix:`, `fix(scope):`, `fix!:` |
| Keyword prefixes | `"bugfix:"`, `"bug fix"`, `"bug:"`, `"hotfix"` |
| Issue-linking phrases | `fixes #123`, `closes #123`, `resolves #123`, `issue #123` |
| Word-boundary terms | `\berror\b`, `\bcrash\b`, `\bregression\b` |

> **Proposed Fix** `[MODEL + LOGIC + PROVIDER]`
>
> Labels / tags are available on every major provider's merge-request API:
>
> | Provider | API field | Format |
> |---|---|---|
> | GitHub | `labels[].name` | string array |
> | GitLab | `labels` | string array |
> | Bitbucket | `properties` / workspace labels | varies |
> | Azure DevOps | linked work-item tags | string array |
> | Gitea | `labels[].name` | string array |
>
> 1. **Add `labels: list[str] = []` to `PullRequest`** in
>    `impact/domain/models.py`.  Each adapter normalises its provider's
>    label representation into this flat string list.  GitHub adapter:
>    `labels=[l["name"] for l in raw.get("labels", [])]`.
>
> 2. **Rewrite `is_bug_fix_indicator()` as a multi-signal scorer** with a new
>    signature `is_bug_fix_indicator(text, labels=None) -> bool`:
>
>    | Priority | Signal | Language-neutral? | Implementation |
>    |---|---|---|---|
>    | **1 (primary)** | PR labels match configurable bug-fix set | Yes -- labels are team-applied metadata | `any(l in _BUG_LABELS for l in labels)` where `_BUG_LABELS = {"bug", "fix", "hotfix", "type:bug", "type:fix", "kind/bug", ...}` loaded from config YAML |
>    | **2** | Conventional commit prefix `fix:` / `fix(scope):` | Yes -- defined by the Conventional Commits *spec*, not English | Keep existing `re.match(r'^fix(\(.*?\))?!?:', ...)` |
>    | **3** | Issue cross-reference anywhere in text | Yes -- purely numeric | `_CROSS_REF_RE.search(text)` matching `#NNN`, `!NNN`, `AB#NNN` across providers |
>    | **4 (fallback)** | English keyword matching (current logic) | No | Keep current patterns but weight as lowest-priority |
>
> 3. **Pass labels downstream**: `bug_fix_focus_rate.run()` already has access
>    to the PR; pass `pr.labels` into the indicator call.
>
> **Metric soundness:** The label signal is *more* accurate than text mining
> (it reflects the team's own classification), the conventional-commit prefix is
> spec-defined, and issue cross-refs are universal. English keywords remain as
> a degraded fallback. Net result: non-English teams with labels get full
> accuracy; without labels, behaviour is no worse than today.

### 1.2 ✅ FIXED — `is_revert_indicator(text)` -- Revert Detection

Used by: **`revert_introduction_rate`**

- `text.lower().startswith("revert")`
- `"reverts commit" in text.lower()`

> **Proposed Fix** `[LOGIC]`
>
> A revert always references the SHA it undoes. That SHA is language-neutral.
>
> Replace the current implementation with a two-layer check:
>
> ```python
> _REVERT_SHA_RE = re.compile(r'[a-f0-9]{40}')
>
> def is_revert_indicator(text: str) -> bool:
>     if not text:
>         return False
>     text_lower = text.lower()
>     # Layer 1 (platform): Git/GitHub always inserts the English "Revert"
>     # prefix AND "This reverts commit <sha>".  Keep as primary for speed.
>     if text_lower.startswith("revert") or "reverts commit" in text_lower:
>         return True
>     # Layer 2 (structural): any message whose body contains a 40-char SHA
>     # in the second line or later (body, not subject) is a revert candidate.
>     # Require the SHA to appear after a newline to avoid matching random
>     # SHAs in non-revert messages.
>     body = text.split("\n", 1)[1] if "\n" in text else ""
>     return bool(_REVERT_SHA_RE.search(body))
> ```
>
> **Metric soundness:** `git revert` *always* writes the reverted SHA into the
> message body regardless of locale.  Layer 2 catches non-English revert
> messages (e.g. `Rueckgaengig: ...` in German Git) as long as the SHA is
> present. False-positive risk is low because the SHA must appear in the
> message *body* (not subject), which is uncommon outside reverts.

### 1.3 ✅ FIXED — `is_conventional_commit(message)` -- Conventional Commit Check

Used by: **`conventional_commit_rate`**

Hardcoded English type prefixes:
```
feat fix docs style refactor perf test build ci chore revert
```

> **Proposed Fix** `[LOGIC]`
>
> The Conventional Commits specification (conventionalcommits.org) defines
> these exact strings as **protocol tokens**, not English prose.  They are
> analogous to HTTP methods `GET`/`POST` -- English-origin but spec-defined.
>
> The fix is therefore *not* to translate them, but to **also recognise any
> structured `word(scope?):` prefix pattern** as a *structured commit* even if
> the type word is not one of the 11 spec types:
>
> ```python
> # Strict conventional commit (spec-defined types)
> _CC_STRICT_RE = re.compile(
>     r'^(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)'
>     r'(?:\(.+?\))?!?:\s', re.IGNORECASE
> )
> # Broad structured commit (any single-word type + colon)
> _CC_BROAD_RE = re.compile(r'^[a-zA-Z\u00C0-\u024F\u3000-\u9FFF]+(?:\(.+?\))?!?:\s')
>
> def is_conventional_commit(message: str) -> bool:
>     return bool(_CC_STRICT_RE.match(message))
>
> def is_structured_commit(message: str) -> bool:
>     """Broader check: any word: prefix, including non-English types."""
>     return bool(_CC_BROAD_RE.match(message))
> ```
>
> Expose **both** rates in `conventional_commit_rate` details:
> `"strict_rate"` (spec types) and `"structured_rate"` (any `word:` prefix).
> Use `structured_rate` for rating so that a team writing `機能:`, `修正:`,
> or `fonctionnalite:` still gets credit.
>
> **Metric soundness:** The metric now rewards *any* structured discipline,
> not just the 11 English tokens.  The strict rate remains available for teams
> that specifically adopt the English spec.

### 1.4 ✅ FIXED — `classify_defect_commit(message)` / `_DEFECT_PATTERNS` -- Defect Mining

Used by: **`commit_message_mining`**

All regex patterns match **English** words only:

| Group | Keywords |
|---|---|
| Core defect | `fix(es/ed/ing)`, `bug(s/fix)`, `hotfix`, `defect`, `issue` |
| Stability | `resolve`, `patch`, `regression`, `crash`, `flaky` |
| Production | `incident`, `outage`, `rollback`, `mitigate`, `triage`, `escalate` |
| Errors | `error`, `fail(ure)`, `panic`, `exception` |

> **Proposed Fix** `[MODEL + LOGIC]`
>
> Same label infrastructure as section 1.1.
>
> 1. **Primary signal -- PR labels:**  If the commit belongs to a PR
>    (`c.pull_request_number`), look up the PR labels.  A label in
>    `_DEFECT_LABELS = {"bug", "defect", "type:bug", "incident", ...}`
>    (configurable YAML) classifies the commit as defect-related regardless
>    of message language.
>
> 2. **Secondary signal -- issue cross-ref:**  If the message contains `#\d+`
>    and the referenced issue (if fetchable) carries a bug label, classify as
>    defect.  (Requires the issue metadata to be in the bundle -- acceptable
>    as a medium-term enhancement.)
>
> 3. **Tertiary signal -- English keywords (current logic):**  Keep as
>    fallback for backward compatibility when labels are unavailable.
>
> ```python
> def classify_defect_commit(message, labels=None):
>     if labels and any(l.lower() in _DEFECT_LABELS for l in labels):
>         return "defect"
>     if re.search(r'#\d+', message):
>         # Future: cross-ref issue labels here
>         pass
>     # Existing English regex fallback
>     return _english_defect_classify(message)
> ```
>
> **Metric soundness:** Labels are the team's own ground truth -- more
> accurate than text mining even for English teams.  The English fallback
> preserves today's behaviour when labels are absent.

### 1.5 ✅ FIXED — `compute_pr_body_quality(body)` -- PR Body Scoring

Used by: **`pr_body_quality_score`**

English-only bonus patterns:
- `(?:fixes|closes|resolves|refs?)\s*#?\d+`
- `pr\s*#?\d+`
- `"pull request" in body.lower()`

> **Proposed Fix** `[LOGIC + PROVIDER]`
>
> The structural quality signals (body length, markdown section count) are
> already language-neutral and contribute 70/100 points.  The English bias is
> only in the 15-point issue-reference bonus.
>
> **Provider consideration:** Issue / MR cross-reference syntax differs:
>
> | Provider | Issue ref | MR/PR ref | Auto-close keywords |
> |---|---|---|---|
> | GitHub | `#123` | `#123` (shared namespace) | `fixes`, `closes`, `resolves` |
> | GitLab | `#123` | `!123` | `fixes`, `closes`, `resolves` + [9 more](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html) |
> | Bitbucket | `#123` | `#123` | `fixes`, `closes`, `resolves` |
> | Azure DevOps | `AB#123` | `!123` | `fixes`, `closes`, `resolves` |
> | Gitea | `#123` | `#123` | `fixes`, `closes`, `resolves` |
>
> Replace the three English patterns with a **provider-neutral union**:
>
> ```python
> # Any cross-reference token across providers:
> #   #NNN (GitHub/GitLab/Bitbucket/Gitea), !NNN (GitLab MR/Azure DevOps),
> #   AB#NNN (Azure DevOps work items)
> _CROSS_REF_RE = re.compile(r'(?<!\w)(?:AB)?[#!]\d{1,6}\b')
>
> if _CROSS_REF_RE.search(body):
>     score += 15
> # Any URL reference (linking to tracker, docs, etc.)
> elif re.search(r'https?://\S+', body):
>     score += 10
> ```
>
> Drop: `"pull request" in body.lower()` (GitHub-specific terminology;
> GitLab calls them "merge requests").
>
> Drop: `(?:fixes|closes|resolves)\s*#\d+` as a separate branch -- the
> universal `_CROSS_REF_RE` already catches these.  Auto-close keywords
> are a platform feature handled by the provider, not a quality signal.
>
> **Metric soundness:** A GitLab MR body with `関連: !456` or an Azure
> DevOps PR with `AB#789` now earns the 15-point bonus.  The URL fallback
> catches references to any external tracker (Jira, Linear, etc.).

### 1.6 ✅ FIXED — Merge Commit Filtering -- `startswith("merge ")`

Used by: **`follow_up_commit_rate`**, **`conventional_commit_rate`**,
**`commit_message_mining`**, `approval_was_final()` in utils.

Git's default merge message is English (`Merge branch ...` / `Merge pull
request ...`). Non-English Git locale configurations may produce translated
prefixes (e.g., German `Zusammenführen`, French `Fusionner`), causing these
commits to leak through the filter and pollute commit-based metrics.

| Location | Pattern |
|---|---|
| `plugins/authored/follow_up_commit_rate.py:45` | `c.message.lower().startswith("merge ")` |
| `plugins/authored/conventional_commit_rate.py:32` | `c.message.strip().lower().startswith("merge ")` |
| `plugins/authored/commit_message_mining.py:33` | `c.message.strip().lower().startswith("merge ")` |
| `utils.py` -- `approval_was_final()` | `c.message.lower().startswith("merge ")` |

> **Proposed Fix** `[MODEL + LOGIC]`
>
> Parent count is a **Git DAG property**, not a provider feature.  Every
> provider's commit API exposes it because it is intrinsic to Git:
>
> | Provider | API field |
> |---|---|
> | GitHub | `parents[]` (array of sha objects) |
> | GitLab | `parent_ids` (array of sha strings) |
> | Bitbucket | `parents[]` (array of commit objects) |
> | Azure DevOps | `parents[]` (array of sha strings) |
> | Gitea | `parents[]` (array of sha objects) |
> | Raw Git | `git cat-file -p <sha>` lists parent lines |
>
> 1. **Add `parent_count: int = 1` to `Commit`** in
>    `impact/domain/models.py`.
>
> 2. **Each adapter populates it from its own API field.**  GitHub adapter:
>    `parent_count=len(raw.get("parents", []) or [1])`
>
> 3. **Add utility:**
>    ```python
>    def is_merge_commit(c: Commit) -> bool:
>        """Structural merge detection via Git DAG parent count."""
>        return c.parent_count >= 2
>    ```
>
> 4. **Replace all 4 locations:**
>    ```python
>    # Before (English-dependent)
>    if c.message.lower().startswith("merge "):
>    # After (structural)
>    if is_merge_commit(c):
>    ```
>
> **Metric soundness:** Parent-count detection is the gold standard used by
> `git log --merges`.  It correctly handles non-English Git locales, custom
> merge messages, and squash-merge commits (which have 1 parent and are
> correctly *not* filtered).  No regressions.

### 1.7 ✅ FIXED — `_GENERATED_MARKERS` -- Generated-File Header Detection

Used by: `is_generated_file()` -- affects **every metric that filters generated
files** (trivial_contribution_rate, code_churn_rate, rework_rate,
complexity_trend, hotspot_detection, entity_fragmentation, knowledge_islands,
code_survival, sum_of_coupling, change_proximity, absolute_churn_trend,
net_code_contribution, pr_size_distribution, bus_factor).

English-only header markers scanned in the first 10 added lines of a patch:
```
@generated
auto-generated
automatically generated
do not edit
do not modify
this file is generated
code generated by
generated by
```

Non-English equivalents (`自動生成`, `nicht bearbeiten`, `ne pas modifier`) will
not be detected, causing generated files to be treated as human-written code.

**Mitigating factor:** this is only one of five detection layers. Basename,
suffix, directory, and entropy/compression layers are language-neutral and will
still catch lockfiles, minified bundles, protobuf output, etc.

> **Proposed Fix** `[LOGIC + CONFIG]`
>
> The text-marker layer is one of **five** detection layers.  The other four
> (basename, suffix, directory, entropy/compression) are already fully
> language-neutral and catch the vast majority of generated files.
>
> 1. **Elevate `@generated` as the primary marker.**  The `@generated` tag is
>    a tool convention (used by Facebook's codegen, Protocol Buffers, Thrift,
>    GraphQL codegen, Dart `build_runner`, etc.) and is never localised.
>    Tools that emit it use it regardless of the developer's spoken language.
>
> 2. **Add `@auto-generated` and `@autogenerated`** (also tool conventions).
>
> 3. **Keep English phrases as secondary markers** but document them as
>    "English-only bonus layer" rather than a primary detection mechanism.
>
> 4. **Add `extra_generated_markers` config knob** (YAML list) so orgs with
>    non-English codegen headers can register their own markers:
>    ```yaml
>    extra_generated_markers:
>      - "自動生成"
>      - "nicht bearbeiten"
>      - "ne pas modifier"
>    ```
>
> **Metric soundness:** The four structural layers already provide strong
> coverage.  Promoting `@generated` (universal) as the text-layer primary
> and allowing config overrides closes the gap without losing any current
> detection.  Risk of regressions: zero.

### 1.8 ✅ FIXED — `is_documentation_file(filename)` -- Documentation File Detection

Used by: **`documentation_touch_rate`**

| Component | English values |
|---|---|
| `_DOC_BASENAMES` | `readme`, `changelog`, `contributing`, `license`, `notice`, `authors`, `faq`, `glossary` |
| `_DOC_DIRS` | `docs/`, `doc/`, `documentation/`, `wiki/`, `guides/`, `manual/` |

If a repository uses non-English directory names (e.g., `ドキュメント/`,
`Dokumentation/`), those files will not be counted as documentation.

**Mitigating factor:** extension-based detection (`.md`, `.rst`, `.adoc`) is
language-neutral and catches most documentation regardless of path naming.

> **Proposed Fix** `[LOGIC + CONFIG]`
>
> 1. **Reorder detection priority: extension first.**  The function already
>    checks extensions, but making this the *documented primary strategy*
>    clarifies that most docs are caught regardless of path language.
>    `.md`, `.rst`, `.adoc`, `.txt` (when under a doc dir) cover >95% of
>    documentation files in practice.
>
> 2. **Keep English basenames and dirs as-is.**  Names like `README`,
>    `LICENSE`, `CHANGELOG` are de-facto universal conventions even in
>    non-English repos (they are convention names, not natural language --
>    similar to how `Makefile` is used worldwide).
>
> 3. **Add `extra_doc_dirs` config knob** (YAML list):
>    ```yaml
>    extra_doc_dirs:
>      - "ドキュメント/"
>      - "Dokumentation/"
>      - "documentacao/"
>    ```
>    Load into `_DOC_DIRS` at startup.
>
> 4. **Add content-based fallback:**  If a file has no doc extension and is
>    not in a known doc dir, but its *sibling directory* contains >50% `.md`
>    files, treat it as documentation.  This catches docs in arbitrarily-
>    named directories.
>
> **Metric soundness:** Extension-based detection is the workhorse and is
> fully universal.  The config knob handles edge cases.  The content-based
> fallback adds a safety net.  No existing detection is removed.

---

## 2. Natural-Language Dependencies in Metric Plugins

### 2.1 ✅ FIXED — `review_comment_substance` -- Actionable Language Detection

**File:** `plugins/influence/review_comment_substance.py`

The `_score_comment()` function awards 10 points for "actionable language" using
a hardcoded English word list:
```python
actionable = [
    "should", "could", "consider", "suggest", "recommend",
    "instead", "rather", "prefer", "nit:", "todo:",
]
```

Non-English review comments with equivalent phrasing (e.g., Spanish `deberia`,
`considerar`; Japanese `すべき`, `検討`) score 0 on this dimension.

> **Proposed Fix** `[LOGIC + PROVIDER]`
>
> The other scoring dimensions (length 40pts, code blocks 25pts, questions
> 15pts, references 10pts) are already language-neutral and total 90/100.
> The actionable-language dimension contributes only 10 points.
>
> Replace the English word list with **language-neutral structural feedback
> signals** that indicate high-quality review comments in any language:
>
> ```python
> # Structured feedback dimension (replaces "actionable language", 10 pts)
> structured_signals = 0
> # Bullet / numbered lists (suggests organised feedback)
> if re.search(r'(?m)^\s*[-*]\s+\S', body) or re.search(r'(?m)^\s*\d+[.)]\s+\S', body):
>     structured_signals += 5
> # @mentions (suggests targeted, directed feedback)
> if re.search(r'@[a-zA-Z0-9_-]+', body):
>     structured_signals += 3
> # Inline code suggestion block (provider-neutral: any fenced code block
> # in a review comment indicates concrete code feedback)
> if re.search(r'```\w*\n', body):
>     structured_signals += 5
> # Conditional/suggestive punctuation (? already scored separately,
> # but colon-terminated labels like "nit:", "NOTE:", "提案:" are universal)
> if re.search(r'\b[A-Z]{2,}:', body) or re.search(r'[\u3000-\u9FFF]+:', body):
>     structured_signals += 2
> score += min(structured_signals, 10)
> ```
>
> **Provider consideration:** The previous draft checked for GitHub's
> ```` ```suggestion ```` syntax specifically.  This is a GitHub-only
> feature (GitLab has a similar "insert suggestion" button but the markdown
> differs; Bitbucket has no equivalent).  The updated check detects **any**
> fenced code block in a review comment, which works across all providers
> and captures the same intent: "the reviewer included concrete code."
>
> **Metric soundness:** The replacement signals (bullet lists, @mentions,
> code blocks, structured labels) indicate high-quality review feedback in
> *any* language and on *any* provider.  The dimension's weight (10/100) is
> preserved.  No net scoring loss for English users.

### 2.2 ✅ FIXED — `ai_assisted_pr_rate` -- AI Tool Signature Detection

**File:** `plugins/authored/ai_assisted_pr_rate.py`

**`_AI_TOOL_SIGNATURES`** -- English product names searched in PR titles, bodies,
and commit messages:
```
copilot  github copilot  co-authored-by: github copilot
cursor   cursor.sh       cursor generated
claude   claude code     claude.ai
chatgpt  gpt-4           gpt-3           openai
codewhisperer             jetbrains ai
tabnine                   cody
```

**`_HIGH_CONFIDENCE_PATTERNS`** -- Git trailer patterns:
```
generated[-_\s]with:   generated[-_\s]by:   ai[-_\s]generated[-_\s]with:
```

Also affects: **`ai_adoption_rate`**, **`ai_code_quality`**,
**`ai_phantom_ownership`** (all delegate to `_analyze_pr_for_ai`).

> **Proposed Fix** `[LOGIC]`
>
> This is largely **not a natural-language problem**.  "Copilot", "ChatGPT",
> "Claude", "Cursor" are brand names that are identical in every language.
> A Japanese developer writes `Co-authored-by: GitHub Copilot` -- the brand
> name is never translated.
>
> The trailer patterns (`Generated-with:`, `Generated-by:`) are emerging as
> a 2026 standard and are protocol-level syntax, not English.
>
> Changes needed:
>
> 1. **Keep tool-name detection as-is** -- brand names are universal.
>
> 2. **Add standalone `AI` / `ai` detection** as a generic signal:
>    ```python
>    r'\bAI\b'  # catches "AIが生成" (Japanese), "generado por AI" (Spanish)
>    ```
>    Weight this as `generic_ai` confidence (50).
>
> 3. **Document that the velocity-anomaly, entropy, and style-uniformity
>    signals are already fully language-neutral** -- they rely on line
>    counts, byte statistics, and AST analysis, not text.
>
> **Metric soundness:** No detection is removed.  The standalone `AI` pattern
> closes the gap for non-English descriptions that mention "AI" without
> naming a specific tool (which is common, since "AI" is a global
> abbreviation).  The structural signals (velocity, entropy, style) already
> work cross-language.

### 2.3 ✅ FIXED — `ai_assisted_pr_rate` -- TODO/FIXME Absence Signal

**File:** `plugins/authored/ai_assisted_pr_rate.py` -- `_detect_additional_ai_signals()`

Checks for absence of `"todo"` / `"fixme"` in added lines. While these are
near-universal programming conventions, teams writing comments in their native
language (e.g., `// 修正予定` instead of `// TODO`) will always trigger this
AI-suspicion signal, producing false positives.

> **Proposed Fix** `[LOGIC]`
>
> Downgrade from a **standalone signal** to a **corroborating signal** that
> only fires when at least one *primary* AI signal is already present.
>
> ```python
> # In _detect_additional_ai_signals():
> # OLD: unconditionally add evidence
> if total_code_lines > 100 and total_todo_fixme == 0:
>     evidence.append(...)
>
> # NEW: only add if caller indicates primary signals exist
> # Change signature:
> def _detect_additional_ai_signals(pr, ledger,
>                                   has_primary_signal: bool = False) -> list[dict]:
>     ...
>     if has_primary_signal and total_code_lines > 100 and total_todo_fixme == 0:
>         evidence.append(...)
> ```
>
> Also broaden the token check to catch any **all-caps annotation pattern**:
> ```python
> # Catches TODO, FIXME, HACK, NOTE, XXX, and non-English equivalents
> # that follow the convention (e.g., many Japanese devs still use "TODO:")
> if re.search(r'\b[A-Z]{2,}:', line):
>     total_todo_fixme += 1
> ```
>
> **Metric soundness:** The TODO/FIXME signal has the lowest confidence
> weight (35) and is the most prone to false positives on non-English teams.
> Making it corroborating-only means it can still strengthen a detection
> when other evidence exists, but it can never trigger a false AI
> classification on its own.  The broader `[A-Z]{2,}:` pattern catches
> structured annotations in any language that follows the convention.

### 2.4 ✅ FIXED — `pr_category_diversity` -- PR Classification by Title Keywords

**File:** `plugins/authored/pr_category_diversity.py`

The `_classify_pr()` function first tries conventional commit prefix matching,
then falls back to **English keyword scanning** on the PR title:

| Category | English keywords checked |
|---|---|
| `fix` | `fix`, `bug`, `hotfix`, `patch` |
| `feat` | `feat`, `add`, `implement`, `new` |
| `refactor` | `refactor`, `restructure`, `clean` |
| `docs` | `doc`, `readme`, `changelog` |
| `test` | `test`, `spec`, `coverage` |
| `ci` | `ci`, `pipeline`, `workflow`, `deploy` |
| `chore` | `chore`, `bump`, `upgrade`, `dependency` |
| `perf` | `perf`, `optimiz`, `speed`, `fast` |
| `style` | `style`, `format`, `lint` |

PRs with non-English titles will classify as `"other"`, producing a
falsely-low diversity score.

> **Proposed Fix** `[MODEL + LOGIC + PROVIDER]`
>
> Replace the English keyword fallback with two language-neutral strategies:
>
> **Strategy 1: PR labels (primary fallback after conventional-commit
> prefix).** With labels on the `PullRequest` model (see section 1.1):
>
> ```python
> _LABEL_TO_CATEGORY = {
>     "bug": "fix", "fix": "fix", "hotfix": "fix", "type:bug": "fix",
>     "feature": "feat", "enhancement": "feat", "type:feature": "feat",
>     "documentation": "docs", "docs": "docs",
>     "test": "test", "testing": "test",
>     "ci": "ci", "cd": "ci", "devops": "ci",
>     "dependencies": "chore", "chore": "chore",
>     "performance": "perf",
>     "refactor": "refactor",
>     "style": "style",
> }
>
> def _classify_by_labels(labels: list[str]) -> str | None:
>     for label in labels:
>         cat = _LABEL_TO_CATEGORY.get(label.lower())
>         if cat:
>             return cat
>     return None
> ```
>
> **Strategy 2: Structural diff analysis (secondary fallback).** What files
> were changed tells us more than what the title says:
>
> ```python
> def _classify_by_diff(files: list[FileRecord]) -> str:
>     filenames = [f.filename for f in files]
>     if all(is_test_file(fn) for fn in filenames):
>         return "test"
>     if all(is_documentation_file(fn) for fn in filenames):
>         return "docs"
>     if all(is_ci_config_file(fn) for fn in filenames):
>         return "ci"
>     if all(is_dependency_file(fn) for fn in filenames):
>         return "chore"
>     return "other"
> ```
>
> **Updated `_classify_pr()` priority chain:**
> 1. Conventional commit prefix (existing -- spec-defined, universal)
> 2. PR labels (new -- team-applied metadata, universal)
> 3. Structural diff analysis (new -- file-path-based, universal)
> 4. English keyword fallback (existing -- kept for backward compat)
>
> **Provider consideration:** The `is_ci_config_file()` helper must cover
> CI config paths across providers:
> ```python
> _CI_CONFIG_PATHS = (
>     ".github/workflows/",   # GitHub Actions
>     ".gitlab-ci.yml",       # GitLab CI
>     "bitbucket-pipelines.yml",  # Bitbucket Pipelines
>     "azure-pipelines.yml",  # Azure DevOps
>     ".circleci/",           # CircleCI
>     "Jenkinsfile",          # Jenkins
>     ".drone.yml",           # Drone
>     ".travis.yml",          # Travis CI
>     ".buildkite/",          # Buildkite
> )
> ```
> This list is CI-tool-specific (not provider-specific per se) and is
> already a convention-based heuristic similar to `is_test_file()`.
>
> **Metric soundness:** A Japanese team with labels `["バグ修正"]` mapped
> to `"fix"` via config, or with files only in `tests/`, now classifies
> correctly.  Diversity is no longer gated on English title text.

### 2.5 ✅ FIXED — `time_to_restore` -- Revert Commit Identification

**File:** `plugins/authored/time_to_restore.py`

```python
revert_pattern = re.compile(r'^Revert\s', re.IGNORECASE)
```

Uses the `Revert ` prefix to identify revert commits. This is Git's default
English revert message format. Non-English Git locale configurations (or
manually-written revert messages in another language) will not be detected,
causing the DORA MTTR proxy to report zero incidents when reverts exist.

> **Proposed Fix** `[LOGIC]`
>
> Delegate to the centralised `is_revert_indicator()` from `utils.py`
> (which will contain the structural SHA-based detection from section 1.2):
>
> ```python
> # Before
> revert_pattern = re.compile(r'^Revert\s', re.IGNORECASE)
> revert_commits = [c for c in all_commits if revert_pattern.match(c.message or "")]
>
> # After
> from impact.metrics.utils import is_revert_indicator
> revert_commits = [c for c in all_commits if is_revert_indicator(c.message or "")]
> ```
>
> This is a one-line change that immediately benefits from the enhanced
> detection in `is_revert_indicator()` (section 1.2).
>
> **Metric soundness:** MTTR detection now covers non-English reverts via
> the SHA-based layer.  The existing English pattern is still the first
> check for speed.  No change to the fix-commit detection or time
> calculation logic.

### 2.6 ✅ FIXED — `revert_introduction_rate` -- Revert SHA Extraction

**File:** `plugins/authored/revert_introduction_rate.py`

```python
re.search(r'reverts? commit ([a-f0-9]{7,40})', rc.message, re.IGNORECASE)
```

Parses the original commit SHA from the English revert message format (`This
reverts commit <sha>`). Non-English revert messages will fail SHA extraction,
preventing attribution to the original author.

> **Proposed Fix** `[LOGIC]`
>
> The SHA is the only data we actually need.  Instead of requiring English
> context around it, scan for *any* 40-char (or 7+ char) hex string in the
> message body, then verify it exists in the commit history:
>
> ```python
> _SHA_RE = re.compile(r'\b([a-f0-9]{7,40})\b')
>
> def _extract_reverted_sha(message: str, known_shas: set[str]) -> str | None:
>     """Extract the SHA of the commit being reverted."""
>     # Priority 1: English format (fast path)
>     m = re.search(r'reverts? commit ([a-f0-9]{7,40})', message, re.IGNORECASE)
>     if m:
>         return m.group(1)
>     # Priority 2: Any SHA in the message body that exists in history
>     body = message.split("\n", 1)[1] if "\n" in message else ""
>     for m in _SHA_RE.finditer(body):
>         candidate = m.group(1)
>         # Full match or prefix match against known SHAs
>         if candidate in known_shas or any(s.startswith(candidate) for s in known_shas):
>             return candidate
>     return None
> ```
>
> In `revert_introduction_rate.run()`:
> ```python
> known_shas = {c.sha for c in all_commits}
> for rc in revert_commits:
>     original_sha = _extract_reverted_sha(rc.message, known_shas)
>     if not original_sha:
>         continue
>     ...
> ```
>
> **Metric soundness:** The English path is preserved for speed and backward
> compatibility.  The structural fallback finds the SHA regardless of the
> surrounding text language.  Verification against `known_shas` eliminates
> false positives (random hex strings that happen to be 40 chars).

### 2.7 ✅ FIXED — `co_author_contribution_rate` -- Co-Author Trailer

**File:** `plugins/mixed/co_author_contribution_rate.py`

```python
CO_AUTHOR_PATTERN = re.compile(r'Co-authored-by:\s*(.+)', re.IGNORECASE)
```

The `Co-authored-by:` trailer is a Git standard and is not typically localized.
However, teams using non-standard collaboration trailers (e.g., `Pair-programmed-with:`) will not be detected.

> **Proposed Fix** `[LOGIC]`
>
> `Co-authored-by:` is a Git/GitHub/GitLab standard trailer -- it is
> protocol-level syntax, not English prose, and is never localised.  This
> is a **platform assumption**, not a language dependency.
>
> For robustness, broaden the pattern to catch related collaboration
> trailers:
>
> ```python
> # Catches Co-authored-by:, Pair-programmed-with:, Paired-with:,
> # Helped-by:, Mentored-by:, Reviewed-by: (kernel style), etc.
> _COLLAB_TRAILER_RE = re.compile(
>     r'^(?:Co-authored-by|Pair(?:ed)?-(?:programmed-)?with|'
>     r'Helped-by|Mentored-by|Reviewed-by):\s*(.+)',
>     re.IGNORECASE | re.MULTILINE,
> )
> ```
>
> Alternatively, catch *any* `X-by:` or `X-with:` trailer in the last
> paragraph of the commit message (the trailer block per Git convention):
>
> ```python
> _ANY_COLLAB_TRAILER_RE = re.compile(
>     r'^[A-Za-z][\w-]*-(?:by|with):\s*(.+)',
>     re.IGNORECASE | re.MULTILINE,
> )
> ```
>
> **Metric soundness:** The broader pattern catches pair-programming and
> mentorship trailers that the current regex misses.  Since trailers are
> parsed from the *last paragraph* of a commit message (Git convention),
> false-positive risk from message body text is minimal.

---

## 3. Programming-Language Coverage Limits

These components use programming-language-specific syntax patterns. They
**gracefully degrade** (return empty results / fall back to `"other"`) for
unsupported languages, but metrics that depend on them will produce **less
accurate** results for codebases outside the covered languages.

### 3.1 ✅ FIXED — tree-sitter AST Analysis -- 13 Languages

**File:** `utils.py` -- `_LANGUAGE_REGISTRY`, `parse_functions()`

Used by: **`ai_assisted_pr_rate`** (style uniformity detection)

| Extension | Language |
|---|---|
| `.py` | Python |
| `.js`, `.jsx` | JavaScript |
| `.ts`, `.tsx` | TypeScript |
| `.go` | Go |
| `.rs` | Rust |
| `.java` | Java |

**Not covered:** C, C++, C#, Ruby, PHP, Kotlin, Swift, Scala, Elixir, Dart,
Haskell, Lua, R, and many others. For these languages, `parse_functions()`
returns `[]` and the style-uniformity AI detection signal will not fire.

> **Proposed Fix** `[LOGIC]`
>
> Each new language requires only (a) a pip dependency and (b) a one-line
> entry in `_LANGUAGE_REGISTRY`.  Add the most impactful languages:
>
> ```python
> # New entries in _LANGUAGE_REGISTRY:
> ".c":     ("tree_sitter_c", "language"),
> ".h":     ("tree_sitter_c", "language"),
> ".cc":    ("tree_sitter_cpp", "language"),
> ".cpp":   ("tree_sitter_cpp", "language"),
> ".cxx":   ("tree_sitter_cpp", "language"),
> ".hpp":   ("tree_sitter_cpp", "language"),
> ".cs":    ("tree_sitter_c_sharp", "language"),
> ".rb":    ("tree_sitter_ruby", "language"),
> ".php":   ("tree_sitter_php", "language"),
> ".kt":    ("tree_sitter_kotlin", "language"),
> ".kts":   ("tree_sitter_kotlin", "language"),
> ".swift": ("tree_sitter_swift", "language"),
> ".scala": ("tree_sitter_scala", "language"),
> ```
>
> Add corresponding pip dependencies to `pyproject.toml`:
> ```toml
> tree-sitter-c = ">=0.21"
> tree-sitter-cpp = ">=0.21"
> tree-sitter-c-sharp = ">=0.21"
> tree-sitter-ruby = ">=0.21"
> tree-sitter-php = ">=0.22"
> tree-sitter-kotlin = ">=0.21"
> tree-sitter-swift = ">=0.21"
> tree-sitter-scala = ">=0.21"
> ```
>
> Also update `_FUNCTION_NODE_TYPES` and `_STATEMENT_TYPES` to include
> node types for the new languages (e.g., `function_definition` for C/C++,
> `method_definition` for Ruby, `function_definition` for PHP).
>
> **Metric soundness:** Graceful degradation is preserved -- `_get_language()`
> returns `None` when a grammar is not installed, so these are optional
> dependencies.  Adding them expands style-uniformity detection to cover
> ~90% of GitHub repositories by language.

### 3.2 ✅ FIXED — Structural Diff Classification -- `_STRUCT_PATTERNS`

**File:** `utils.py` -- `classify_diff_structure()`

Currently not consumed by any metric plugin (available as a utility). Patterns
cover function/class/import/test syntax for:
- Python (`def`, `class`, `import`, `from ... import`)
- JavaScript/TypeScript (`function`, `const ... =`, `describe(`, `it(`)
- Java/Kotlin (`@Test`)
- Rust (`fn`, `#[test]`, `mod tests`)
- Go (`func Test...`)

Lines in languages not covered classify as `"other"`.

> **Proposed Fix** `[LOGIC -- low priority]`
>
> Not currently consumed by any metric plugin, so this is informational.
> When/if a metric starts consuming `classify_diff_structure()`, expand
> patterns for additional languages.  The existing patterns already use
> cross-language keywords (`class`, `import`, `if`) that happen to cover
> many languages beyond those explicitly listed.

### 3.3 ✅ FIXED — Comment Syntax Detection

**File:** `plugins/authored/ai_assisted_pr_rate.py` -- `_detect_additional_ai_signals()`

Detects single-line comment prefixes for comment-density scoring:
```python
stripped.startswith("#") or stripped.startswith("//") or
stripped.startswith("/*") or stripped.startswith("*")
```

Covers: Python, Ruby, Perl (`#`); C/C++/Java/JS/TS/Go/Rust/Kotlin/Swift (`//`,
`/*`, `*`).

**Not covered:** SQL/Haskell/Lua (`--`), HTML/XML (`<!--`), MATLAB (`%`),
Lisp (`;`). For these languages, comment density will be under-counted,
potentially triggering a false `low_comment_density` AI signal.

> **Proposed Fix** `[LOGIC]`
>
> Expand the prefix list to cover all major comment syntaxes:
>
> ```python
> _COMMENT_PREFIXES = ("#", "//", "/*", "*", "--", "<!--", "%", ";", "(*")
>
> # In _detect_additional_ai_signals():
> if any(stripped.startswith(p) for p in _COMMENT_PREFIXES):
>     total_comments += 1
> ```
>
> This adds:
> - `--` : SQL, Haskell, Lua, Ada, PL/SQL
> - `<!--` : HTML, XML, SVG, Markdown
> - `%` : MATLAB, Erlang, LaTeX, Prolog
> - `;` : Lisp, Scheme, Clojure, Assembly
> - `(*` : OCaml, Pascal, F#
>
> **Metric soundness:** Comment detection is a heuristic -- the goal is to
> estimate whether a PR has "very low comment density".  Adding more
> prefixes reduces false positives (incorrectly low density) for the
> listed languages without affecting detection for already-covered
> languages.

### 3.4 ✅ FIXED — Constructor Name Detection

**File:** `utils.py` -- `_classify_function_kind()`

Identifies constructors by name:
```python
if name in ("__init__", "constructor", "init"):
    return "constructor"
```

Covers Python (`__init__`), JavaScript/TypeScript (`constructor`), Swift (`init`).

**Not covered:** Go (`New*()`), Rust (`new()`), Java (class-name constructor),
C++ (class-name constructor), Kotlin (`init {}`).

> **Proposed Fix** `[LOGIC]`
>
> Use a combination of AST node types (primary) and name patterns (fallback):
>
> ```python
> def _classify_function_kind(node: Any) -> str:
>     # AST node type is the most reliable signal
>     if node.type == "constructor_declaration":     # Java, Kotlin
>         return "constructor"
>     if node.type in ("method_definition", "method_declaration"):
>         name_node = node.child_by_field_name("name")
>         if name_node and name_node.text:
>             name = name_node.text.decode("utf-8", errors="replace")
>             # Explicit constructor names (language-specific but exhaustive)
>             if name in ("__init__", "constructor", "init", "new"):
>                 return "constructor"
>             # Go convention: NewFoo() is a constructor
>             if re.match(r'^New[A-Z]', name):
>                 return "constructor"
>         return "method"
>     return "function"
> ```
>
> Changes from current code:
> - Added `"new"` to the name set (Rust constructors by convention)
> - Added Go `New[A-Z]` pattern (Go constructor convention)
> - `constructor_declaration` node type was already handled; confirmed
>   correct for Java/Kotlin
>
> **Metric soundness:** Constructor detection is used for trivial-function
> classification.  The additions cover Go and Rust conventions.  Java/C++
> class-name constructors are already handled by the `constructor_declaration`
> AST node type.  No regressions for existing languages.

---

## 4. Metrics Unaffected by Language

The following metrics are fully language-independent -- they rely solely on
timestamps, counts, structural graph analysis, or diff line numbers, and will
produce identical results regardless of human language or programming language:

| Category | Metrics |
|---|---|
| **Time-based** | `cycle_time`, `merge_delay`, `time_to_first_review`, `slow_review_response`, `coding_time_to_pr`, `review_turnaround_time`, `unblock_time` |
| **Count-based** | `pr_throughput`, `delivery_volume`, `net_code_contribution`, `pr_size_distribution`, `wip_load`, `reviews_given`, `pr_merge_rate`, `pr_merge_effectiveness` |
| **Activity-based** | `active_weeks`, `coding_days`, `burstiness`, `off_hours_activity_rate` |
| **Structural** | `code_churn_rate`, `rework_rate`, `code_survival`, `change_proximity`, `sum_of_coupling`, `temporal_logical_coupling`, `entity_fragmentation`, `hotspot_detection`, `bus_factor`, `knowledge_islands` |
| **Workflow-based** | `review_iterations`, `first_time_approval_rate`, `flow_efficiency`, `self_merge_rate`, `abandoned_pr_rate`, `blocking_comment_rate`, `first_reviewer_rate`, `approval_to_merge_ratio`, `review_demand`, `review_leverage`, `review_breadth`, `mentorship_signal`, `change_inducing_review_rate`, `inline_comment_density` |
| **Whitespace proxy** | `complexity_trend` (indentation-based, fully language-neutral) |
| **Path-based** | `module_area_breadth`, `dependency_change_rate`, `test_file_ratio` (path heuristics like `/test/`, `.spec.` are universal) |

---

## 5. Affected Metrics -- Full Reference

| Metric Slug | Plugin File | Dependency Type | Impact | Fix Section |
|---|---|---|---|---|
| `bug_fix_focus_rate` | `authored/bug_fix_focus_rate.py` | English keywords in `is_bug_fix_indicator()` | Non-English bug-fix PRs/commits not detected | 1.1 |
| `revert_introduction_rate` | `authored/revert_introduction_rate.py` | English `is_revert_indicator()` + `reverts? commit` regex | Non-English reverts invisible | 1.2, 2.6 |
| `conventional_commit_rate` | `authored/conventional_commit_rate.py` | English type list in `is_conventional_commit()` | Non-English prefixes get 0% strict rate | 1.3 |
| `commit_message_mining` | `authored/commit_message_mining.py` | English `_DEFECT_PATTERNS` via `classify_defect_commit()` | Non-English defect messages missed | 1.4 |
| `pr_body_quality_score` | `authored/pr_body_quality.py` | English refs in `compute_pr_body_quality()` | Missing bonus points for non-English refs | 1.5 |
| `pr_category_diversity` | `authored/pr_category_diversity.py` | English fallback keywords in `_classify_pr()` | Non-English titles classify as "other" | 2.4 |
| `time_to_restore` | `authored/time_to_restore.py` | English `^Revert\s` pattern | Non-English revert messages missed | 2.5 |
| `review_comment_substance` | `influence/review_comment_substance.py` | English `actionable` word list | Non-English actionable comments score 0 | 2.1 |
| `ai_assisted_pr_rate` | `authored/ai_assisted_pr_rate.py` | Brand names + TODO/FIXME + comment syntax | False positives (TODO/FIXME absence) | 2.2, 2.3, 3.3 |
| `ai_adoption_rate` | `authored/ai_adoption_rate.py` | Delegates to `ai_assisted_pr_rate` detection | Same as `ai_assisted_pr_rate` | 2.2 |
| `ai_code_quality` | `authored/ai_code_quality.py` | Delegates to `ai_assisted_pr_rate` detection | AI vs human split may be inaccurate | 2.2 |
| `ai_phantom_ownership` | `authored/ai_phantom_ownership.py` | Delegates to `ai_assisted_pr_rate` detection | Phantom files may be mis-classified | 2.2 |
| `co_author_contribution_rate` | `mixed/co_author_contribution_rate.py` | `Co-authored-by:` trailer pattern | Non-standard collab trailers missed | 2.7 |
| `follow_up_commit_rate` | `authored/follow_up_commit_rate.py` | `startswith("merge ")` filter | Localized merge messages leak through | 1.6 |
| `documentation_touch_rate` | `authored/documentation_touch_rate.py` | English `_DOC_BASENAMES` / `_DOC_DIRS` | Non-English doc paths not detected | 1.8 |
| `ai_suggestion_acceptance` | `authored/ai_suggestion_acceptance.py` | GitHub ```` ```suggestion ```` syntax + `[bot]` suffix | Non-functional on non-GitHub providers | 7.2 |
| *(all influence metrics)* | *(multiple)* | `is_bot_user()` GitHub-only layers | Bots not filtered on non-GitHub providers | 7.1 |
| `flow_efficiency` | `authored/flow_efficiency.py` | `WorkflowRunRecord` GitHub Actions naming | Cosmetic; fields are provider-neutral | 7.3 |

---

## 6. Implementation Priority

Fixes are ordered by **impact x effort** ratio:

### Phase 1: Structural Fixes (highest impact, no config needed)

| Priority | Fix | Sections | Tag | Rationale |
|---|---|---|---|---|
| **P0** | Add `parent_count` to `Commit` model; replace all `startswith("merge ")` | 1.6 | MODEL+LOGIC | Eliminates 4 English-dependent call sites with one structural field. Zero false positives. |
| **P1** | Centralise `is_revert_indicator()` with SHA-based detection | 1.2, 2.5, 2.6 | LOGIC | Fixes 3 metrics (`revert_introduction_rate`, `time_to_restore`, and SHA extraction) in one shot. |
| **P2** | Replace `compute_pr_body_quality()` English patterns with universal `#\d+` | 1.5 | LOGIC | One-function change, immediate improvement for non-English PR bodies. |
| **P3** | Expand comment syntax prefixes | 3.3 | LOGIC | One-line change, eliminates false AI positives for SQL/Haskell/Lua/HTML teams. |
| **P4** | Downgrade TODO/FIXME to corroborating signal | 2.3 | LOGIC | One-condition change, eliminates false AI positives for non-English teams. |
| **P5** | Replace actionable-language word list with structural signals | 2.1 | LOGIC | Self-contained change in one function, 10/100 point dimension. |

### Phase 2: Model Enrichment (high impact, requires adapter change)

| Priority | Fix | Sections | Tag | Rationale |
|---|---|---|---|---|
| **P6** | Add `labels: list[str]` to `PullRequest` model | 1.1, 1.4, 2.4 | MODEL | Unlocks label-based detection for bug-fix, defect mining, and PR category diversity. One adapter change enables three metric fixes. |
| **P7** | Rewrite `is_bug_fix_indicator()` with label-first multi-signal | 1.1 | LOGIC | Depends on P6. |
| **P8** | Rewrite `_classify_pr()` with label + diff-structure fallback | 2.4 | LOGIC | Depends on P6. |
| **P9** | Rewrite `classify_defect_commit()` with label-first detection | 1.4 | LOGIC | Depends on P6. |

### Phase 3: Coverage Expansion (moderate impact, additive)

| Priority | Fix | Sections | Tag | Rationale |
|---|---|---|---|---|
| **P10** | Add `is_structured_commit()` broad check | 1.3 | LOGIC | Rewards non-English structured commit discipline. |
| **P11** | Expand tree-sitter to 13+ languages | 3.1 | LOGIC | pip deps + one-line entries. |
| **P12** | Expand constructor name detection | 3.4 | LOGIC | Small change, Go + Rust coverage. |
| **P13** | Broaden `Co-authored-by:` to `_COLLAB_TRAILER_RE` | 2.7 | LOGIC | Small regex change. |
| **P14** | Add `extra_doc_dirs` / `extra_generated_markers` config | 1.7, 1.8 | CONFIG | Org-level customisation for edge cases. |
| **P15** | Add standalone `\bAI\b` detection signal | 2.2 | LOGIC | Small pattern addition. |
| **P16** ✅ | Refactor `is_bot_user()` for multi-provider bot detection | 7.1 | LOGIC+PROVIDER | Enables influence metrics on non-GitHub providers. |
| **P17** ✅ | Abstract `ai_suggestion_acceptance` from GitHub suggestion syntax | 7.2 | LOGIC+PROVIDER | Currently non-functional on non-GitHub providers. |
| **P18** | Ensure cross-ref patterns cover all providers | 1.5 | LOGIC+PROVIDER | `#\d+` misses GitLab `!NNN` and Azure DevOps `AB#NNN`. |

---

## 7. Provider-Specific Dependencies in Existing Code

Beyond the natural-language and programming-language issues in sections 1-3,
the metrics layer contains **provider-specific assumptions** that will break or
produce incorrect results when DevRank adds GitLab, Bitbucket, or other adapters.

The adapter architecture (`impact/adapters/base.py`) correctly isolates
raw-API parsing, but some logic in the metrics layer leaks provider details.

### 7.1 ✅ FIXED `is_bot_user()` -- Bot Detection (GitHub-Only)

**File:** `impact/metrics/utils.py` (lines 12-45)

Used by: **every influence metric** that filters out bot reviewers/commenters,
plus `ai_phantom_ownership`, `ai_suggestion_acceptance`, `is_immediate_approval`.

All three detection layers are GitHub-specific:

| Layer | Pattern | GitHub | GitLab | Bitbucket | Azure DevOps |
|---|---|---|---|---|---|
| 1 | `user.type == UserType.BOT` | API `type` field | No `type` field; `bot: true` flag | No bot concept | No bot concept |
| 2 | `login.endswith("[bot]")` | GitHub Apps naming | No `[bot]` suffix; bots have normal usernames | No convention | No convention |
| 3 | `node_id.startswith("BOT_")` | GraphQL global ID | No `node_id` | No `node_id` | No `node_id` |

On a GitLab-sourced bundle, Layer 1 would mis-classify all bots as humans
(the `type` field would not be set to `Bot`), causing bot reviews to inflate
influence metrics.

> **Proposed Fix** `[MODEL + LOGIC + PROVIDER]`
>
> 1. **Add `is_bot: bool = False` to `User`** in `impact/domain/models.py`.
>    This is a **canonical** boolean that each adapter sets based on its
>    own provider logic:
>
>    | Adapter | How to determine `is_bot` |
>    |---|---|
>    | GitHub | `type == "Bot"` OR `login.endswith("[bot]")` OR `node_id.startswith("BOT_")` |
>    | GitLab | API field `bot: true` (available on User objects) |
>    | Bitbucket | Workspace-managed "app users" (check `type == "app"`) |
>    | Azure DevOps | Service principal identities (check `uniqueName` pattern) |
>    | Gitea | `login.endswith("[bot]")` (same convention as GitHub) |
>
> 2. **Simplify `is_bot_user()` in utils.py** to rely solely on the
>    canonical field:
>    ```python
>    def is_bot_user(user) -> bool:
>        return getattr(user, "is_bot", False)
>    ```
>
> 3. **Keep the current multi-layer logic in the GitHub adapter only** as
>    the source for `is_bot`.  This moves provider-specific knowledge out
>    of the metrics layer and into the adapter where it belongs.
>
> **Impact:** All 10+ metrics that call `is_bot_user()` become
> provider-independent in one shot.
>
> **Implementation (completed):**
> - Added `is_bot: bool = False` to `User` in `impact/domain/models.py`.
> - Added `GitHubAdapter._is_github_bot()` static method implementing the
>   three-layer GitHub-specific detection (type/suffix/node_id).
> - `ensure_user()` in the adapter now sets `is_bot` via `_is_github_bot()`.
> - Simplified `is_bot_user()` in `utils.py` to `getattr(user, "is_bot", False)`.
> - Updated `make_user()` factory in conftest to accept `is_bot` kwarg.
> - Added `TestIsGitHubBot` tests in `test_github_adapter.py` for the adapter logic.
> - Updated `TestIsBotUser` in `test_ai_phantom_ownership.py` for canonical field.
> - 920/920 tests pass.

### 7.2 ✅ FIXED `ai_suggestion_acceptance` -- GitHub Suggestion Blocks

**File:** `plugins/authored/ai_suggestion_acceptance.py`

This metric detects ```` ```suggestion ```` code blocks in review comments from
AI bots.  Both components are GitHub-specific:

| Component | GitHub | GitLab | Bitbucket |
|---|---|---|---|
| ```` ```suggestion ```` syntax | Native feature | Different syntax (line-level suggestions via API) | No equivalent |
| `[bot]` suffix matching | GitHub Apps convention | Not applicable | Not applicable |

On a non-GitHub provider, this metric would always return "no AI suggestions
found" -- a silent false negative.

> **Proposed Fix** `[MODEL + LOGIC + PROVIDER]`
>
> 1. **Abstract the suggestion concept into the domain model.**  Add an
>    optional `has_code_suggestion: bool = False` field to `CommentRecord`.
>    Each adapter determines this from its own API:
>
>    | Adapter | How to detect suggestions |
>    |---|---|
>    | GitHub | ```` ```suggestion ```` block in `body` |
>    | GitLab | Comment has `type: "DiffNote"` with `suggestions` array |
>    | Bitbucket | No native equivalent; field stays `False` |
>
> 2. **Move bot detection to canonical `is_bot` field** (see section 7.1).
>
> 3. **The metric checks `comment.has_code_suggestion`** instead of
>    parsing ```` ```suggestion ```` syntax directly.  On providers without
>    native suggestions, the metric gracefully reports `no_data`.
>
> **Metric soundness:** The metric continues to work on GitHub exactly as
> today.  On GitLab it gains suggestion detection via the adapter.  On
> providers without suggestions, it returns `no_data` rather than a
> misleading 0%.
>
> **Implementation (completed):**
> - Added `has_code_suggestion: bool = False` to `CommentRecord` in models.
> - Added `GitHubAdapter._has_github_suggestion()` detecting ```` ```suggestion ````
>   blocks via regex; called in the review comments section of `parse_dump()`.
> - Updated `_is_ai_bot()` to accept User objects (with string backward-compat)
>   and use `is_bot_user()` as the broad catch-all layer.
> - Metric loop now filters on `c.has_code_suggestion` first, then `_is_ai_bot(c.user)`.
> - Added `TestHasGitHubSuggestion` tests in `test_github_adapter.py`.
> - Updated MagicMock tests in `test_ai_suggestion_acceptance.py` with
>   explicit `has_code_suggestion=True` and `user.is_bot=True`.
> - `make_comment()` factory in conftest now accepts `has_code_suggestion` kwarg.
> - 920/920 tests pass.

### 7.3 ✅ FIXED `WorkflowRunRecord` -- GitHub Actions Assumption

**File:** `impact/domain/models.py` (line 176)

Used by: **`flow_efficiency`** (CI wait time component)

The `WorkflowRunRecord` model is explicitly documented as "GitHub Actions
workflow run" and its docstring references `GET /repos/{owner}/{repo}/actions/runs`.

> **Proposed Fix** `[MODEL + PROVIDER]`
>
> 1. **Rename to `CIRunRecord`** and make the model provider-neutral:
>    ```python
>    class CIRunRecord(BaseModel):
>        """CI pipeline run (provider-neutral).
>        Populated from GitHub Actions, GitLab CI, Bitbucket Pipelines, etc.
>        """
>        id: int
>        name: str | None = None
>        head_sha: str
>        event: str | None = None
>        status: str | None = None       # queued, running, completed
>        conclusion: str | None = None    # success, failure, cancelled
>        created_at: datetime
>        updated_at: datetime | None = None
>        run_started_at: datetime | None = None
>        pull_request_number: int | None = None
>        duration_seconds: int | None = None
>    ```
>
> 2. **Each adapter maps its CI system into this model:**
>
>    | Adapter | Source |
>    |---|---|
>    | GitHub | `GET /repos/{o}/{r}/actions/runs` |
>    | GitLab | `GET /projects/{id}/pipelines` |
>    | Bitbucket | `GET /repositories/{ws}/{slug}/pipelines` |
>    | Azure DevOps | `GET /{org}/{project}/_apis/build/builds` |
>
> 3. Rename `CanonicalBundle.workflow_runs` to `ci_runs`.
>
> **Metric soundness:** No metric logic changes.  The field names and
> semantics are already provider-neutral; only the model name and docstring
> leak the GitHub assumption.
>
> **Implementation (completed):**
> - Renamed `WorkflowRunRecord` → `CIRunRecord` in `impact/domain/models.py`.
> - Updated docstring to be provider-neutral (GitHub Actions, GitLab CI, etc.).
> - Renamed `CanonicalBundle.workflow_runs` → `ci_runs`.
> - Added backward-compatible alias `WorkflowRunRecord = CIRunRecord` at module level.
> - Updated `test_new_metrics_gaps.py`: imports, test name, assertion.
> - GitHub fetcher (`fetch_workflow_runs`) retains its name — it's correctly
>   scoped to the GitHub-specific provider layer.
> - 920/920 tests pass.

### 7.4 ✅ FIXED `ReleaseRecord` / `DeploymentRecord` -- GitHub API References

**Files:** `impact/domain/models.py` (lines 143, 161)

Used by: **`time_to_restore`** (DORA MTTR), future DORA metrics

The docstrings reference GitHub-specific API endpoints.  The model fields
themselves are already provider-neutral.

> **Proposed Fix** `[MODEL]`
>
> Update docstrings to be provider-neutral.  No structural changes needed:
> ```python
> class ReleaseRecord(BaseModel):
>     """Release / tag event (DORA Deployment Frequency data source).
>     Populated from provider release APIs (GitHub Releases, GitLab
>     Releases, Bitbucket tags, etc.).
>     """
> ```
>
> Each adapter maps its release concept into these fields.
>
> **Implementation (completed):**
> - Updated `ReleaseRecord` docstring: "Release / tag event (DORA Deployment
>   Frequency data source). Populated from provider release APIs (GitHub
>   Releases, GitLab Releases, Bitbucket tags, etc.)."
> - Updated `DeploymentRecord` docstring: "Deployment event (DORA Deployment
>   Frequency data source). Populated from provider deployment APIs (GitHub
>   Deployments, GitLab Environments, Bitbucket Deployments, Azure DevOps
>   Releases, etc.)."
> - No structural changes — fields were already provider-neutral.

### 7.5 Domain Terminology: "Pull Request" vs "Merge Request"

The canonical model uses `PullRequest` (GitHub terminology).  GitLab uses
"Merge Request" (MR).  Bitbucket uses "Pull Request".  Azure DevOps uses
"Pull Request".

> **Proposed Fix** -- **No change needed.**
>
> "Pull request" is the more common industry term (used by 3 of 4 major
> providers).  GitLab's "merge request" is semantically identical.  The
> canonical model's naming is a reasonable abstraction.  Adapters simply
> map MRs to `PullRequest` objects.  Metrics never display the term to
> users; report templates can use provider-appropriate terminology.

### 7.6 Cross-Provider Compatibility Matrix

Summary of how proposed canonical model fields map to each provider:

| Canonical Field | GitHub | GitLab | Bitbucket | Azure DevOps | Git (raw) |
|---|---|---|---|---|---|
| `PullRequest.labels` | `labels[].name` | `labels[]` | workspace labels | work-item tags | N/A |
| `Commit.parent_count` | `parents[]` | `parent_ids[]` | `parents[]` | `parents[]` | `git cat-file -p` |
| `User.is_bot` | type/suffix/node_id | `bot: true` | `type == "app"` | service principal | N/A |
| `CommentRecord.has_code_suggestion` | ```` ```suggestion ```` | `suggestions[]` | N/A | N/A | N/A |
| Cross-references in text | `#123` | `#123`, `!123` | `#123` | `AB#123` | N/A |
| CI runs | Actions API | Pipelines API | Pipelines API | Builds API | N/A |
| Releases | Releases API | Releases API | Tags API | Releases API | `git tag` |
| Deployments | Deployments API | Environments API | Deployments API | Releases API | N/A |
| Timeline events | Timeline API | Events API (partial) | Activity API (partial) | N/A | N/A |
| Review states | APPROVED/CHANGES_REQUESTED/COMMENTED | approved/unapproved | approved/needs_work | approved/rejected | N/A |

Items marked N/A will produce empty data and metrics will return `no_data`.

---

## 8. Design Principles for Provider Independence

1. **Canonical model is the contract.**  Metrics import only from
   `impact/domain/models.py`.  They never import from `impact/adapters/`
   or `impact/providers/`.

2. **Adapters are the translation layer.**  All provider-specific parsing,
   field mapping, and API knowledge lives in the adapter.  The adapter's
   job is to populate the canonical model completely and correctly.

3. **Graceful degradation over hard failure.**  If a provider cannot supply
   a field (e.g., Bitbucket has no code-suggestion feature), the canonical
   field defaults to a safe value (`None`, `False`, `[]`) and the metric
   returns `no_data` rather than crashing or producing misleading results.

4. **Convention-based heuristics are acceptable.**  Path-based detection
   (`is_test_file()`, `is_ci_config_file()`, `is_documentation_file()`)
   relies on cross-ecosystem conventions (e.g., `/test/`, `Jenkinsfile`,
   `.md`).  These are not provider-specific -- they are tool/framework
   conventions that exist regardless of where the repo is hosted.

5. **Git is the universal substrate.**  Properties of the Git DAG (parent
   count, SHA references, commit messages, trailers) are available on
   every provider because every provider hosts Git repositories.  These
   are safe to depend on.

6. **Text-matching must be configurable.**  When a metric matches patterns
   in free text (cross-references, keywords, labels), the pattern set must
   be configurable or expressed as a union across known providers, not
   hardcoded to one provider's syntax.

---

## 9. Beyond the Metrics Pipeline — Broader Codebase Dependencies

The sections above (1–8) focus on the metrics layer.  This section catalogues
language-dependent and provider-dependent patterns in the **rest of the
codebase**: the data-fetching pipeline, report generation, ingestion, CLI,
API layer, persistence, and configuration.

### 9.1 ✅ FIXED Report Generation — Hardcoded English Text

**Files:**
- `impact/templates/pdf_report.py`
- `impact/scripts/generate_report.py`

The PDF report template and CLI report output contain dozens of hardcoded
English strings that will be opaque to non-English-speaking stakeholders
reviewing the report.

| Location | English string | Line(s) |
|---|---|---|
| `pdf_report.py` — title | `"Engineering Impact Report"` | 1013, 1293 |
| `pdf_report.py` — `_rating_label()` | `"Excellent"`, `"Good"`, `"Neutral"`, `"Needs Work"`, `"Informational"`, `"Unknown"`, `"Insufficient Data"` | 628–650 |
| `pdf_report.py` — summary headers | `"Top Metrics"`, `"Low Metrics"` | 1194–1195 |
| `pdf_report.py` — empty placeholder | `"None in this period"` | 1173 |
| `pdf_report.py` — boolean formatting | `"Yes"` / `"No"` | 977 |
| `pdf_report.py` — score label | `"Overall Score ({n} groups)"` | 1034 |
| `pdf_report.py` — pluralisation | `"metric" + "s"` (English plural) | 1245 |
| `pdf_report.py` — footer | `"Generated {date} \| Page {n}"` | 1285 |
| `pdf_report.py` — stat labels | `"Avg"`, `"Median"`, `"P75"`, `"Rate"`, etc. in `METRIC_DISPLAY_CONFIG` | 91–563 |
| `generate_report.py` — CLI banner | `"🚀 DevRank Impact Report"`, `"👤 User:"`, `"📅 Period:"`, `"📊 Data Summary:"`, etc. | 249–263 |
| `generate_report.py` — metric output | `"🏆 Rating:"`, `"💡 Summary:"`, `"📈 Details:"` | 302–317 |

> **Proposed Fix** `[CONFIG]`
>
> 1. **Extract all user-facing strings into a locale file.**  Create
>    `impact/templates/locales/en.yaml` with keys like:
>    ```yaml
>    report_title: "Engineering Impact Report"
>    rating_excellent: "Excellent"
>    rating_good: "Good"
>    rating_neutral: "Neutral"
>    rating_bad: "Needs Work"
>    top_metrics: "Top Metrics"
>    low_metrics: "Low Metrics"
>    none_in_period: "None in this period"
>    overall_score: "Overall Score ({count} groups)"
>    ```
>    The report generator loads the locale file based on a `--locale`
>    parameter (default: `en`).
>
> 2. **For the CLI output (`generate_report.py`)**:  Either reuse the same
>    locale file, or treat CLI output as developer-facing (English is
>    acceptable for developer tools — the locale concern is primarily for
>    the *exported* PDF report consumed by non-technical stakeholders).
>
> 3. **The stat labels** (`"Avg"`, `"Median"`, `"P75"`, etc.) in
>    `METRIC_DISPLAY_CONFIG` are borderline — they are statistical
>    abbreviations recognisable across many languages.  These can be
>    localised as a Phase 2 enhancement.
>
> **Priority:** Medium.  English reports are acceptable for most global
> engineering teams today.  Localisation becomes critical when DevRank
> reports are consumed by non-technical, non-English management audiences.

> **Implementation (completed):**
> - Created `impact/templates/locales/en.yaml` with 25 locale keys covering
>   all user-facing PDF strings (title, ratings, footer, header, boolean labels,
>   metric count, executive summary labels).
> - Added `_load_locale(locale)` and `_t(strings, key, default, **kwargs)` helpers
>   to `pdf_report.py` for locale-aware string resolution with fallback.
> - Threaded `locale` parameter through `generate_candidate_pdf()`,
>   `_build_executive_summary()`, `_build_category_section()`, and
>   `_make_header_footer()`.
> - Added `--locale` CLI flag to `generate_report.py`.
> - All hardcoded English strings replaced with `_t()` calls.

### 9.2 ✅ FIXED Report Generation — US Letter Page Size

**File:** `impact/templates/pdf_report.py` (line 15, 1310)

```python
from reportlab.lib.pagesizes import letter
doc = SimpleDocTemplate(output_path, pagesize=letter, ...)
```

The PDF uses US Letter (8.5 × 11 in), which is the standard in the US and
Canada only.  Most of the world uses **A4** (210 × 297 mm).  Printing a
US Letter PDF on A4 paper clips margins or scales down.

> **Proposed Fix** `[CONFIG]`
>
> Accept an optional `page_size` parameter (default `letter`, accept `a4`):
> ```python
> from reportlab.lib.pagesizes import letter, A4
> _PAGE_SIZES = {"letter": letter, "a4": A4}
>
> def generate_candidate_pdf(
>     ...,
>     page_size: str = "letter",
> ) -> None:
>     pagesize = _PAGE_SIZES.get(page_size.lower(), letter)
>     doc = SimpleDocTemplate(output_path, pagesize=pagesize, ...)
> ```
>
> Thread `--page-size` through `generate_report.py` CLI args.
>
> **Priority:** Low.  Cosmetic impact only.

> **Implementation (completed — 9.2):**
> - Added `page_size` parameter to `generate_candidate_pdf()` accepting
>   `"letter"` (default) or `"a4"`.
> - Added `PAGE_SIZES` dict mapping names to reportlab page-size tuples.
> - `_make_header_footer()` now receives the actual `pagesize` tuple
>   instead of hardcoding `letter`.
> - Added `--page-size` CLI flag to `generate_report.py`.

### 9.3 ✅ FIXED Fetch Pipeline — Provider Abstraction for Fetching

**Files:**
- `impact/providers/github_live.py` — `GitHubLiveFetcher`
- `impact/providers/github/client.py` — `GitHubClient`
- `impact/providers/github/fetcher.py` — `GitHubFetcher`
- `impact/tasks/fetch.py` — Celery task
- `devrank/cli.py` — `fetch github` subcommand

The ingestion/adapter layer has a clean abstraction (`ProviderAdapter` base
class + `get_adapter(provider)` registry).  **The fetch layer has no
equivalent abstraction.**  Everything is hardwired to GitHub:

| Component | GitHub assumption |
|---|---|
| `GitHubClient` | Base URL `https://api.github.com`, `Bearer` auth, GitHub-specific `X-RateLimit-*` headers, `Link` header pagination, `"rate limit" in resp.text.lower()` English error parsing |
| `GitHubFetcher` | All endpoint paths (`/repos/{r}/pulls`, `/repos/{r}/issues/{n}/timeline`, `/repos/{r}/actions/runs`, etc.) |
| `GitHubLiveFetcher` | `"provider": "github"` in manifest, GitHub Search API `reviewed-by:` qualifier |
| Celery task `run_fetch` | Directly imports `GitHubLiveFetcher` — no provider dispatch |
| CLI `fetch github` | Hardcoded subcommand name, no extensible provider pattern |

> **Proposed Fix** `[MODEL + PROVIDER]`
>
> 1. **Define a `ProviderFetcher` abstract base class** (mirroring
>    `ProviderAdapter`):
>    ```python
>    # impact/providers/base.py
>    from abc import ABC, abstractmethod
>    from impact.domain.models import CanonicalBundle
>
>    class ProviderFetcher(ABC):
>        @abstractmethod
>        def fetch(self) -> CanonicalBundle:
>            """Fetch data and return a canonical bundle."""
>
>        @abstractmethod
>        def check_health(self) -> bool:
>            """Verify connectivity / auth."""
>    ```
>
> 2. **Create a fetcher registry** (like `impact/adapters/registry.py`):
>    ```python
>    # impact/providers/registry.py
>    def get_fetcher(provider: str, config: dict) -> ProviderFetcher:
>        if provider == "github":
>            return GitHubLiveFetcher(...)
>        raise ValueError(f"Unsupported provider: {provider}")
>    ```
>
> 3. **Update the Celery task** to accept a `provider` parameter and
>    dispatch through the registry.
>
> 4. **Update the CLI** to make `fetch` a generic command with a
>    `--provider` flag (default `github`), replacing the hardcoded
>    `fetch github` subcommand.
>
> **Priority:** High for multi-provider support.  This is the
> architectural prerequisite for adding GitLab/Bitbucket fetchers.

> **Implementation (completed):**
> - Created `impact/providers/base.py` with `FetchConfig` dataclass and
>   `ProviderFetcher` ABC (`run()` + `check_health()` abstract methods).
> - Created `impact/providers/registry.py` with dict-based fetcher registry:
>   `register_fetcher()`, `get_fetcher()`, `available_providers()`.
> - `GitHubLiveFetcher` now inherits from `ProviderFetcher`;
>   `LiveFetchConfig` extends `FetchConfig` with `fetch_contents`.
> - Added `check_health()` implementation for GitHub (calls `/rate_limit`).

### 9.4 ✅ FIXED Fetch Pipeline — English Error Message Parsing

**File:** `impact/providers/github/client.py` (line 100)

```python
if resp.status_code == 403 and "rate limit" in resp.text.lower():
```

This checks for the English phrase "rate limit" in the GitHub API error
response body.  While GitHub currently always returns English error messages,
this is a fragile assumption — a future API version or alternative provider
might return different text.

> **Proposed Fix** `[LOGIC]`
>
> Use HTTP headers and status codes (structural signals) instead of parsing
> error message text:
> ```python
> # GitHub signals rate limit exhaustion via:
> #   - 403 + X-RateLimit-Remaining: 0
> #   - 429 (secondary rate limit)
> if resp.status_code == 403:
>     remaining = resp.headers.get("X-RateLimit-Remaining")
>     if remaining is not None and int(remaining) == 0:
>         # Primary rate limit exhausted
>         ...
> ```
>
> **Priority:** Medium.  Works today but is a latent fragility.

> **Implementation (completed):**
> - Rewrote the HTTP 403 handler in `impact/providers/github/client.py` to
>   use structural signals first: `X-RateLimit-Remaining: 0` (primary),
>   `Retry-After` header (secondary), and English text parsing as a
>   last-resort fallback only when headers are absent.

### 9.5 No Change Needed — GitHub Adapter URL Structure Parsing

**File:** `impact/adapters/github.py` (lines 210, 296, 332)

The adapter extracts PR numbers from GitHub API URLs:
```python
pr_number = int(review_dict["pull_request_url"].split("/")[-1])
```

This is correctly located inside the GitHub adapter (not in metrics), but
depends on GitHub's URL structure (`/repos/{owner}/{repo}/pulls/{number}`).

> **Proposed Fix** `[LOGIC]`
>
> This pattern is **acceptable** since it's inside the GitHub adapter, which
> is by definition GitHub-specific.  However, a more robust approach is to
> use the `pull_request_number` field that the persistence layer enriches:
>
> For reviews and comments that lack an enriched `pull_request_number`, keep
> the URL parsing but wrap it in a helper with error handling:
> ```python
> def _extract_number_from_url(url: str) -> int | None:
>     """Extract trailing numeric ID from a GitHub API URL."""
>     try:
>         return int(url.rstrip("/").split("/")[-1])
>     except (ValueError, IndexError):
>         return None
> ```
>
> **Priority:** Low.  Correct as-is within the adapter boundary.

### 9.6 ✅ FIXED Config — ai_bots.yaml Multi-Provider Structure

**File:** `impact/config/ai_bots.yaml`

The bot list contains GitHub-specific logins:
```yaml
ai_review_bots:
  - copilot
  - copilot-pull-request-reviewer[bot]
  - codeant-ai-for-open-source[bot]
  - bito-code-review[bot]
  - ...
```

Comments reference `[bot]` suffix and GitHub API data.  On GitLab, bots
have different login formats (e.g., `project_NNNN_bot`).

> **Proposed Fix** `[CONFIG + PROVIDER]`
>
> 1. **Restructure as a multi-provider config:**
>    ```yaml
>    ai_review_bots:
>      # Cross-provider bot logins (checked on all providers)
>      common:
>        - copilot
>
>      # Provider-specific bot patterns
>      github:
>        - copilot-pull-request-reviewer[bot]
>        - codeant-ai-for-open-source[bot]
>        - bito-code-review[bot]
>        suffix_patterns: ["[bot]"]
>
>      gitlab:
>        - gitlab-bot
>        suffix_patterns: ["_bot"]
>
>      bitbucket:
>        - atlassian-connect
>    ```
>
> 2. **The adapter uses the provider-specific section** to set `User.is_bot`
>    (see section 7.1), while `common` entries apply to all providers.
>
> 3. **Alternatively**, if `User.is_bot` is the canonical solution (section
>    7.1), this config file becomes adapter-configuration for the GitHub
>    adapter specifically, and should move to `impact/adapters/github/` or
>    be loaded only by the GitHub adapter.
>
> **Priority:** Medium.  Prerequisite for correct bot filtering on non-GitHub
> providers.

> **Implementation (completed):**
> - Restructured `ai_bots.yaml` into multi-provider format:
>   `common` (cross-provider), `github` (logins + suffix_patterns),
>   `gitlab`, `bitbucket` sections.
> - Updated `_load_ai_bot_logins()` in `ai_suggestion_acceptance.py` to
>   handle both the new dict structure and the legacy flat list.
>   All provider sections are merged for the provider-neutral metrics layer.

### 9.7 ✅ FIXED CLI and API — Provider-Neutral Terminology in Help Text

**Files:**
- `devrank/cli.py` — lines 325, 352–357
- `impact/scripts/generate_report.py` — lines 103, 107, 176
- `impact/api/dependencies.py` — line 121

| Location | Text | Issue |
|---|---|---|
| `cli.py` L325 | `--user` help: `"GitHub login"` | Should say `"User login"` |
| `cli.py` L352 | `fetch github` subcommand | Should be extensible `fetch --provider github` |
| `cli.py` L357 | `"Fetch GitHub dump"` | Should be provider-neutral |
| `generate_report.py` L103 | `--fetch-repos` help: `"(owner/repo)"` | Format varies by provider |
| `generate_report.py` L107 | `--fetch-token` help: `"GitHub token"` | Should say `"API token"` |
| `generate_report.py` L176 | Error: `"no GitHub token provided"` | Should say `"no API token"` |
| `dependencies.py` L121 | `"GitHub login"` in query param | Should say `"User login"` |

> **Proposed Fix** `[LOGIC]`
>
> Replace "GitHub" with provider-neutral terminology:
> - `"GitHub login"` → `"User login (e.g., GitHub username)"`
> - `"GitHub token"` → `"API token (e.g., GitHub PAT)"`
> - `fetch github` → `fetch --provider github` (or keep `fetch github`
>   as a shortcut but add a generic `fetch --provider` flag)
>
> **Priority:** Low.  Cosmetic; does not affect functionality.

> **Implementation (completed):**
> - `cli.py`: `--user` help text changed from `"GitHub login"` to
>   `"User login (e.g., GitHub username)"`.
> - `cli.py`: Added `fetch run --provider` command alongside existing
>   `fetch github` shortcut (backward compatible).
> - `generate_report.py`: `--fetch-token` help and error messages now say
>   `"API token"` instead of `"GitHub token"`.
> - `dependencies.py`: Query param description now says `"User login"`.
> - `fetch_github.py`: Token help/error messages now provider-neutral.

### 9.8 ✅ FIXED Persistence — Provider-Neutral Docstring

**Files:**
- `impact/persistence/filesystem.py` — line 21
- JSONL files in canonical dumps

The `FileSystemDumpWriter` docstring says `"Writes canonical GitHub dump
files"` even though it is used by the canonical persistence layer and is
provider-neutral.

The JSONL file names (`pull_requests.jsonl`, `issue_comments.jsonl`) use
GitHub terminology.  GitLab has "merge requests" and "notes" rather than
"pull requests" and "issue comments".

> **Proposed Fix** `[LOGIC]`
>
> 1. Update docstring: `"Writes canonical dump files to a target directory."`
>
> 2. The JSONL file names are **acceptable as canonical naming**.  Since the
>    canonical model uses `PullRequest` (section 7.5), the file names are
>    consistent.  Adapters map provider-specific concepts into these
>    canonical files.  No change needed.
>
> **Priority:** Low.  Docstring fix only.

> **Implementation (completed):**
> - Updated `FileSystemDumpWriter` docstring from
>   `"Writes canonical GitHub dump files"` to
>   `"Writes canonical dump files to a target directory."`
>   with a note about provider-neutral canonical naming.

### 9.9 ✅ FIXED Adapter Registry — Plugin-Based Provider List

**File:** `impact/adapters/registry.py`

```python
def get_adapter(provider: str) -> ProviderAdapter:
    if provider == "github":
        return GitHubAdapter()
    else:
        raise ValueError(f"Unsupported provider: {provider}")
```

Adding a new provider requires modifying this function.

> **Proposed Fix** `[LOGIC]`
>
> Use a plugin-based registry pattern:
> ```python
> _ADAPTER_REGISTRY: dict[str, type[ProviderAdapter]] = {
>     "github": GitHubAdapter,
> }
>
> def register_adapter(provider: str, adapter_class: type[ProviderAdapter]):
>     _ADAPTER_REGISTRY[provider] = adapter_class
>
> def get_adapter(provider: str) -> ProviderAdapter:
>     cls = _ADAPTER_REGISTRY.get(provider)
>     if cls is None:
>         raise ValueError(
>             f"Unsupported provider: {provider}. "
>             f"Available: {list(_ADAPTER_REGISTRY.keys())}"
>         )
>     return cls()
> ```
>
> New adapters call `register_adapter("gitlab", GitLabAdapter)` at import
> time.  Alternatively, use Python entry points for true plugin discovery.
>
> **Priority:** Medium.  Architectural cleanliness for multi-provider.

> **Implementation (completed):**
> - Rewrote `impact/adapters/registry.py` with dict-based
>   `_ADAPTER_REGISTRY`, `register_adapter()`, `get_adapter()`,
>   `available_adapters()`.  GitHub registered via `_register_builtins()`.
>   New adapters call `register_adapter("gitlab", GitLabAdapter)`.

### 9.10 ✅ FIXED Celery Task — Provider-Dispatched via Registry

**File:** `impact/tasks/fetch.py`

```python
from impact.providers.github_live import GitHubLiveFetcher, LiveFetchConfig
...
fetcher = GitHubLiveFetcher(cfg)
```

The Celery task directly imports and instantiates the GitHub fetcher.
Adding a second provider would require duplicating the task or adding
conditional logic.

> **Proposed Fix** `[LOGIC + PROVIDER]`
>
> Accept a `provider` parameter and dispatch through the fetcher registry
> (section 9.3):
> ```python
> @shared_task(bind=True)
> def run_fetch(self, provider: str = "github", **kwargs):
>     from impact.providers.registry import get_fetcher
>     fetcher = get_fetcher(provider, kwargs)
>     bundle = fetcher.fetch()
>     return {...}
> ```
>
> **Priority:** High.  Same as section 9.3
>
> **Implementation (completed):**
> - `run_fetch()` now accepts a `provider` parameter (default `"github"`).
> - Uses `_build_config()` to create provider-specific `FetchConfig` subclass.
> - Dispatches through `get_fetcher(provider, cfg)` from the fetcher registry.
> - Adding a new provider requires zero changes to the Celery task.

---

## 10. Updated Implementation Priorities — Broader Codebase

Additions from section 9, merged into the existing priority framework:

### Phase 1: Structural Fixes (existing P0–P5 unchanged)

### Phase 2: Model Enrichment (existing P6–P9 unchanged)

### Phase 3: Coverage Expansion (existing P10–P18 unchanged)

### Phase 4: Broader Codebase — Provider Abstraction

| Priority | Fix | Section | Tag | Rationale |
|---|---|---|---|---|
| **P19** ✅ | Define `ProviderFetcher` base class + fetcher registry | 9.3 | MODEL+PROVIDER | Architectural prerequisite for any non-GitHub fetcher. |
| **P20** ✅ | Update Celery task to dispatch through fetcher registry | 9.10 | LOGIC+PROVIDER | Depends on P19. |
| **P21** ✅ | Update CLI `fetch` to accept `--provider` | 9.7 | LOGIC | Depends on P19. |
| **P22** ✅ | Restructure `ai_bots.yaml` as multi-provider config | 9.6 | CONFIG+PROVIDER | Prerequisite for bot filtering on non-GitHub providers. |
| **P23** ✅ | Replace English error-message parsing with HTTP headers | 9.4 | LOGIC | Latent fragility; small fix. |
| **P24** ✅ | Make adapter registry plugin-based | 9.9 | LOGIC | Architectural cleanliness. |

### Phase 5: Broader Codebase — Report Localisation

| Priority | Fix | Section | Tag | Rationale |
|---|---|---|---|---|
| **P25** ✅ | Extract PDF report strings to locale file | 9.1 | CONFIG | Enables non-English report output for management audiences. |
| **P26** ✅ | Add A4 page size option | 9.2 | CONFIG | International paper standard. |

### Phase 6: Cosmetic / Low Priority

| Priority | Fix | Section | Tag | Rationale |
|---|---|---|---|---|
| **P27** ✅ | Replace "GitHub" with provider-neutral text in CLI/API help | 9.7 | LOGIC | Cosmetic terminology. |
| **P28** ✅ | Fix FileSystemDumpWriter docstring | 9.8 | LOGIC | One-line docstring fix. |

---

*Last updated: Full codebase audit covering `impact/metrics/`, `impact/providers/`,
`impact/templates/`, `impact/scripts/`, `impact/adapters/`, `impact/ingestion/`,
`impact/persistence/`, `impact/api/`, `impact/config/`, `api/`, `devrank/cli.py`,
`errors.py`, and `impact/exceptions.py`. Keep in sync when adding new text-based
patterns, expanding language coverage, or adding new provider adapters.*
