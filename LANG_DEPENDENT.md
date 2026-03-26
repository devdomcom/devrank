# Language-Dependent Functions and Metrics

This document captures all English/language-dependent functions, metrics, and patterns in the DevRank codebase. Non-English repositories may experience degraded signal quality in these areas.

---

## 1. Language-Dependent Functions in `impact/metrics/utils.py`

### 1.1 Text Classification Helpers

#### `is_bug_fix_indicator(text: str) -> bool` (line 412)
Detects bug-fix focus in titles/bodies/messages.

**English patterns:**
- Conventional commit: `fix:`, `fix(scope):`, `fix!:`
- Prefixes: `"bugfix:"`, `"bug fix"`, `"bug:"`, `"hotfix"`
- Issue refs: `fixes #123`, `closes #123`, `resolves #123`, `issue #123`
- Word boundaries: `\berror\b`, `\bcrash\b`, `\bregression\b`

#### `is_revert_indicator(text: str) -> bool` (line 432)
Detects revert commits/PRs.

**English patterns:**
- `text.startswith("revert")`
- `"reverts commit" in text`

#### `is_conventional_commit(message: str) -> bool` (line 748)
Checks if commit follows conventional commit format.

**English types:**
```python
types = ["feat", "fix", "docs", "style", "refactor", "perf", "test", "build", "ci", "chore", "revert"]
```

#### `classify_defect_commit(message: str) -> bool` (line 786)
Returns True if commit message suggests defect-related work.

**Uses `_DEFECT_PATTERNS` (lines 758-782):**
- Core defect: `fix`, `bug`, `hotfix`, `defect`, `issue`
- Stability/resolution: `resolve`, `patch`, `regression`, `rollback`, `mitigate`, `triage`, `escalate`
- Production/support: `crash`, `flaky`, `incident`, `outage`
- Errors: `error`, `fail`, `panic`, `exception`

#### `compute_pr_body_quality(body: str | None) -> int` (line 542)
Scores PR body 0-100: length, markdown sections, issue/PR refs bonus.

**English patterns (lines 560-562):**
- `(?:fixes|closes|resolves|refs?)\s*#?\d+`
- `pr\s*#?\d+`
- `"pull request" in body.lower()`

### 1.2 Test Detection Patterns

#### `_TEST_CONTENT_PATTERNS` (lines 469-493)
Regex patterns for detecting test code in patch content.

**English/framework keywords:**
- `def test_\w+`
- `(?:describe|it|test)\s*\(`
- `@(?:Test|ParameterizedTest|RepeatedTest|BeforeEach|AfterEach)\b`
- `import\s+(?:unittest|pytest)`
- `import\s+(?:org\.junit|org\.testng)`
- `require\s+['"](?:rspec|minitest|test/unit)`
- `RSpec\.describe`
- `mod\s+tests\s*\{`

#### `is_test_file(filename: str) -> bool` (line 441)
Heuristic test file classification by path.

**English path patterns:**
- `"/test/"`, `"/tests/"`, `"__tests__/"`, `".test."`, `".spec."`, `"spec_"`, `"/testing/"`
- `startswith("tests/")`, `startswith("test/")`
- `(^|/)test_` regex

---

## 2. Metrics That Are Language-Dependent

| Metric Slug | File | Language Dependency |
|-------------|------|---------------------|
| `bug_fix_focus_rate` | `plugins/authored/bug_fix_focus_rate.py` | Uses `is_bug_fix_indicator()` on PR titles/bodies and commit messages |
| `revert_introduction_rate` | `plugins/authored/revert_introduction_rate.py` | Uses `is_revert_indicator()`; regex `reverts? commit ([a-f0-9]{7,40})` |
| `conventional_commit_rate` | `plugins/authored/commit_message_clarity.py` | Uses `is_conventional_commit()` (English types: feat, fix, docs, etc.) |
| `commit_message_mining` | `plugins/authored/commit_message_mining.py` | Uses `classify_defect_commit()` (all English defect keywords) |
| `pr_body_quality_score` | `plugins/authored/pr_body_quality.py` | Uses `compute_pr_body_quality()` (English: fixes, closes, pull request) |
| `review_comment_substance` | `plugins/influence/review_comment_substance.py` | Hardcoded `actionable` list (line 38-41): `"should"`, `"could"`, `"consider"`, `"suggest"`, `"recommend"`, `"instead"`, `"rather"`, `"prefer"`, `"nit:"`, `"todo:"` |

---

## 3. Other English-Dependent Patterns

### 3.1 AI Tool Signatures

**File:** `plugins/authored/ai_assisted_pr_rate.py`

**`_AI_TOOL_SIGNATURES`** (lines 17-59):
Tool names are English product names:
- `copilot`, `github copilot`, `co-authored-by: github copilot`
- `cursor`
- `claude`, `claude code`
- `chatgpt`, `gpt-4`, `gpt-3`
- `codewhisperer`
- `tabnine`
- `cody`

**`_HIGH_CONFIDENCE_PATTERNS`** (lines 67-76):
- `generated[-_\s]with:`, `generated[-_\s]by:`, `ai[-_\s]generated[-_\s]with:`

### 3.2 Merge Commit Filtering

| File | Line | Pattern |
|------|------|---------|
| `plugins/authored/follow_up_commit_rate.py` | 45 | `c.message.lower().startswith("merge ")` |
| `plugins/authored/commit_message_clarity.py` | 32 | `c.message.strip().lower().startswith("merge ")` |
| `utils.py` | 369 | `c.message.lower().startswith("merge ")` |

### 3.3 Revert Commit Detection

**File:** `plugins/authored/revert_introduction_rate.py` (line 54)
```python
match = re.search(r'reverts? commit ([a-f0-9]{7,40})', rc.message, re.IGNORECASE)
```

---

## 4. Language-Neutral (Programming Languages)

The following are **programming-language-specific**, not natural-language dependent:

| Component | Example | Notes |
|-----------|---------|-------|
| Tree-sitter language mappings | `.py` → `tree_sitter_python`, `.js` → `tree_sitter_javascript` | Programming syntax, not English |
| Pygments lexer detection | `score_comment_code_quality()` uses Pygments | Syntax highlighting, language-agnostic |
| `_WORKSPACE_DIRS` | `"packages"`, `"plugins"`, `"services"` | Universal monorepo conventions |
| `_MANIFEST_FILENAMES` | `package.json`, `pyproject.toml`, `go.mod` | Standard ecosystem file names |
| Programming keywords in static analysis | `def`, `function`, `class`, `import`, `return`, `async` | Source code keywords, universal |

---

## 5. Implications

### For Non-English Repositories

1. **Conventional commit metrics** (`conventional_commit_rate`) will return near-zero if commits use native-language prefixes (e.g., `feat:` → `機能:`, `fix:` → `修正:`).

2. **Defect mining** (`commit_message_mining`, `bug_fix_focus_rate`) will miss non-English defect indicators:
   - Japanese: `バグ`, `修正`, `不具合`
   - German: `Bug`, `Fehler`, `Korrektur`
   - Spanish: `bug`, `error`, `corrección`

3. **PR body quality** (`pr_body_quality_score`) may under-score bodies that use native-language equivalents of "fixes", "closes", "pull request".

4. **Review comment substance** (`review_comment_substance`) will miss actionable language in non-English (e.g., Spanish "debería", "podría", "considerar").

5. **AI tool detection** (`ai_assisted_pr_rate`) relies on English tool names; if a team uses localized tool names or non-English descriptions, detection may fail.

### Recommendations

- Consider adding locale-aware overrides or configuration for conventional commit types.
- Allow custom keyword lists for defect/bug-fix indicators per organization.
- Document that text-based metrics assume English as the primary language for developer communications.

---

## 6. Quick Reference Table

| Category | Functions/Patterns | File(s) |
|----------|-------------------|---------|
| Bug-fix detection | `is_bug_fix_indicator` | utils.py:412 |
| Revert detection | `is_revert_indicator` | utils.py:432 |
| Conventional commits | `is_conventional_commit` | utils.py:748 |
| Defect mining | `classify_defect_commit`, `_DEFECT_PATTERNS` | utils.py:758-791 |
| PR body quality | `compute_pr_body_quality` | utils.py:542-566 |
| Test detection | `_TEST_CONTENT_PATTERNS`, `is_test_file` | utils.py:441-493 |
| Review comments | `actionable` keyword list | review_comment_substance.py:38-41 |
| AI tool names | `_AI_TOOL_SIGNATURES` | ai_assisted_pr_rate.py:17-59 |
| Merge filter | `startswith("merge ")` | 3 files (see §3.2) |
| Revert regex | `reverts? commit` | revert_introduction_rate.py:54 |

---

*Last updated: Auto-generated from codebase scan. Keep in sync when adding new text-based patterns.*
