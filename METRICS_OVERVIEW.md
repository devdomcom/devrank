# DevRank Metrics Overview v5

**78 implemented | 21 ready to implement | 24 planned | 15 PM tool | 1 methodology variant | 11 methodology-specific | 3 CI/CD | 9 AI advanced | 9 competitive gap | 6 new AI era | 1 extension | 178 total**

_Updated: April 2026 — incorporates competitive analysis (GitKraken Insights, March 2026), 2026 AI metrics research, PM tool integration design, and audit-driven reclassification of planned vs ready metrics._

---

## AI Impact Legend

| Symbol | Meaning |
|--------|---------|
| 🆕 New | Metric not present in previous documents |
| ⚠️ Caution | AI can distort or invert the meaning |
| 🔄 Evolved | Same metric, but meaning, formula, or priority updated |
| 🔁 Renamed | Same concept, name or focus updated |
| ✅ Unchanged | No significant impact from AI |

## Structural Category Legend

| Symbol | Meaning |
|--------|---------|
| 🌐 Universal | Works for all methodologies — calculated for every team |
| 🔀 Variant | Same question, different calculation per Scrum/Kanban/Waterfall — formula chosen by ETL based on `team_config.methodology` |
| 🏃 Scrum-only | Exists only for Scrum teams — calculated and shown only in Scrum dashboard |
| 📋 Kanban-only | Exists only for Kanban teams — calculated and shown only in Kanban dashboard |
| 🏗️ Waterfall-only | Exists only for Waterfall teams — calculated and shown only in Waterfall dashboard |

## Priority Legend

| Symbol | Meaning |
|--------|---------|
| P0 | Critical — first thing the analyst must see |
| P1 | High — within first two weeks of onboarding |
| P2 | Medium — implement within 60 days |
| P3 | Low — nice to have, V2+ |

## Framework Legend

Metrics are tagged with their originating or primary framework(s):

| Framework | Description | Example Metrics |
|-----------|-------------|-----------------|
| **DORA** | DevOps Research & Assessment — the 4 core metrics for software delivery performance | Deployment Frequency, Lead Time for Changes, Change Failure Rate, MTTR. _DORA¹ marker_ on a metric (e.g. #9 Cycle Time) means it is a *partial* / pre-production proxy of a DORA concept rather than the canonical end-to-end metric. |
| **SPACE** | Developer productivity framework covering Satisfaction, Performance, Activity, Communication, Efficiency | Coding Days, Review Turnaround Time, Flow Efficiency, On-Call Burden |
| **CodeScene** | Adam Tornhill's research on code evolution, knowledge ownership, and hotspots | Hotspot Detection, Temporal Coupling, Bus Factor, Code Age, Knowledge Islands |
| **Lean** | Lean manufacturing principles applied to software — flow, waste reduction, cycle times | Cycle Time, Pickup Time, Deploy Time, Rework Rate, tt100 |
| **Kanban** | Pull-based flow management with WIP limits and continuous delivery | WIP Load, Flow Efficiency, Aging WIP, WIP Compliance Rate |
| **Traditional** | Classic software engineering metrics (McCabe, Chidamber & Kemerer, SIG) | Cyclomatic Complexity, LCOM4, Technical Debt Ratio, Maintainability Rating |
| **Network** | Social graph analysis applied to developer collaboration | Centrality measures, Review Network Density, Communication Strength, Team Coupling |
| **DevRank** | Custom metrics specific to this platform — novel combinations or AI-era innovations | AI-Assisted PR Rate, Review Leverage, PR Body Quality Score, Discussion Cycles |

---

## New AI-Era Metrics (6)

Metrics not present in earlier documents. No competitor has them.

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| ★1 | AI Retention Rate | % of devs still using AI after 20 weeks | 🆕 New | 🌐 Universal | DevRank | P1 |
| ★2 | AI Code Authorship % | % of production code written by AI | 🆕 New | 🌐 Universal | DevRank | P1 |
| ★3 | Comprehension Debt | % of code approved without being understood (survey) | 🆕 New | 🌐 Universal | DevRank | P1 |
| ★4 | AI Acceptance vs Merge Rate | Delta between accepted AI suggestions and code that passes review | 🆕 New | 🌐 Universal | DevRank | P2 |
| ★5 | Developer Experience Index (DXI) | Survey on perceived productivity, interruptions, AI usefulness — based on the SPACE-derived methodology developed by DX (Abi Noda / Margaret-Anne Storey) | 🆕 New | 🌐 Universal | SPACE | P1 |
| ★6 | Feature Throughput to Customer | Real features → business goals achieved | 🆕 New | 🌐 Universal | DevRank | P1 |

> **All 6 New AI-Era metrics are deferred** — they require external data sources (AI tool APIs, survey infrastructure, or business outcome tracking). See **Deferred D4** (AI Tool API Data) and **Deferred D5** (External Platform Integration) for unblock paths. ★4 specifically is a derived comparison rather than a standalone metric (see Cross-Section Relationships near Competitive Gap).

---

## Implemented — Authored (60)

Engineer-owned PRs and activity. All metrics work with the user-centric fetch pipeline (`impact/providers/github_live.py`) unless flagged in [Data Scope Caveats](#data-scope-metrics-requiring-repo-wide-data).

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 1 | AI-Assisted PR Rate | % PRs created with AI assistance (Copilot/Cursor/Claude/etc.), detected via commit/PR signatures | 🔄 Evolved | 🌐 Universal | DevRank |
| 2 | AI Adoption Rate | Per-engineer indicator of AI tool usage (inferred from commit/PR signatures) | 🔄 Evolved | 🌐 Universal | DevRank |
| 3 | AI Code Quality | Rework rate comparison: AI-assisted PRs vs human PRs (review iterations) | ✅ Unchanged | 🌐 Universal | DevRank |
| 4 | AI Phantom Ownership | Code primarily touched by AI with low human review depth — knowledge and maintenance risk indicator (no human truly understands the code in production) | ⚠️ Caution | 🌐 Universal | DevRank |
| 5 | AI Suggestion Acceptance | Ratio of accepted vs dismissed AI suggestions from review bots | 🔄 Evolved | 🌐 Universal | DevRank |
| 6 | PR Throughput | PR merge ratio (merged/opened); conversion rate, not volume — see #7 Delivery Volume for absolute count | ⚠️ Caution | 🌐 Universal | SPACE • Lean |
| 7 | Delivery Volume | Total lines added+deleted across merged PRs | ⚠️ Caution | 🌐 Universal | SPACE |
| 8 | Net Code Contribution | Lines added minus lines deleted | ⚠️ Caution | 🌐 Universal | SPACE |
| 9 | Cycle Time | Time from PR creation to merge — **PR-level cycle time, not full DORA Lead Time** (commit→production); see A4-2 for the canonical DORA Lead Time for Changes | 🔄 Evolved | 🌐 Universal | DORA¹ • SPACE • Lean |
| 10 | Coding Time To PR | Time from first commit to PR creation | ✅ Unchanged | 🌐 Universal | Lean |
| 11 | Merge Delay | Time from first approval to merge | ✅ Unchanged | 🌐 Universal | Lean |
| 12 | Pickup Time | PR opened to first non-author review activity | ✅ Unchanged | 🌐 Universal | Lean |
| 13 | PR Size Distribution | Statistical distribution of PR sizes (small/medium/large/XL) | 🔄 Evolved | 🌐 Universal | DevRank |
| 14 | Trivial Contribution Rate | % of PRs that are trivial (auto-generated, tiny, boilerplate) | 🔄 Evolved | 🌐 Universal | DevRank |
| 15 | Code Churn Rate | % of recently written lines overwritten | 🔄 Evolved | 🌐 Universal | SPACE • CodeScene |
| 16 | Rework Rate | % of changes that rewrite author's own recent code | 🔄 Evolved | 🌐 Universal | SPACE • Lean |
| 17 | First-Time Approval Rate | % of PRs approved without change requests | ✅ Unchanged | 🌐 Universal | SPACE • DevRank |
| 18 | Review Iterations | Number of review round-trips before merge | 🔄 Evolved | 🌐 Universal | Lean • DevRank |
| 19 | Discussion Cycles | Alternating-person comment exchanges per merged PR | ✅ Unchanged | 🌐 Universal | DevRank |
| 20 | Time to First Review | Median time from PR creation to initial reviewer feedback | ✅ Unchanged | 🌐 Universal | Lean |
| 21 | Slow Review Response | Median author response time to changes-requested reviews | ✅ Unchanged | 🌐 Universal | Lean |
| 22 | Follow-Up Commit Rate | % of PRs with post-review follow-up commits | ✅ Unchanged | 🌐 Universal | DevRank |
| 23 | Self-Merge Rate | % of PRs merged without non-author approval | ⚠️ Caution | 🌐 Universal | DevRank |
| 24 | Abandoned PR Rate | % of PRs closed without merging | ✅ Unchanged | 🌐 Universal | Lean |
| 25 | PR Merge Effectiveness | Combines merge speed with review interaction count for merge smoothness | ✅ Unchanged | 🌐 Universal | SPACE |
| 26 | PR Body Quality Score | Quality of PR descriptions (length, structure, links) | ✅ Unchanged | 🌐 Universal | DevRank |
| 27 | Conventional Commit Rate | % of commits following conventional commit format | ✅ Unchanged | 🌐 Universal | DevRank |
| 28 | Test File Ratio | Ratio of test file changes to production file changes | ⚠️ Caution | 🌐 Universal | Traditional |
| 29 | Documentation Touch Rate | % of PRs that include documentation changes | ✅ Unchanged | 🌐 Universal | DevRank |
| 30 | Dependency Change Rate | % of PRs modifying dependency/manifest files | ✅ Unchanged | 🌐 Universal | DevRank |
| 31 | Module / Area Breadth | Number of distinct modules/areas touched | ✅ Unchanged | 🌐 Universal | DevRank |
| 32 | PR Category Diversity | Diversity of PR types (features, fixes, refactors, etc.) | ✅ Unchanged | 🌐 Universal | DevRank |
| 33 | Bug Fix Focus Rate | % of PRs addressing bug fixes | ✅ Unchanged | 🌐 Universal | DevRank |
| 34 | Coding Days | Number of days with commit activity | ✅ Unchanged | 🌐 Universal | SPACE |
| 35 | Active Weeks | Number of weeks with at least one contribution | ✅ Unchanged | 🌐 Universal | SPACE |
| 36 | Off-Hours Activity Rate | % of commits made outside business hours | ✅ Unchanged | 🌐 Universal | SPACE |
| 37 | Burstiness | Ratio of max weekly activity to average — pacing/sustainability signal | ✅ Unchanged | 🌐 Universal | DevRank |
| 38 | Revert Introduction Rate | % of PRs that introduce reverts | 🔄 Evolved | 🌐 Universal | DevRank |
| 39 | Hotspot Detection | Files with highest revision frequency × complexity | 🔄 Evolved | 🌐 Universal | CodeScene |
| 40 | Bus Factor | Min developers who could leave before code is unmaintainable | 🔄 Evolved | 🌐 Universal | CodeScene |
| 41 | Knowledge Islands | Files/modules where 95%+ written by one person — ownership concentration risk | ⚠️ Caution | 🌐 Universal | CodeScene |
| 42 | Knowledge Loss | Code where 50%+ written by departed/inactive contributors | ✅ Unchanged | 🌐 Universal | CodeScene |
| 43 | Code Familiarity | % of codebase known by current active team | ⚠️ Caution | 🌐 Universal | CodeScene |
| 44 | Main Developer (by lines) | Primary author per file by lines added | 🔄 Evolved | 🌐 Universal | CodeScene |
| 45 | Main Developer (by revisions) | Primary author per file by commit count | ✅ Unchanged | 🌐 Universal | CodeScene |
| 46 | Entity Ownership | Per-author contribution percentages per file | 🔄 Evolved | 🌐 Universal | CodeScene |
| 47 | Contributor Experience | Relative share of codebase activity by the target developer | 🔄 Evolved | 🌐 Universal | CodeScene |
| 48 | Temporal / Logical Coupling | Files that always change together (hidden dependencies) | ✅ Unchanged | 🌐 Universal | CodeScene |
| 49 | Entity Fragmentation | Herfindahl-like index of author scatter per file | ✅ Unchanged | 🌐 Universal | CodeScene |
| 50 | Complexity Trend | Whitespace-based complexity tracked per file over time | ✅ Unchanged | 🌐 Universal | Traditional |
| 51 | Change Proximity | Sum of distances between changed lines within a file | ✅ Unchanged | 🌐 Universal | CodeScene |
| 52 | Sum of Coupling | Per-entity total coupling score across all revisions | ✅ Unchanged | 🌐 Universal | CodeScene |
| 53 | Absolute Churn Trend | Lines added/deleted per date — detects integration bottlenecks | ✅ Unchanged | 🌐 Universal | CodeScene |
| 54 | Commit Message Mining | Regex search of commit messages for defect indicators | ✅ Unchanged | 🌐 Universal | CodeScene |
| 55 | Code Survival | % of contributed lines still alive over time | 🔄 Evolved | 🌐 Universal | CodeScene |
| 56 | Flow Efficiency | Active coding time / total lead time for merged PRs (Kanban flow health) | ✅ Unchanged | 🌐 Universal | Kanban • Lean |
| 57 | WIP Load | Concurrent open PRs per day (Lean WIP indicator; high WIP = context-switching overhead) | ✅ Unchanged | 🌐 Universal | Lean • Kanban |
| 58 | Review Coverage | % of PR files with at least one human inline review comment | ⚠️ Caution | 🌐 Universal | DevRank |
| 59 | Delivery Risk Score (1-10) | Per-commit risk based on code, file count, diffusion, experience | 🔄 Evolved | 🌐 Universal | DevRank |
| 60 | Time to Restore (MTTR / Mean Time to Recovery) | Time from revert to fix on the same files (DORA MTTR proxy from revert→fix cycles; deployment-aware version pending — see D1) | 🔁 Renamed | 🌐 Universal | DORA |

## Implemented — Influence (17)

Impact on others' work — reviewing, mentorship, network position.

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 61 | Reviews Given | Total reviews submitted for others' PRs | 🔄 Evolved | 🌐 Universal | SPACE |
| 62 | Review Turnaround Time | Time from review request to submission | 🔄 Evolved | 🌐 Universal | SPACE • Lean |
| 63 | Unblock Time | Time to unblock others via reviews/approvals | ✅ Unchanged | 🌐 Universal | Lean |
| 64 | Inline Comment Density | Average inline comments given per PR reviewed (review depth on others' PRs) | ✅ Unchanged | 🌐 Universal | DevRank |
| 65 | Review Comment Substance | Pygments-based scoring of code content in review comments | 🔄 Evolved | 🌐 Universal | DevRank |
| 66 | Review Leverage | Effectiveness of change requests in driving author updates (review impact rate) | ✅ Unchanged | 🌐 Universal | DevRank |
| 67 | Review Breadth | Number of distinct PR authors reviewed | ✅ Unchanged | 🌐 Universal | SPACE • Network |
| 68 | Review Demand | How sought-after as a reviewer (review requests received) | ✅ Unchanged | 🌐 Universal | Network |
| 69 | PR Merge Rate | Proportion of user's reviews followed by merge in close sequence (no other interveners) — measures review-to-merge causality | ✅ Unchanged | 🌐 Universal | SPACE • Lean |
| 70 | Approval To Merge Ratio | % of user's approvals that were the last activity before merge (no subsequent reworks) — clean approval rate | ⚠️ Caution | 🌐 Universal | DevRank |
| 71 | Change-Inducing Review Rate | % of reviews that led to code changes | ✅ Unchanged | 🌐 Universal | DevRank |
| 72 | Blocking Comment Rate | % of review comments that block merge | ✅ Unchanged | 🌐 Universal | DevRank |
| 73 | First Reviewer Rate | % of reviews where person was first reviewer | ✅ Unchanged | 🌐 Universal | DevRank |
| 74 | Mentorship Signal | Reviews targeting PRs from low-activity contributors | ✅ Unchanged | 🌐 Universal | SPACE • Network |
| 75 | Knowledge Sharing Index | How evenly code reviews distribute across team (0-1 entropy-based) | ✅ Unchanged | 🌐 Universal | Network |
| 76 | Degree Centrality | Number of direct collaborators | ✅ Unchanged | 🌐 Universal | Network |
| 77 | Betweenness Centrality | Whether developer bridges disconnected teams | ✅ Unchanged | 🌐 Universal | Network |

## Implemented — Mixed (1)

Both authored and influence signals.

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 78 | Co-Author Contribution Rate | Inbound/outbound co-author commit % (collaboration on user's own PRs and on others') | 🔄 Evolved | 🌐 Universal | SPACE |

---

## Ready to Implement (21)

These metrics have **all required infrastructure already present** — utility functions, dependencies, sample data, ledger indexes — but no plugin file yet. Each one is implementable in 1–3 days and unblocks immediately. Effort labels: 🟢 Low (<100 LOC), 🟡 Medium (100–300 LOC).

| # | Metric | Description | Effort | Existing Infrastructure | Section / Framework |
|---|--------|-------------|--------|-------------------------|----------------------|
| R1 | Code Age | Months since last **file-level** modification (file age, not line survival — see #55 Code Survival for line-level) | 🟢 | `Commit.date` field; needs commit→file index built from `bundle.files` and `bundle.commits` join (no current per-file commit history index) | A1 / CodeScene |
| R2 | History Complexity (Entropy) | Normalized entropy of changes across files | 🟡 | `_shannon_entropy()` at `utils.py:854` | A1 / CodeScene |
| R3 | Hunks Count (Change Fragmentation) | Median diff hunks per file | 🟢 | `parse_hunks()` at `utils.py:1182` | A1 / CodeScene |
| R4 | Delta Maintainability Model | Per-function cyclomatic complexity | 🟡 | `parse_functions()` at `utils.py:2018` (statement count exists; needs decision-point counting) | A1 / Traditional |
| R5 | Cyclomatic Complexity | Linearly independent paths through code | 🟡 | tree-sitter already in deps; extend AST walker | A6 / Traditional |
| R6 | AST-Based Duplication | Structural hashing for duplicate code blocks | 🟡 | tree-sitter ready; needs ~150 LOC normalization + hash | A6 / Traditional |
| R7 | Time to Approve | First review activity → first approval | 🟢 | Filter `ReviewRecord.state == APPROVED` | A5 / Lean |
| R8 | Innovation Rate | % merged PRs representing new feature work | 🟢 | Apply existing `pr_category_diversity` classifier to merged + `category=feat` | A7 / DevRank |
| R9 | Inefficiency Pool | Synthesis: PR idle time + friction + wasted effort | 🟡 | Combine #11 Merge Delay + #18 Review Iterations + #16 Rework Rate + #12 Pickup Time | A7 / Lean |
| R10 | Context Switch Frequency | Intra-day switches between repos/projects | 🟡 | Same timezone infra as #36 Off-Hours Activity Rate | A8 / SPACE |
| R11 | Productive Impact | Impact × (1 − Rework Rate) | 🟢 | #16 Rework Rate available; "Impact" = #7 Delivery Volume or #59 Delivery Risk Score | A9 / Lean |
| R12 | tt100 (Time to 100) | Time to write 100 lines of productive code | 🟡 | #9 Cycle Time + #7 Delivery Volume + boilerplate filter | A9 / Lean |
| R13 | Closeness Centrality | How quickly a developer reaches entire org | 🟡 | Hand-rolled BFS (extend `betweenness_centrality.py` pattern); NetworkX dependency planned for future refactor | A3 / Network |
| R14 | Eigenvector Centrality | Influence through association | 🟡 | **Prerequisite blocker:** add `networkx>=3.0` to `pyproject.toml` (currently NOT in deps) for power-method calc | A3 / Network |
| R15 | Communication Strength | Conway's Law via shared commits | 🟡 | Co-author data via #78 + commit-to-file mapping | A3 / Network |
| R16 | Review Network Density | How interconnected the review graph is | 🟢 | Reuse review graph from `betweenness_centrality.py`; density = 2·E/(N·(N−1)) | A3 / Network |
| R17 | Deployment Frequency | Count of production deployments / period | 🟢 | `ledger.get_deployments()` exists; **see caveat note below** | A4 / DORA |
| R18 | PR Comments Count | Total comments left on PRs per period | 🟢 | `ledger.get_comments_for_pr()` exists at `ledger.py:183` (currently unused — most metrics call `get_review_comments_for_review()` instead) | CG / DevRank |
| R19 | Commit Count | Total commits pushed in period (already used as denominator) | 🟢 | Trivially `len(bundle.commits)` per period filter | CG / DevRank |
| R20 | PR Maturity Ratio | How much a PR changes between open and merge (size delta, distinct from #16 self-rewrite) | 🟢 | Compare PR.created additions vs final additions; data in `pr_raw` already | A9 / DevRank |
| R21 | Idle Completion Time | Time from rework complete → merge (post-rework window, distinct from #11 post-approval) | 🟢 | Use last commit timestamp on PR + merged_at | A9 / Lean |

> **R17 Deployment Frequency — sample data caveat:** the apache/superset reference dump contains only **1 deployment in 3 months**, all to `github-pages` (documentation environment, not production). Implementing this metric requires strict guards:
> - `no_data: True` if fewer than 3 deployments in the period
> - `no_data: True` if no deployment matches a production-environment allow-list (e.g. `production`, `prod`, `live`, customer-configurable)
> - Mitigation: customer onboarding should validate that their CI/CD pipeline calls `POST /repos/{owner}/{repo}/deployments` for production releases. Many teams using GitHub Actions skip this and only use `Releases` — in which case we should fall back to release-based frequency.

> **R20 PR Maturity Ratio vs #16 Rework Rate:** Rework Rate measures lines that *overwrite the author's own past lines* (line-level overlap). PR Maturity Ratio measures *total diff growth* between PR open and merge — a PR can mature heavily (lots of new commits) without rewriting any past lines. Both signals are useful; implement together for fuller PR-volatility picture.

> **R21 Idle Completion Time vs #11 Merge Delay:** Merge Delay = first approval → merge. Idle Completion Time = last code change → merge. They differ when a PR has rework after approval (common pattern: approve → request small change → rework → merge). Idle Completion isolates the pure waiting period.

---

## Implemented with Caveats — TODO

The following metrics are **implemented and registered** (counted in the 78) but have known issues surfaced during the multi-agent code audit. They produce results today but should be revisited for correctness, naming, or completeness before claiming production-grade quality. Each item is **scheduled for follow-up** — intentionally not auto-fixed because some changes are invasive (renaming) or require product judgment (semantics).

### Type A — Metric name doesn't match implementation (4)

The metric name suggests one thing; the code computes another. Doc descriptions now align to code, but **the metric name itself is misleading**.

| # | Metric | Name implies | Code actually computes | Recommended action |
|---|--------|--------------|------------------------|--------------------|
| 25 | PR Merge Effectiveness | Generic "merge effectiveness" | Composite of merge speed + review interaction count | Acceptable — name is vague enough; document better in code docstring |
| 64 | Inline Comment Density | A density (ratio per file/LOC/comment) | Simple average inline comments per PR | **Rename** to "Avg Inline Comments per PR" OR rewrite as a true density (inlines / total review comments) |
| 66 | Review Leverage | Lines of code influenced per review (amplification metric) | Effectiveness rate of change requests in driving author updates | **Significant divergence.** Either rewrite to compute true LOC-influenced leverage, or rename to "Change Request Effectiveness" |
| 70 | Approval To Merge Ratio | A:M numerical ratio | % approvals that were the *last activity* before merge (no subsequent reworks) | **Rename** to "Clean Approval Rate" or similar — current name implies a different calculation |

> **Why deferred:** renaming a metric breaks: (a) slug in `get_metrics()` registry, (b) threshold key in `impact/thresholds.py`, (c) role config YAML files, (d) tests, (e) any external consumer (DB rows, dashboards). Needs a coordinated rename PR with migration.

### Type B — Framework tags missing in code (1)

Metric class is missing a framework tag that conceptually applies.

| # | Metric | Code has | Should also have | Why |
|---|--------|----------|-------------------|-----|
| 78 | Co-Author Contribution Rate | `['SPACE']` | `'Network'` | Co-authorship is a core Network/collaboration signal — it measures the developer's social graph through commit pairing, not just SPACE Activity |

> **Fix:** one-line change to `frameworks` property in `impact/metrics/plugins/mixed/co_author_contribution_rate.py`. After the change, restore "SPACE • Network" in the doc framework column for #78 and bump Network framework count from 6 → 7.

### Type C — Code descriptions missing data-scope caveats (3)

The metric's `description` property doesn't warn that the metric produces unreliable results under the user-centric fetch pipeline. The doc's "Data Scope" section has the warning, but anyone reading the metric in isolation (API response, tooltip, generated report) will not see it.

| # | Metric | Caveat to append to code description |
|---|--------|--------------------------------------|
| 47 | Contributor Experience | "Grossly inflated under user-centric fetch — denominator only counts user-related PR lines, not full repo lines." |
| 60 | Time to Restore | "Only sees reverts in PRs involving the assessed user — misses most repo-wide incidents." |
| 75 | Knowledge Sharing Index | "Fundamentally limited under user-centric fetch — measures distribution only across reviewers visible in user-related PRs, not the full team." |

> **Fix:** append the caveats to each metric's `description` property and consider adding a `details["caveat"]` field to `MetricResult` so downstream consumers (PDF report, API) can surface them prominently. Also add a `data_scope_caveat` flag to the `Metric` base class so all current/future metrics can declare scope limits in a structured way.

### Type D — Ambiguous code descriptions (2)

The metric's code description is unclear or contradicts implementation details.

| # | Metric | Issue | Recommended description |
|---|--------|-------|--------------------------|
| 62 | Review Turnaround Time | Description "Median hours to first review/action on opened PRs" reads as author-side (PR opened → reviewed) but the metric is in `influence/` and is about the **reviewer's** response time | "Median hours from review request received to first response — measures the user's responsiveness as a reviewer (balanced by period)." |
| 74 | Mentorship Signal | Description hardcodes "<5 PRs in period" but code dynamically scales the threshold by period: `max(2, int(5 * period_days / 30))` | "% of reviews targeting low-activity contributors (junior threshold scales with period: ~5 PRs / 30-day window)." |

### Summary

10 caveat items across 4 types. Recommended fix order:
1. **Type C (3 items)** — append caveats to descriptions. Lowest risk, highest user-protection. ~5 LOC per metric.
2. **Type B (1 item)** — add `'Network'` framework tag to #78. Trivial.
3. **Type D (2 items)** — clarify wording in code descriptions. Trivial, no behavior change.
4. **Type A (4 items)** — needs product call (rename vs rewrite). Coordinated PR with migration plan for #66, #70 (potential rename), and #64 (could go either way). #25 acceptable as-is with docstring polish.

> **New dependency added for Ready-tier graph metrics:** `networkx>=3.0` is now in `pyproject.toml` and installs via `uv sync`. Used for R14 Eigenvector Centrality and the planned refactor of R13 Closeness Centrality + #77 Betweenness Centrality (currently hand-rolled Brandes BFS) to use NetworkX for consistency and performance.

---

## Data Scope: Metrics Requiring Repo-Wide Data

The current fetch pipeline (`impact/providers/github_live.py`) is **user-centric**: it only fetches PRs authored by, assigned to, or reviewed by the assessed engineer. This means `bundle.pull_requests`, `bundle.commits`, `bundle.reviews`, and `bundle.files` do **not** contain the full repository picture.

The following 12 metrics access bundle-level data to compute cross-contributor statistics. Because the bundle only contains user-related PRs, these metrics operate on **incomplete data** and their results should be interpreted with that caveat.

### Hybrid Scope (user's files + all-contributor context needed)

| Metric | What It Needs | Impact of User-Centric Data |
|--------|--------------|----------------------------|
| Bus Factor | All contributors to user's files | Underestimates — misses contributors from PRs not involving the user |
| Knowledge Islands | All commits per file for ownership % | Overestimates — other owners' contributions invisible |
| Knowledge Loss | All active contributors in the repo | Overestimates — active contributors invisible if they didn't touch user's PRs |
| Main Developer (by revisions) | Full commit distribution per file | Skewed — may misidentify main developer |
| Main Developer (by lines) | Full line-contribution distribution per file | Skewed — may misidentify main developer |
| Entity Ownership | Full ownership breakdown per file | Skewed — ownership percentages inflated for visible authors |
| Code Familiarity | Whether other active contributors know user's files | Underestimates — team members only visible if they appeared in user-related PRs |
| Contributor Experience | user_lines / total_repo_lines | Grossly inflated — denominator is user-related lines, not repo-wide |
| Revert Introduction Rate | All reverts of user's code (by anyone) | Underestimates — misses reverts in PRs not involving the user |
| Self-Merge Rate | Repo-wide self-merge baseline for culture context | `repo_self_merge_rate` field is skewed (only user-related PRs in denominator) |

### Repo-Wide Scope (all-repo data needed)

| Metric | What It Needs | Impact of User-Centric Data |
|--------|--------------|----------------------------|
| Time to Restore (DORA MTTR) | All repo commits to detect revert→fix cycles | Misses most incidents — only sees reverts in user-related PRs |
| Knowledge Sharing Index | All reviews from all reviewers | Fundamentally broken — measures distribution only across reviewers visible in user-related PRs, not the full team |

The remaining 66 metrics use `ledger.get_prs_for_user()`, `get_reviews_for_user()`, or `get_commits_for_user()` to filter to the assessed engineer's own activity and work correctly with the user-centric fetch.

---

## New PM Tool Metrics (15)

Require Jira/Linear/Asana integration. Map to typical project-management questions analysts get from clients.

### Group 1 — Ticket Workflow (8)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| PM1 | Ticket Throughput | Tickets completed per week by type/team/epic | 🆕 New | 🌐 Universal | P1 |
| PM2 | Ticket Cycle Time | Time from creation to ticket closure | 🆕 New | 🌐 Universal | P1 |
| PM3 | Time in State | Average time in each workflow state | 🆕 New | 🌐 Universal | P1 |
| PM4 | Ticket Blocked Rate | % blocked tickets + average block duration | 🆕 New | 🌐 Universal | P2 |
| PM5 | Workflow Regression Rate | % tickets that move backward in workflow | 🆕 New | 🌐 Universal | P2 |
| PM6 | Backlog Age Distribution | Distribution of ticket age in backlog (30/60/90 days) | 🆕 New | 🌐 Universal | P2 |
| PM7 | Bug Trend | Trend of opened vs closed bugs over time | 🆕 New | 🌐 Universal | P2 |
| PM8 | Ticket Reopening Rate | % closed tickets that get reopened | 🆕 New | 🌐 Universal | P2 |

### Group 2 — Planning Quality (3)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| PM9 | Estimation Accuracy *(= V3)* | Original estimates vs actual time spent | 🆕 New | 🔀 Variant | P2 |
| PM10 | Unplanned Work Rate (ex Injected Work Rate) *(= V2)* | % of unplanned work | 🆕 New | 🔀 Variant | P2 |
| PM11 | Requirements Change Rate | Frequency of requirements changes after work begins | 🆕 New | 🌐 Universal | P2 |

**Variant notes:**
- **Estimation Accuracy**: Scrum = story points estimated vs real. Kanban = real cycle time vs historical percentile (p50/p85). Waterfall = phase duration estimated vs real.
- **Unplanned Work Rate**: Scrum = work injected mid-sprint. Kanban = expedited tickets bypassing the flow. Waterfall = change requests after requirements sign-off.

> **PM9 = V3 and PM10 = V2** — these are the same metrics listed under both naming schemes (PM-tool perspective and methodology-variant perspective). Counted **once** in totals (counted under PM Tool here, NOT double-counted under Methodology Variants below). V1 (Planning Reliability ex CRR) has no PM-numbered equivalent.

### Group 3 — Git + PM Tool combined (4) — UNIQUE DIFFERENTIATOR

No competitor can compute these because they require both data sources.

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| PM12 | PR to Ticket Ratio | Average number of PRs per ticket | 🆕 New | 🌐 Universal | P1 |
| PM13 | Time from Ticket to First Commit | From "In Progress" status to first commit | 🆕 New | 🌐 Universal | P1 |
| PM14 | Ticket Lead Time | From request to delivery (includes backlog time) | 🆕 New | 🌐 Universal | P1 |
| PM15 | Bug Escape Rate | % bugs in production vs found during testing | 🆕 New | 🌐 Universal | P1 |

### Resolved Overlaps

| Case | Decision |
|------|----------|
| Sprint Predictability vs Commitment Reliability Rate | **Merged.** CRR becomes "Planning Reliability" variant. Sprint Predictability removed. |
| Bug Trend vs Defect Rate / Bug Fix Focus Rate | **Not duplicates.** Different sources (PM tool vs GitHub). Bug Trend is additive. |

---

## Methodology Variant Metrics (1 unique + 2 cross-references)

These metrics exist for all teams but the calculation changes based on the methodology configured in `team_config.methodology`. The ETL applies the right formula. Superset receives the precomputed value.

| # | Metric | Scrum | Kanban | Waterfall | Counted Under |
|---|--------|-------|--------|-----------|---------------|
| V1 | **Planning Reliability** (ex CRR) | % of planned work completed in sprint | Throughput stability per week (coefficient of variation) | Adherence to phase milestones | **Methodology Variants** (this section) |
| V2 | **Unplanned Work Rate** (= PM10) | % of work injected mid-sprint | % of expedited/urgent tickets that bypass the flow | % change requests after requirements sign-off | PM Tool (cross-reference only) |
| V3 | **Estimation Accuracy** (= PM9) | Story points estimated vs real time | Real cycle time vs historical percentile (p50/p85) | Phase duration estimated vs real | PM Tool (cross-reference only) |

> **V2 = PM10 and V3 = PM9** — to avoid double-counting, only V1 is counted under "Methodology Variants" in totals. V2 and V3 are listed here for the methodology view but counted under "PM Tool" above.

---

## Methodology-Specific Metrics (11)

These exist only for one methodology. Calculated only for teams configured with that methodology and shown only in the corresponding dashboard.

### Kanban-only (6) 📋

| # | Metric | Description | AI Status | Priority | Notes |
|---|--------|-------------|-----------|----------|-------|
| K1 | Aging WIP | For each ticket in progress, how many days it has been in that state | 🆕 New | P1 | Real-time alert. Without sprints there's no natural deadline — tickets stay open for weeks unnoticed |
| K2 | WIP Compliance Rate | % of time WIP limits are respected | 🆕 New | P1 | The most important metric in Kanban. No equivalent in Scrum |
| K3 | Throughput Variability | Standard deviation of weekly throughput | 🆕 New | P2 | Mature Kanban team = low variability. Complementary to Planning Reliability |
| K4 | Queue Time Ratio | % of cycle time spent waiting vs actively worked on | 🆕 New | P2 | More granular than Flow Efficiency — distinguishes queue waiting from inactivity |
| K5 | Service Level Expectation (SLE) | % of tickets completed within N days per type | 🆕 New | P1 | How Kanban teams give predictability to the business. Percentile-based, not average |
| K6 | Cumulative Flow Diagram data | Tickets in each workflow state over time | 🆕 New | P1 | The signature Kanban visualization. The "bands" between states reveal bottlenecks |

### Scrum-only (2) 🏃

| # | Metric | Description | AI Status | Priority | Notes |
|---|--------|-------------|-----------|----------|-------|
| S1 | Sprint Burndown | Story points/tickets remaining per day vs ideal line | 🆕 New | P1 | The most-used chart in Scrum. Daily data points for the curve |
| S2 | Carry-over Rate | % planned tickets not completed that move to next sprint | 🆕 New | P2 | Different from Planning Reliability: measures accumulation volume, not success rate |

### Waterfall-only (3) 🏗️

| # | Metric | Description | AI Status | Priority | Notes |
|---|--------|-------------|-----------|----------|-------|
| W1 | Phase Gate Compliance | % milestones met on time | 🆕 New | P2 | Yes/no per milestone — management asks for it first |
| W2 | Defect Density per Phase | Defects found in each phase (design review, testing, UAT, production) | 🆕 New | P2 | Bug found in testing costs 10x less than in production. Waterfall's signature metric |
| W3 | Schedule Variance | How much current phase is ahead/behind original plan | 🆕 New | P2 | Quantitative (days or %). Complements binary Phase Gate Compliance |

---

## AI-Era Prioritization Rationale

In the AI-assisted development era, metric priorities shift significantly from traditional software engineering. The priorities below explain why specific themes are P0/P1 vs P2/P3 in the planned sections.

**P0 (Critical) — Implement First:**

1. **AI Transparency** — Without visibility into AI contribution (AI-Assisted PR Rate, AI Code Quality, AI Phantom Ownership), teams cannot assess AI tool ROI or risks.
2. **Knowledge Risk** — AI-generated code creates "phantom ownership" where no human truly understands the code. Bus Factor and Knowledge Islands become existential risks.
3. **DORA Fundamentals** — Lead Time and Deployment Frequency remain the ultimate measures of delivery performance.
4. **Quality Gates** — With AI producing code faster, Delivery Risk Score, Review Coverage, and Code Health Score prevent quality collapse.

**P1 (High) — Implement Second:**

1. **Work Classification** — Understanding what work AI does vs humans enables resource allocation decisions (Work Type Breakdown, Innovation Rate).
2. **Flow Efficiency** — AI-human handoffs in the PR process create new bottlenecks (Pickup Time, Flow Efficiency, Discussion Cycles).
3. **Code Quality** — Cognitive complexity matters more when AI generates verbose solutions.
4. **Network Health** — Collaboration patterns reveal whether AI is isolating developers or enhancing teamwork (Knowledge Sharing Index, Centrality measures).

**P2/P3 (Medium/Low) — Implement Later:**

- **Traditional complexity metrics** (cyclomatic, LCOM4) — AI-generated code often has different complexity patterns; the legacy thresholds may not directly apply.
- **Meeting time tracking** — Less relevant as AI reduces certain coordination needs.
- **Code age/entropy** — Long-term metrics that matter less in rapidly evolving AI-assisted codebases.

---

## Planned — A1. Codebase Evolution (0)

> All 4 metrics in this section have moved to **Ready to Implement** (R1-R4) — utility functions and data exist; only plugin files needed.

## Planned — A3. Graph/Network Collaboration (0)

> Degree Centrality (#76) and Betweenness Centrality (#77) are implemented. The remaining 4 (Closeness, Eigenvector, Communication Strength, Review Network Density) have moved to **Ready to Implement** (R13–R16).
>
> _Visualization note:_ Closeness, Eigenvector, Communication Strength would benefit from a custom D3.js / Cytoscape.js graph component; Superset alone is insufficient for force-directed layouts. Review Network Density (a single scalar) renders fine in Superset.

## Planned — A3bis. Graph/Network in Superset (2)

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A3-6 | Team Coupling | Overlap in commits to same code by different teams | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-7 | Team Cohesion | Whether team members work in same code areas | ✅ Unchanged | 🌐 Universal | Network | P2 |

> **A3-5 (Collaboration Asymmetry Index) is already partially covered by #78 Co-Author Contribution Rate** (which tracks inbound vs outbound co-authorship). Extracting the explicit ratio is trivial — see also A8-4 below which is a duplicate of this metric.
>
> **A3-6 and A3-7 require team membership data** (Jira teams, LDAP groups, or CSV import). Without it, these are not computable. See deferred section D2.

## Planned — A4. DORA & Deployment (4)

> Deployment data (releases + deployments) is now collected by the fetcher. Time to Restore is implemented (see #60). Deployment Frequency moved to **Ready to Implement** (R17). The 4 metrics below have additional unblock requirements:

| # | Metric | Description | AI Status | Structure | Framework | Priority | Blocker |
|---|--------|-------------|-----------|-----------|-----------|----------|---------|
| A4-2 | Lead Time for Changes | Commit to production duration | ✅ Unchanged | 🌐 Universal | DORA | P1 | Sparse deployment data + SHA→PR linkage. Implementable as "deployments-only beta" with caveats. |
| A4-3 | Change Failure Rate | % deployments causing failures | 🔄 Evolved | 🌐 Universal | DORA | P0 | **Missing `status` field** on `DeploymentRecord`. Unblock: extend `fetch_deployments()` to follow each deployment's `statuses_url` field (returned in the base `/deployments` response) and store the latest status. ~30 LOC fetcher change, then trivial implementation. |
| A4-4 | Time to Deploy | PR merge to production deployment | ✅ Unchanged | 🌐 Universal | DORA | P2 | Same SHA-linkage problem as A4-2. |
| A4-5 | Deploy Time | Merge to production release | ✅ Unchanged | 🌐 Universal | DORA | P2 | `target_commitish` is **inconsistent**: some releases use branch names (e.g. "master"), others use commit SHAs. SHA-based releases can be linked directly via `commit.sha` matching; branch-based releases require a commit ancestry index to determine which PRs were ancestors of the branch tip at release time. |

## Planned — A5. Cycle Time Sub-Phases (0)

> Pickup Time (#12) implemented. Time to Approve moved to **Ready to Implement** (R7).

## Planned — A6. Code Quality & Complexity (5)

> Cyclomatic Complexity (A6-3) and AST-Based Duplication (A6-6) moved to **Ready to Implement** (R5, R6) — tree-sitter infrastructure already in deps.

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A6-1 | Code Health Score (1-10) | Multi-factor aggregate (CodeScene's canonical Code Health uses ~28 underlying factors). Implementation here = composite of A6-2, A6-4, A6-5, A6-7 (planned) + R5 Cyclomatic + R6 AST Duplication (ready) + scaling/weighting logic. The "25-30 factor" framing refers to underlying signal count across all components, not component count. | ✅ Unchanged | 🌐 Universal | Traditional | P0 |
| A6-2 | Cognitive Complexity | How difficult code is for a human to understand | 🔄 Evolved | 🌐 Universal | Traditional | P2 |
| A6-4 | LCOM4 (Lack of Cohesion) | Connected components within a class — God Class detector | ✅ Unchanged | 🌐 Universal | Traditional | P2 |
| A6-5 | Technical Debt Ratio | Remediation time / estimated rewrite time | 🔄 Evolved | 🌐 Universal | Traditional | P2 |
| A6-7 | Maintainability Rating (A-F) | Based on technical debt ratio thresholds | ✅ Unchanged | 🌐 Universal | Traditional | P3 |

> **A6-1 Code Health Score** is a *composite metric* — it requires several of the others (A6-2 through A6-7) to be implemented first, plus aggregation logic. P0 priority refers to its strategic value once the components are available.
>
> **A6-2 Cognitive Complexity is partially covered by #50 Complexity Trend** (whitespace-based proxy). True cognitive complexity (per SonarQube/Codacy definition) counts break-the-flow statements; #50 measures indentation depth. They correlate but are not equivalent.
>
> **A6-4 LCOM4, A6-5 Technical Debt Ratio, A6-7 Maintainability Rating** are best served by integrating an external static-analysis tool (SonarQube, Codacy). LCOM4 requires interprocedural class-level analysis; tech debt requires multi-factor estimation.

## Planned — A7. Work Classification (3)

> Innovation Rate (A7-2) and Inefficiency Pool (A7-5) moved to **Ready to Implement** (R8, R9).

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A7-1 | Work Type Breakdown | New Work vs Refactor vs Rework vs Help Others | ✅ Unchanged | 🌐 Universal | Lean | P1 |
| A7-3 | Defect Rate | % merged PRs addressing defects (post-production) | 🔄 Evolved | 🌐 Universal | DevRank | P2 |
| A7-4 | Investment Balance | Time allocation: roadmap vs bugs vs tech debt | ✅ Unchanged | 🌐 Universal | Lean | P2 |

> **A7-1 Work Type Breakdown is partially covered by #32 PR Category Diversity** which classifies PRs into feat/fix/refactor/etc. via conventional commits. A7-1 adds "Help Others" (reviews given to teammates), which would need synthesis with #61 Reviews Given. Implementation = #32 + Reviews Given aggregation.
>
> **A7-3 Defect Rate is partially covered by #33 Bug Fix Focus Rate** (any bug repair, pre- or post-production). True post-production defect rate requires deployment + incident data — see deferred section.
>
> **A7-4 Investment Balance requires PM-tool sprint data** to categorize work items as roadmap/bug/tech-debt/unplanned. See deferred D2.

## Planned — A8. Developer Experience (6)

> Flow Efficiency (#56) implemented. Context Switch Frequency (A8-1) moved to **Ready to Implement** (R10). Collaboration Asymmetry (A8-4) is a duplicate of A3-5 — **removed from this section**.

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A8-2 | Cognitive Load Distribution | How evenly complex work distributes | 🔄 Evolved | 🌐 Universal | SPACE | P2 |
| A8-3 | Decision Latency | Time from problem identification to decision | ✅ Unchanged | 🌐 Universal | DevRank | P3 |
| A8-5 | On-Call Burden | Time/frequency of on-call rotations | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-6 | Time Spent in Meetings | Meeting load as productivity drain | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-7 | Onboarding Time | Time for new hires to reach first productive contribution | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-8 | Work-Life Balance Signals | Late-night/weekend patterns | ✅ Unchanged | 🌐 Universal | SPACE | P3 |

> **A8-2 Cognitive Load Distribution** requires per-PR complexity estimates (depends on A6-2 or A6-3 being implemented first).
>
> **A8-3 Decision Latency** has no clear data source in Git or PR metadata. Could be inferred from issue tracker (D2) or discussion cycles, but the "problem identification" timestamp is undefined.
>
> **A8-8 Work-Life Balance Signals is partially covered by #36 Off-Hours Activity Rate** (basic weekend/night detection). A8-8 could extend to momentum patterns (Monday surges, Friday drops, sustained fatigue) — but the core off-hours signal is already there.

## Planned — A9. PR Quality & Risk (4)

> Discussion Cycles (#19), Delivery Risk Score (#59), Review Coverage (#58) implemented. Productive Impact (A9-4), tt100 (A9-5), PR Maturity Ratio (A9-2), and Idle Completion Time (A9-6) moved to **Ready to Implement** (R11, R12, R20, R21).

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A9-1 | Estimated Review Time | ML-based minutes estimate per PR | 🔄 Evolved | 🌐 Universal | DevRank | P2 |
| A9-3 | Unreviewed PR Rate | % PRs merged with zero review | ⚠️ Caution | 🌐 Universal | DevRank | P2 |
| A9-7 | PRs Unlinked | % PRs not linked to issue tracker | ✅ Unchanged | 🌐 Universal | DevRank | P2 |
| A9-8 | Batch Size Classification | Small/Medium/Large/Gigantic weighted blend | 🔄 Evolved | 🌐 Universal | DevRank | P3 |

> **A9-1 Estimated Review Time** requires training a regression model on historical PR data (size, complexity, file count → review duration). Possible but ML-investment intensive.
>
> **A9-3 Unreviewed PR Rate is partially covered by #23 Self-Merge Rate.** A merged PR with no non-author review *is* a self-merge by definition. The subtle difference: Unreviewed = "zero non-author activity", Self-Merge = "zero non-author approval". A reviewer who comments without approving counts in one but not the other. Implementation = boolean refinement of #23.
>
> **A9-7 PRs Unlinked** requires Jira/Linear integration to verify link validity. Heuristic regex matching (already in `utils.py:515` `_CROSS_REF_RE`) gives a partial signal but high false-negative rate — valid links to issues outside the connected tracker won't match.
>
> **A9-8 Batch Size Classification is partially covered by #13 PR Size Distribution** which already buckets into small/medium/large/XL. A9-8 adds weighted blend (additions + 0.5·deletions) and "Gigantic" tier. Implementation = enhancement to #13.

---

## CI/CD Build Metrics (3) — Deferred Pending Orchestrator Re-Enable

**Status:** Infrastructure exists but disabled. The `fetch_workflow_runs()` method in `impact/providers/github/fetcher.py`, the `CIRunRecord` domain model, the ledger index (`ledger.ci_runs`, `get_ci_runs_for_pr()`), and the writer support (`write_repo_data("ci_runs", ...)`) are all in place. The fetch call in `GitHubLiveFetcher.run()` is commented out.

**What was the problem:**
1. **No metric currently consumes CI data** — fetching ~250K workflow runs/quarter for apache/superset would burn API quota for no user-visible benefit.
2. **PR linkage via `pull_requests[]` field is unreliable** — GitHub's Actions API returns PR numbers from fork repositories (e.g. `TheTechOddBug/superset#280`), not the upstream `apache/superset` PR number. The correct link is via `head_sha` matching, not the `pull_requests[]` array.
3. **High-volume repos require pagination capping** — `fetch_workflow_runs()` already supports `max_pages` (default 10 = 1000 runs); without it a quarterly fetch would page indefinitely.
4. **Date filter format** — Actions API uses `created=YYYY-MM-DD..YYYY-MM-DD`, not the ISO 8601 with `>=`/`<=` qualifiers used elsewhere. This was fixed in the fetcher but documented here for future maintainers.

**What needs to be done to finalize:**
1. Re-enable the `fetch_workflow_runs()` call in `GitHubLiveFetcher.run()` (currently commented out around line 147).
2. Implement three plugin files under `impact/metrics/plugins/authored/`:
   - `build_count.py` — `len(ledger.get_ci_runs(start, end))` per period
   - `build_duration.py` — median + p95 of `run.duration_seconds`
   - `build_success_rate.py` — `count(conclusion='success') / total_completed`
3. Each plugin needs a `no_data` guard for repos with zero CI runs (i.e. teams not using GitHub Actions / GitLab CI / Jenkins).
4. Add thresholds in `impact/thresholds.py`.
5. Use `head_sha` matching (not `pull_requests[]`) when linking CI runs to user PRs for per-PR build status.

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CI1 | Build Count | Number of builds per period (day/week/month), by team/repo/branch. Includes both CI builds (test, lint) and deployment pipelines. Measures team integration rhythm — a team with few builds integrates rarely, a signal of too-large batch size or fear of merging. | 🆕 New | 🌐 Universal | P1 |
| CI2 | Build Duration | Average/median/p95 pipeline execution time from start to completion. Measured per pipeline type (CI, deploy, test suite). A growing trend signals codebase complexity or slow tests — both slow developer feedback and impact cycle time. | 🆕 New | 🌐 Universal | P1 |
| CI3 | Build Success Rate | % of builds completed successfully vs failed, per team/repo/period. Correlated with Change Failure Rate (DORA) but more granular — CFR only measures production deploys, Build Success Rate covers all pipelines. High failure rate on CI indicates flaky tests, broken configs, or unstable code being pushed too often. | 🆕 New | 🌐 Universal | P1 |

---

## Advanced AI Metrics (9)

Emerging in 2026, measuring real AI impact on development. Beyond adoption rate — they measure cost, efficiency, quality, and ROI. No competitor has all of them.

> **Citation status:** specific market figures cited in this section (token cost ranges, ROI benchmarks, acceptance-rate trajectories, code-survival multipliers, the 304K-commit AI-issue study, GitClear's 211M LOC duplication analysis) are drawn from 2026 industry reports and vendor publications. They are intended as **directional benchmarks** for product positioning. When implementing thresholds for any of these metrics, treat the cited numbers as reference points only — validate against the specific customer's own historical data before setting alerts or scoring bands. Specific study attributions to be added once integrations land and we cite live sources from the AI tool API responses.

### Cost and consumption (3)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| AI-A1 | Token Consumption per Developer | Tokens consumed per developer per week/month, by AI tool (Copilot, Cursor, Claude Code, etc.). In 2026, agentic-tool token costs run $200–$2,000+/dev/month — no longer trivial. A VP Engineering needs to know per-dev AI spend and whether it correlates with real output. Growing trend without delivery improvement is a red flag. | 🆕 New | 🌐 Universal | P1 |
| AI-A2 | AI Tool Cost per Merged PR | Average token/dollar cost to produce a PR that ships, broken down by AI tool. Total team token cost ÷ merged PRs in period. Most direct ROI metric: not how much you use AI, but how much each unit of real output costs. Lets you compare tools (Copilot vs Cursor vs Claude Code) on the same team. | 🆕 New | 🌐 Universal | P1 |
| AI-A3 | AI ROI Ratio | Ratio of value produced (time saved × hourly cost) to total AI cost (licenses + tokens + extra review overhead). Healthy ROI is 2.5–3.5x average, 4–6x top quartile. Calculation must include real token costs, not just seat licenses — otherwise ROI is artificially inflated. The metric a VP Engineering brings to the board. | 🆕 New | 🌐 Universal | P1 |

### Adoption and behavior (3)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| AI-A4 | AI Weekly Active Users (WAU) % | % of devs with AI license who actually use the tool at least once in the week. More granular than AI Adoption Rate (★) which measures only license holders. Target is >50% within 90 days of rollout. Low WAU with high licenses = paying for unused tools. WAU trend reveals whether adoption is sustainable or drops off after initial enthusiasm. | 🆕 New | 🌐 Universal | P1 |
| AI-A5 | Power User Density | % of devs using AI tools daily across multiple features (completion + chat + agent + inline edit). Elite orgs exceed 40%. Distinguishes between "tried AI once" and "integrated into daily workflow". A team with high WAU but low Power User Density uses AI superficially. Correlation between Power User Density and delivery metrics reveals whether deep AI use yields better results. | 🆕 New | 🌐 Universal | P2 |
| AI-A6 | AI Acceptance Rate | % of AI suggestions accepted by the developer. Market average 27–30%, improving with use (29% in first 3 months → 34% after 6). Measured per dev, team, and tool. Very low acceptance = tool not useful in context. Very high (>50%) may indicate uncritical acceptance — correlate with AI-Introduced Issue Rate. Requires Copilot Metrics or Cursor logs. | 🆕 New | 🌐 Universal | P1 |

### Code quality (3)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| AI-A7 | AI Code Survival Rate (30/60/90d) | % of AI-generated code that survives without rewrite at 30, 60, 90 days. Evolution of AI Retention Rate (★1) with multiple time windows. AI code surviving 30 days might not survive 90 — multi-window reveals whether debt accumulates. Market data shows expert AI users produce 4–10x more durable code than non-AI users, but variance is enormous. Requires AI commit/PR tagging and survival tracking (similar to Code Survival #55, filtered by AI source). | 🆕 New | 🌐 Universal | P1 |
| AI-A8 | AI-Introduced Issue Rate | % of AI-generated commits introducing at least one issue (smell, bug, vulnerability, duplication). March 2026 research on 304,000+ AI-authored commits shows >15% of commits from every AI assistant introduce at least one issue, and 24% of those issues survive into the latest version — becoming permanent debt. Compare issue rate in AI-tagged vs human commits. Requires linter/SAST integration or post-commit static analysis. The metric that answers "is AI helping or creating debt?" | 🆕 New | 🌐 Universal | P1 |
| AI-A9 | AI Code Duplication Rate | % of AI-generated code that duplicates existing repo code. AI tends to regenerate implementations rather than reuse modules because it lacks full codebase context. GitClear analysis of 211M LOC documented an 8x increase in code duplication correlated with AI use. Compare duplication rate in AI-touched files vs human files. Requires structural code analysis (block hashes, AST matching) or SonarQube integration. | 🆕 New | 🌐 Universal | P2 |

---

## Competitive Gap Metrics — GitKraken Insights (9)

Metrics identified from competitive analysis of GitKraken Insights official documentation (March 2026). Close specific gaps GitKraken uses as sales messaging that are becoming market standard.

> CG1 (PR Comments Count) and CG3 (Commit Count) moved to **Ready to Implement** (R18, R19) — both are trivial counts on existing bundle data with no external dependencies.

### Group 1 — Pull Request extended (1)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG2 | Code Review Hours (Cumulative) | Average hours spent in review per committer (Total review hours ÷ committers). Differs from Review Turnaround Time (#62, single-review duration) and Reviews Given (#61, count). CRH measures cumulative review load — reveals if a team has few "review bottleneck" devs doing most of the work or if load is distributed. Lets a leader balance workload and identify dev overloaded with review vs dev focused on writing code. | 🆕 New | 🌐 Universal | P1 |

### Group 2 — Velocity extended (1)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG4 | Estimated Coding Hours | Estimated real coding hours per dev/team, derived from commit temporal patterns (gaps between consecutive commits, activity clustering, exclusion of inactive periods). Not "hours worked" but "hours actually spent writing code". Use alongside DORA and PR metrics, not standalone — higher hours don't mean higher productivity. Controversial because it can feel like surveillance, so present at team/aggregate level not individual. Useful for understanding real team capacity vs formal hours. | ⚠️ Caution | 🌐 Universal | P3 |

### Group 3 — Code Quality extended (2)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG5 | Code Change Rate (Age of Modified Code) | Average age (days/months) of code modified in new commits. Different from Code Age (A1-1, age of files in repo) — CCR measures how old the code devs are still touching. Frequent modifications to old code reveal tech debt hotspots: legacy code requiring continuous maintenance without ever being replaced. Growing trend signals team is "treating symptoms" on old systems instead of investing in refactoring. Useful for prioritizing architectural work. | 🆕 New | 🌐 Universal | P2 |
| CG6 | Code Change by Operation (per System Layer) | % distribution of coding work by system area: tests, docs, frontend, backend, infrastructure, configuration. Different from Work Type Breakdown (A7-1, classifies by intent) — CCO classifies by where in the system work happens. Reveals whether team invests in balanced way (e.g., 40% feature BE, 30% FE, 20% test, 10% doc) or unbalanced (90% BE, 5% test, 0% doc → debt incoming). Classify files touched in commits by path and extension. | 🆕 New | 🌐 Universal | P2 |

### Group 4 — AI granularity (5)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG7 | Post PR Work Occurring | Volume of work (fix commits, bug reports, rework) AFTER a PR has been merged, linked to the same feature/ticket. Different from Bug Escape Rate (PM15, production-only) and Follow-Up Commit Rate (#22, pre-merge). Captures all "cleanup" following a merge: immediate fixes, emergency refactors, patches for issues that emerged just after deploy. Powerful for evaluating AI-assisted code initial quality: code that passes review but requires many post-merge fixes has hidden quality issues. Correlate merged PRs with subsequent commits/issues on same files or ticket. | 🆕 New | 🌐 Universal | P1 |
| CG8 | AI Suggestions Volume | Total AI suggestions offered to devs in period, measured in suggestion count and lines of suggested code. Different from Token Consumption per Developer (AI-A1, cost) — Suggestions Volume measures supply (what AI proposes), not cost. Serves as denominator for Acceptance Rate and indicator of how "actively" AI is used in workflow. Low volume + high acceptance = selective targeted use. High volume + low acceptance = "AI spam". Requires Copilot Metrics or Cursor API. | 🆕 New | 🌐 Universal | P2 |
| CG9 | Copy/Paste vs Moved Percent | % code duplicated (copy/paste) vs moved/refactored. Specialization of AI Code Duplication Rate (AI-A9) — key distinction is "moved" code is healthy refactoring (extracting logic into reusable module) while "copy/paste" is tech debt (duplicating logic instead of abstracting). AI tends to produce more copy/paste than moved because it generates code in local context without seeing the whole codebase. GitKraken's headline sales metric for "measure AI true impact". Requires advanced diff analysis (not just block comparison but tracking code movement between files). | 🆕 New | 🌐 Universal | P1 |
| CG10 | AI Prompt Acceptance Rate | % of AI suggestions from prompts (chat, AI-assisted composition) accepted. Specialization of AI Acceptance Rate (AI-A6) — GitKraken measures Prompt Acceptance and Tab Acceptance separately. Fundamentally different behaviors: Prompt Acceptance measures prompting capability and conversational AI use, Tab Acceptance measures AI integration in the coding flow. High Prompt + low Tab = uses AI for specific tasks. High Tab + low Prompt = uses AI as advanced autocomplete. Requires Cursor/Copilot APIs that distinguish the two events. | 🆕 New | 🌐 Universal | P1 |
| CG11 | AI Tab Acceptance Rate | % of AI suggestions from inline autocomplete (tab completion in editor) accepted. Specialization of AI Acceptance Rate (AI-A6) — see CG10 description for comparison with Prompt Acceptance. Tab Acceptance is generally higher than Prompt Acceptance (autocompletions are small and contextual; prompts are larger and can fail). Market average for Tab Acceptance is 27–34% — historic reference metric in GitHub Copilot studies. Requires Cursor/Copilot APIs that expose separate inline-completion events. | 🆕 New | 🌐 Universal | P1 |

### Cross-Section Relationships

> **AI-A6 (AI Acceptance Rate) ⊃ CG10 (Prompt Acceptance) + CG11 (Tab Acceptance):** AI-A6 is the parent metric — overall acceptance rate across all suggestion types. CG10 and CG11 are specializations split by suggestion source. Implementation order: AI-A6 first (general) once any AI tool API is connected; CG10/CG11 only viable when the API distinguishes prompt vs tab events (Cursor and recent Copilot SDKs do).
>
> **★1 (AI Retention Rate) ≠ AI-A7 (AI Code Survival Rate):** ★1 measures whether *developers* keep using AI tools after 20 weeks (engagement metric). AI-A7 measures whether *AI-generated code* survives in the codebase at 30/60/90 days (quality metric). Both deferred but for different reasons — ★1 needs WAU tracking; AI-A7 needs AI commit tagging.
>
> **CG7 (Post PR Work Occurring) ≠ #22 (Follow-Up Commit Rate):** CG7 captures work *after* merge on the same files/ticket (post-deploy fixes, emergency patches). #22 captures *pre-merge* commits added in response to review feedback. Different windows, different signals — both useful.
>
> **★2 (AI Code Authorship %) is partially covered by #1 (AI-Assisted PR Rate):** #1 measures % PRs created with AI tools (PR-level). ★2 measures % production *lines* written by AI (line-level). Implementing ★2 requires per-line AI tagging which is harder than PR-level detection.
>
> **★4 (AI Acceptance vs Merge Rate) is a derived comparison, not a standalone metric:** It compares two existing values (#5 AI Suggestion Acceptance vs PR merge rate). Implement as a Superset chart overlay rather than a separate metric plugin.

---

## Lead Time Percentiles (extension of an existing metric)

| Metric | What changes | Structure |
|--------|--------------|-----------|
| **Lead Time Percentiles** (extension of Ticket Lead Time PM14) | Not a new metric. Adds percentile calculation (p50, p75, p85, p95) on top of average. Box Plot in Superset. | 🌐 Universal |

---

## Deferred: Missing Data Sources

Following metrics are blocked by missing data in the current pipeline. Domain models exist or are easy to add — the gating factor is upstream data ingestion.

### D1. Deployment Status / DORA Production Data Required

The DORA deployment metrics (A4-2 Lead Time for Changes, A4-3 Change Failure Rate, A4-4 Time to Deploy, A4-5 Deploy Time) need either status data on deployments or improved commit-to-deployment linkage.

**Status:** the fetcher already pulls `releases.jsonl` and `deployments.jsonl` via the GitHub Releases and Deployments APIs. Domain models `ReleaseRecord`, `DeploymentRecord`, and `CIRunRecord` are defined in `impact/domain/models.py` and indexed by the ledger. Deployment Frequency (A4-1) is now in **Ready to Implement** (R17) with strict no-data guards.

**What's still missing per metric:**
- **A4-3 Change Failure Rate** is the easiest to unblock: extend `fetch_deployments()` in `impact/providers/github/fetcher.py` to also call `GET /repos/{owner}/{repo}/deployments/{id}/statuses` for each deployment, and store the latest status on `DeploymentRecord`. ~30 LOC fetcher change, then trivial implementation.
- **A4-2, A4-4** depend on commit-to-deployment SHA linkage. The data exists (`deployment.sha` matches `commit.sha`) but most apache/superset-style customers won't have enough deployments for meaningful percentile statistics.
- **A4-5 Deploy Time** is the hardest: releases use `target_commitish` which is often a branch name ("master") not a SHA, so linking releases to specific PRs requires a commit ancestry index.

> **Time to Restore (#60)** is implemented as a revert→fix proxy from commit history alone. It is not deployment-aware. When deployment status data is consistently available (after A4-3 unblock), MTTR can be enhanced to detect deployment failure → recovery cycles directly.

### D2. Issue Tracker / PM Tool Data Required

PM-tool metrics (PM1–PM15, V1–V3, K1–K6, S1–S2, W1–W3) and Investment Balance (A7-4) require sprint, ticket, and issue-type data from external project-management tools (Jira, Linear, Asana, Azure DevOps Boards, GitHub Projects, etc.).

> **Unblock path:** Add a Jira/Linear adapter under `impact/providers/`. Define an `IssueRecord` domain model with fields for issue type, sprint, status transitions, and timestamps. Partial inference from PR labels and conventional commit prefixes (`fix:`, `feat:`, `chore:`) is possible for crude type tagging but insufficient for sprint-level accuracy or workflow regression detection.

### D3. CI/CD Pipeline Data Required

CI/CD metrics (CI1–CI3) require GitHub Actions / Jenkins / GitLab CI data. **Detailed problem description and unblock plan are documented inline with the CI/CD Build Metrics section above.** Summary: infrastructure (fetcher, model, ledger) all exist; only the fetch call and three plugins remain.

### D4. AI Tool API Data Required

Advanced AI metrics (AI-A1 through AI-A9, ★1–★6 partially) require integration with Copilot Metrics API, Cursor API, or Claude Code billing logs. ROI metrics (AI-A1, AI-A2, AI-A3) need both license costs and token consumption per developer.

> **Unblock path:** Add per-platform clients under `impact/providers/ai/` (e.g. `copilot_metrics.py`, `cursor.py`, `claude_code.py`). Each platform uses its own auth model and rate limits. Tag commits/PRs with the AI tool source (commit trailer, PR label, or paste-detection heuristic) so survival/quality metrics can scope to AI-generated code.

### D5. External Platform Integration Required

Some Developer Experience metrics need data from systems entirely outside Git/code-review and PM tools:

| Metric | Needs |
|--------|-------|
| On-Call Burden (A8-5) | PagerDuty, Opsgenie, or Grafana OnCall schedule data |
| Onboarding Time (A8-7) | Employee start dates from HR system or directory service |
| Time Spent in Meetings (A8-6) | Calendar data from Google Calendar, Outlook, etc. |
| Comprehension Debt (★3), DXI (★5) | Survey infrastructure (custom or third-party like CultureAmp) |

> **Unblock path:** These require purpose-built integrations with external services, each with their own authentication, rate limiting, and data models. Recommended approach: define a plugin interface in `impact/providers/` for non-Git data sources, with adapters per platform. **Lowest-effort fallback:** accept CSV/YAML imports for on-call schedules, employee directories, and survey results — many customers already export these from their internal systems.

---

## Data Quality Caveats

PM-tool metrics depend on the quality of data in the customer's tool. DevRank must explicitly flag when data is insufficient.

| Data quality issue | Affected metrics |
|--------------------|------------------|
| Tickets without creation dates | Ticket Cycle Time, Lead Time, Backlog Age |
| Workflow states not updated | Time in State, Workflow Regression Rate, Aging WIP |
| Tickets without estimates | Estimation Accuracy |
| PRs not linked to tickets | PR to Ticket Ratio, Time to First Commit |
| Tickets without type (bug/feature/chore) | Ticket Throughput per type, Bug Trend |
| No WIP limits configured | WIP Compliance Rate (not computable) |
| No sprints configured | Planning Reliability (Scrum variant), Sprint Burndown |
| No CI/CD pipeline connected | Build Count, Build Duration, Build Success Rate |
| No AI tool connected / no AI API data | Token Consumption, AI Cost per PR, WAU %, Acceptance Rate, AI ROI |
| Commits/PRs not tagged AI-generated | AI Code Survival Rate, AI-Introduced Issue Rate, AI Code Duplication Rate |

---

## Architecture: Metrics by Methodology and Data Source

```
team_config.methodology = 'scrum' | 'kanban' | 'waterfall' | 'hybrid'

Data sources:
  - Git (GitHub/GitLab): commits, PRs, reviews, branches, files — ~78 implemented + ~30 planned
  - PM Tool (Jira/Linear): tickets, sprints, workflow — 17 PM metrics + variants
  - Git + PM Tool (cross-source): PR↔ticket mapping — 4 metrics
  - CI/CD (GitHub Actions/Jenkins/GitLab CI): builds, pipelines — 3 metrics
  - AI Tool API (Copilot/Cursor/Claude Code): tokens, cost, usage, acceptance — 9 metrics
  - Survey (DXI, Comprehension Debt): developer feedback — 2 metrics

ETL pipeline:
  1. Read team_config.methodology
  2. Compute universal metrics (identical for all teams)
  3. Compute 3 variant metrics (different formula per methodology)
  4. Compute methodology-specific metrics for the configured methodology only
  5. Write everything to devrank_metrics with uniform metric_name

Progressive onboarding by data source:
  - Phase 1 (Day 1): Connect GitHub → 78 Git metrics live
  - Phase 2: Connect PM Tool → +17 PM metrics + 4 Git+PM cross-source
  - Phase 3: Connect CI/CD → +3 build metrics
  - Phase 4: Connect AI Tool API → +9 advanced AI metrics
  - Phase 5: Enable surveys → +2 experience metrics

Superset:
  - Separate dashboards per methodology at Team level
  - devrank-team-scrum: universal + variants + S1, S2
  - devrank-team-kanban: universal + variants + K1–K6
  - devrank-team-waterfall: universal + variants + W1–W3
  - Universal charts shared across dashboards (reusable Superset entities)
  - Methodology-specific charts only in pertinent dashboard
  - CI/CD and AI metrics: dedicated tab or section per dashboard
  - Zero empty charts, zero additional custom code
```

---

## Count Summary

| Category | Count |
|----------|-------|
| **Implemented (Authored 60 + Influence 17 + Mixed 1)** | **78** |
| **Ready to Implement (R1–R21)** | **21** |
| Planned (A3bis 2 + A4 4 + A6 5 + A7 3 + A8 6 + A9 4) | 24 |
| New AI-Era (★1–★6) | 6 |
| New PM Tool (PM1–PM15, includes V2=PM10 and V3=PM9) | 15 |
| Methodology Variants (V1 only — V2/V3 counted under PM Tool) | 1 |
| Kanban-only (K1–K6) | 6 |
| Scrum-only (S1–S2) | 2 |
| Waterfall-only (W1–W3) | 3 |
| CI/CD Build (CI1–CI3) | 3 |
| Advanced AI (AI-A1–AI-A9) | 9 |
| Competitive Gap GitKraken (CG2, CG4–CG11) | 9 |
| Lead Time Percentiles (extension) | 1 |
| **Total** | **178** |

| By structural category | Count |
|------------------------|-------|
| 🌐 Universal | ~163 |
| 🔀 Variant | 3 (V1 + PM9/V3 + PM10/V2 — last two counted as PM, not double-counted) |
| 📋 Kanban-only | 6 |
| 🏃 Scrum-only | 2 |
| 🏗️ Waterfall-only | 3 |
| Custom graph component (D3.js/Cytoscape) | 3 (R13, R14, R15) |

| By data source | Count |
|----------------|-------|
| Git only (GitHub/GitLab) | ~135 |
| PM Tool (Jira/Linear) | 15 |
| Git + PM Tool combined | 4 (PM12–PM15) |
| CI/CD (GitHub Actions/Jenkins/GitLab CI) | 3 |
| AI Tool API (Copilot/Cursor/Claude Code) | 9 (AI-A1–AI-A9) |
| Survey (DXI ★5, Comprehension Debt ★3) | 2 |
| Mixed Git + Custom Viz | 3 |

---

## Framework Coverage Summary

> Implemented counts come directly from the live metric registry (each plugin's `frameworks` property). Metrics can belong to multiple frameworks (e.g., Cycle Time is DORA + SPACE + Lean) so totals exceed metric count due to multi-tagging. Planned + Ready columns are estimates based on framework column in the planned tables.

| Framework | Implemented | Ready (R1–R21) | Planned (A-sections) | Other (PM/CI/AI/CG/etc.) |
|-----------|-------------|----------------|----------------------|---------------------------|
| DORA      | 2           | 1 (R17)        | 4 (A4-2, A4-3, A4-4, A4-5) | 0                   |
| SPACE     | 18          | 1 (R10)        | 6 (A8-2,A8-3,A8-5,A8-6,A8-7,A8-8) | 1 (★5 DXI)     |
| CodeScene | 17          | 3 (R1, R2, R3) | 0                    | 0                         |
| Lean      | 15          | 5 (R7, R9, R11, R12, R21) | 2 (A7-1, A7-4) | 0                       |
| Kanban    | 2           | 0              | 0                    | 6 (K1–K6)                 |
| Traditional | 2         | 3 (R4, R5, R6) | 5 (A6-1, A6-2, A6-4, A6-5, A6-7) | 0             |
| Network   | 6           | 4 (R13, R14, R15, R16) | 2 (A3-6, A3-7) | 0                       |
| DevRank   | 30          | 4 (R8, R18, R19, R20) | 5 (A9-1, A9-3, A9-7, A9-8, A7-3) | many (CG, ★, AI-A) |

### Notes on Framework Overlap

- **DORA metrics** are a subset of SPACE (Performance/Efficiency) and Lean (flow metrics).
- **CodeScene metrics** often overlap with SPACE's Communication dimension (knowledge sharing, ownership distribution).
- **Lean metrics** heavily overlap with SPACE's Efficiency and Activity dimensions.
- **Network metrics** primarily map to SPACE's Communication & Collaboration dimension.
- **DevRank-specific metrics** fill gaps in existing frameworks, particularly around AI-assisted development and PR-level quality signals (PR Body Quality Score, Discussion Cycles, AI Phantom Ownership).

---

*DevRank Metrics Overview v5 — updated April 2026*
*Sources: previous METRICS_OVERVIEW.md + competitive analysis of GitKraken Insights (March 2026 official docs) + 2026 emerging AI metrics research + PM tool integration design + live metric registry verification*
