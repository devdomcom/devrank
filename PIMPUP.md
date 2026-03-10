# PIMPUP: DevRank Gap Analysis vs. State-of-the-Art Engineering Analytics

> Comprehensive competitive intelligence compiled March 2026.
> Sources: Open-source codebases (code-maat, Sourcegraph, GitStream, Code Climate), platform documentation (LinearB, Swarmia, Pluralsight Flow, CodeScene, Jellyfish, DX, Sleuth, Haystack, Faros AI, CodeSee), and academic research (SPACE, DX Core 4, developer social networks).

**Verification status:** All gaps verified against DevRank source code (46 metric plugins, utils.py, ledger, providers, adapters). ~55 items confirmed as **REAL GAPS**, ~10 as **PARTIAL GAPS** (DevRank has related but not equivalent functionality), 1 as **FALSE GAP** (Defect Rate — `bug_fix_focus_rate` already covers this). Items marked `PARTIAL GAP` inline where applicable.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [What DevRank Already Does Well](#2-what-devrank-already-does-well)
3. [Gap Category A: Missing Metrics](#3-gap-category-a-missing-metrics)
4. [Gap Category B: Missing Features & Capabilities](#4-gap-category-b-missing-features--capabilities)
5. [Gap Category C: What Competitors Do Better](#5-gap-category-c-what-competitors-do-better)
6. [Detailed Competitor Breakdown](#6-detailed-competitor-breakdown)
7. [Priority Roadmap](#7-priority-roadmap)

---

## 1. Executive Summary

DevRank implements **46 metrics** across 7 categories (6 scored + 1 contextual). This is already one of the deepest individual-contributor metric sets in the industry. However, gaps exist in **five major areas**:

| Gap Area | Severity | Industry Coverage |
|----------|----------|-------------------|
| **Graph/Network-based collaboration analysis** | Critical | CodeScene, academic research, code-maat |
| **Codebase evolution metrics** (hotspots, coupling, age, complexity trends) | Critical | CodeScene/code-maat (26 analyses), Sourcegraph |
| **Knowledge ownership & risk** (bus factor, knowledge islands, knowledge loss) | High | CodeScene, Pluralsight Flow, Sourcegraph |
| **DORA metrics & deployment pipeline** | High | All major platforms (LinearB, Swarmia, Sleuth, etc.) |
| **Team-level & organizational metrics** | Medium | Swarmia, LinearB, Jellyfish, Pluralsight Flow |

DevRank's **unique strengths** — per-developer continuous 0-100 scoring, tree-sitter function-level analysis, 3-phase deterministic static analysis, and granular influence metrics — are unmatched by any single competitor. The opportunity is to layer the missing dimensions on top of this already-strong foundation.

---

## 2. What DevRank Already Does Well

These are areas where DevRank meets or exceeds the state of the art:

| Capability | DevRank | Closest Competitor |
|------------|---------|-------------------|
| **Per-developer continuous scoring (0-100)** | All 45 rated metrics | LinearB uses 4-tier buckets; no competitor does continuous |
| **Tree-sitter function-level analysis** | Phase 3: Python, JS/TS, Go, Rust, Java | CodeScene does function-level but proprietary |
| **Influence/review metrics depth** | 14 dedicated influence metrics | Pluralsight Flow has 4-5 review metrics |
| **Review comment substance scoring** | Pygments-based code detection in comments | Code Climate counts words (<8 = trivial) |
| **Mentorship signal detection** | Targets reviews of low-activity contributors | No competitor has this |
| **Generated-file filtering** | Deterministic detection in Phase 1 | GitStream auto-excludes lock files only |
| **Self-review filtering** | All influence metrics filter `pr.user.login != context.user_login` | Most platforms don't address this |
| **No-data guards** | Combined period+count guards on all metrics | Most platforms show zeros or N/A |
| **Structural diff classification** | Test code, dependency files, documentation detection | GitStream has `allTests`/`allDocs` booleans |
| **Rework rate with context period** | Uses actual start_date/end_date | LinearB/Flow use fixed 21-30 day windows |

---

## 3. Gap Category A: Missing Metrics

### A1. Codebase Evolution Metrics (from CodeScene / code-maat)

These metrics analyze how code changes over time. **None** are currently in DevRank.

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Hotspot Detection** | Files with highest revision frequency × complexity — where tech debt hurts most | code-maat `revisions` + complexity merge | **P0** |
| **Temporal/Logical Coupling** | Files that always change together (hidden dependencies). `shared_revisions / avg_revisions × 100` | code-maat `coupling` | **P0** |
| **Code Age** | Months since last modification per file. Code should be very old (stable) or very fresh (recently worked on) | code-maat `age` | **P1** |
| **Entity Fragmentation** | Herfindahl-like index: `1 - Σ(author_revs/total)²`. 0 = one author, ~1 = many scattered authors. Better than raw author count | code-maat `fragmentation` | **P1** |
| **Complexity Trend** | Whitespace-based complexity (language-neutral) tracked per file over time: mean, max, std dev | maat-scripts `git_complexity_trend.py` | **P1** |
| **Change Proximity** | Sum of distances between changed lines within a file. High = scattered risky changes, Low = concentrated safe changes | maat-scripts `git_proximity_analysis.py` | **P2** |
| **Sum of Coupling (SoC)** | Per-entity total coupling score across all revisions — prioritizes most entangled files | code-maat `soc` | **P2** |
| **Absolute Churn Trend** | Lines added/deleted per date — detects integration bottleneck periods | code-maat `abs-churn` | **P2** |
| **Commit Message Mining** | Regex search of commit messages per file (e.g., "bug", "fix", "error") to estimate defect distribution | code-maat `messages` | **P3** |

**Additional codebase evolution metrics from OSS tools:**

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Code Survival / Cohort Analysis** | % of a developer's contributed lines still alive over time — shows code half-life and durability of contributions | git-of-theseus | **P2** |
| **History Complexity (Entropy)** | Normalized entropy of changes across files — measures how spread out changes are (IEEE HCPF metric) | PyDriller | **P2** |
| **Hunks Count (Change Fragmentation)** | Median diff hunks per file — scattered hunks = higher risk than concentrated changes | PyDriller | **P3** |
| **Delta Maintainability Model** | Per-function unit size, unit complexity, and unit interfacing via lizard — cyclomatic complexity at function level | PyDriller | **P2** |
| **Contributor Experience** | % of lines authored by the highest contributor per file — ownership concentration | PyDriller | **P3** |

**Why this matters:** CodeScene's research shows unhealthy code (hotspots) has **15× more defects**, **2× slower development**, and **10× more delivery uncertainty**. code-maat has been used in production for 10+ years.

### A2. Knowledge Ownership & Risk Metrics

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Bus Factor** | Minimum developers who could leave before code becomes unmaintainable | CodeScene, Truck-Factor tool, academic research | **P0** |
| **Knowledge Islands** | Files/modules where 95%+ of code was written by a single contributor | CodeScene | **P0** |
| **Knowledge Loss** | Code where 50%+ was written by departed/inactive contributors | CodeScene | **P1** |
| **Main Developer (by lines)** | Primary author per file by most lines added + ownership ratio | code-maat `main-dev` | **P1** |
| **Main Developer (by revisions)** | Primary author per file by most commits | code-maat `main-dev-by-revs` | **P2** |
| **Entity Ownership** | Per-author contribution percentages per file (churn-based) | code-maat `entity-ownership` | **P2** |
| **Knowledge Sharing Index** | How thoroughly reviews are distributed across team members. Scale 0-1 (0 = siloed, 1 = well-distributed) | Pluralsight Flow | **P1** |
| **Code Familiarity** | Percentage of codebase known by current active team | CodeScene | **P2** |

**Why this matters:** Academic research on 133 popular GitHub projects found **65% have bus factor ≤ 2**. CodeScene's off-boarding simulator lets teams proactively identify risk before attrition hits.

### A3. Graph/Network Collaboration Metrics

DevRank has 14 influence metrics but **zero graph-based analysis**. This is the biggest structural gap.

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Degree Centrality** | Number of direct collaborators a developer has | Academic DSN research | **P0** |
| **Betweenness Centrality** | Whether a developer is a "bridge" connecting otherwise disconnected teams | Academic DSN research | **P0** |
| **Closeness Centrality** | How quickly a developer can reach/influence the entire org | Academic DSN research | **P1** |
| **Eigenvector Centrality** | Influence through association with other influential developers | Academic DSN research | **P2** |
| **Communication Strength** | Conway's Law heuristic: `shared_commits / ceil(avg(my_commits, peer_commits)) × 100` | code-maat `communication` | **P1** |
| **Review Network Density** | How interconnected the review graph is (all-pairs review relationships) | Academic SNA research | **P1** |
| **Collaboration Asymmetry Index** | Help-giving vs help-receiving ratio. >3:1 → knowledge silos | Predictive metrics research | **P2** |
| **Team Coupling** | Overlap in commits to identical code by different teams — coordination bottleneck | CodeScene | **P2** |
| **Team Cohesion** | Whether team members work in the same parts of the code — architectural alignment | CodeScene | **P2** |

**Why this matters:** Academic research shows social factors (closeness, betweenness, clustering coefficient) have **evident correlation with developer productivity** in both local and global contexts. No production SaaS tool has fully productionized these — this is a differentiation opportunity.

### A4. DORA & Deployment Metrics

Every major competitor implements DORA. DevRank has **zero deployment-related metrics**.

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Deployment Frequency** | How often code deploys to production | DORA, all platforms | **P1** |
| **Lead Time for Changes** | Commit to production duration | DORA, all platforms | **P1** |
| **Change Failure Rate** | % deployments causing failures | DORA, all platforms | **P1** |
| **Mean Time to Recovery (MTTR)** | Incident recovery duration | DORA, all platforms | **P1** |
| **Time to Deploy** | PR merge to production deployment | Swarmia | **P2** |

**Note:** DORA requires CI/CD and incident management integration (GitHub Actions, PagerDuty, etc.). This is a data source expansion, not just a metric addition.

### A5. Cycle Time Sub-Phase Metrics

DevRank has `cycle_time` (creation to merge) and `coding_time_to_pr`. LinearB and others break this into **4-5 granular phases**.

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Pickup Time** | PR opened to first non-author review activity | LinearB, Swarmia | **P1** |
| **Time to Approve** | First review activity to first approval | LinearB | **P2** |
| **Time to Merge (post-approval)** | First approval to merge | LinearB | Already have as `merge_delay` |
| **Deploy Time** | Merge to production release | LinearB, Swarmia | **P2** (requires DORA) |

**DevRank already has:** `cycle_time`, `coding_time_to_pr`, `merge_delay`, `time_to_first_review`, `review_turnaround_time`. Note: `review_turnaround_time` already uses `review_requested` timeline events when available, making it functionally close to pickup time from the reviewer's perspective. The remaining gap is **pickup time as an authored metric** — "how quickly did someone pick up MY PR" (author-side), which is subtly different from `review_turnaround_time` (reviewer-side influence metric). **PARTIAL GAP.**

### A6. Code Quality & Complexity Metrics

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Code Health Score** (1-10) | 25-30 factor aggregate including brain classes, DRY violations, nested complexity, bumpy road | CodeScene | **P1** |
| **Cognitive Complexity** | How difficult code is for a human to understand. SonarSource spec: +1 for `else`/`elsif`/label-jumps/boolean operator changes; +nesting_level for `if`/ternary/switch/loop/catch. Else-if chains don't increase nesting. | Code Climate/Qlty (Rust impl), CodeScene | **P2** |
| **Cyclomatic Complexity** | Linearly independent paths through code. McCabe: starts at 1, +1 for each `if`/`elsif`/`ternary`/`case`/`loop`/`catch`/boolean operator. Qlty also counts iterator method calls (`.map`, `.filter`). | Code Climate/Qlty (Rust impl), CodeScene | **P2** |
| **LCOM4 (Lack of Cohesion)** | Connected component count within a class — methods sharing fields or calling each other form groups. Higher = more responsibilities in one class (God Class signal). | Code Climate/Qlty (Rust impl) | **P2** |
| **Technical Debt Ratio** | Remediation time / estimated rewrite time (COCOMO model). Effort formula: `base_minutes + (value_delta × minutes_per_delta)`. Grades: A (<5%), B (5-10%), C (10-20%), D (20-50%), F (≥50%). | Code Climate Quality | **P2** |
| **AST-Based Duplication** | Structural hashing: MD5 of (node kind + children hashes) for every AST node with mass ≥ threshold. Identical = same source hash; Similar = same structural hash but different source. Pruning removes subtree duplicates. | Code Climate/Qlty (Rust impl) | **P3** |
| **Maintainability Rating** (A-F) | Based on technical debt ratio thresholds | Code Climate Quality | **P3** |

**Note:** DevRank already does tree-sitter function-level analysis (trivial detection, function identity) for Python, JS/TS, Go, Rust, Java. The Qlty/Code Climate codebase proves these complexity metrics can be built on tree-sitter — same foundation DevRank uses. LCOM4 is particularly interesting as a God Class detector.

### A7. Work Classification & Investment Metrics

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Work Type Breakdown** | New Work vs Legacy Refactor vs Rework vs Help Others (by line age + authorship) | Pluralsight Flow, LinearB | **P1** |
| **Innovation Rate** | % merged PRs representing new feature work | Code Climate Velocity | **P2** |
| **Defect Rate** | % merged PRs addressing defects (keyword detection: "fix", "revert", "bug") | Code Climate Velocity | **P2** |
| **Investment Balance** | Engineering time allocation: roadmap vs bugs vs tech debt vs unplanned | Swarmia, LinearB | **P2** |
| **Inefficiency Pool** | PR idle time, friction, wasted effort | LinearB | **P3** |

**DevRank already has:** `bug_fix_focus_rate` (similar to Defect Rate). The main gap is the New Work / Refactor / Rework / Help Others breakdown based on line age analysis.

### A8. Developer Experience & Wellbeing Metrics

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Context Switch Frequency** | >5 major intra-day switches between repos/projects → 30% productivity drop. Note: DevRank's `burstiness` measures weekly burst ratios (max/avg), NOT intra-day switching — this is a distinct concept. | Academic research | **P2** |
| **Cognitive Load Distribution Index** | How evenly complex work distributes. >80% by 2-3 people → burnout | Predictive research | **P2** |
| **Flow Efficiency** | % of days an issue was actively worked on vs total lifetime | Swarmia | **P2** |
| **Decision Latency** | Time from problem identification to decision. >72 hours → 45% more rework | Predictive research | **P3** |
| **Collaboration Asymmetry** | Help-given vs help-received ratio. >3:1 → knowledge silos | Predictive research | **P3** |

**Additional wellbeing/experience metrics from platform research:**

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **On-Call Burden** | Time and frequency of on-call rotations per developer | Jellyfish | **P3** |
| **Time Spent in Meetings** | Meeting load as a developer productivity drain | Jellyfish | **P3** |
| **Onboarding Time** | Time for new hires to reach first productive contribution | Jellyfish | **P3** |
| **Work-Life Balance Signals** | Late-night commits, weekend work patterns (beyond off_hours_activity) | Jellyfish, Haystack | **P3** |

**DevRank already has:** `off_hours_activity_rate` (burnout signal), `burstiness` (pacing/sustainability). These would extend the wellbeing dimension.

### A9. PR Quality & Risk Metrics

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Estimated Review Time** | ML-based minutes estimate per PR | GitStream/LinearB | **P2** |
| **PR Maturity Ratio** | How much a PR changes between open and merge | LinearB | **P2** |
| **Unreviewed PR Rate** | % PRs merged with zero review (benchmark: median 11%). **PARTIAL GAP**: DevRank's `self_merge_rate` checks for merges without any non-author approval, which is functionally very close. Gap is smaller than it appears. | Code Climate, Pluralsight Flow | **P2** |
| **Delivery Risk Score** (1-10) | Per-commit risk based on code changed, file count, diffusion, developer experience | CodeScene | **P2** |
| **Review Coverage** | % of PR files/hunks receiving at least one review comment | Code Climate Velocity | **P2** |
| **Discussion Cycles** | Alternating-person comment exchanges (A→B→A = 2 cycles) | Code Climate Velocity | **P2** |
| **Productive Impact** | Impact minus rework: `Impact × (1 - Rework Rate)` | Code Climate Velocity | **P2** |
| **tt100** (Time to 100) | Time for an engineer to write 100 lines of productive code | Pluralsight Flow | **P3** |

**Additional PR quality metrics from platform research:**

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **Idle Completion Time** | Time from rework complete to merge — post-review waiting/idle | Haystack | **P2** |
| **PRs Unlinked** | % PRs not linked to issue tracker — traceability enforcement | Haystack | **P2** |
| **Commitment Reliability Rate** | Sprint completion excluding injected issues — true reliability | Haystack | **P3** |
| **Batch Size Classification** | Small/Medium/Large/Gigantic using weighted blend of PRs, commits, lines | Sleuth | **P3** |

**DevRank already has:** `self_merge_rate` (very close to unreviewed PR rate — checks merges without non-author approval), `review_iterations` (counts change-request rounds, which is different from Discussion Cycles that count alternating comment exchanges), `pr_body_quality_score`. Main gaps are estimated review time and delivery risk scoring.

### A10. AI-Assisted Development Metrics

Emerging category (2025-2026) — most platforms are adding this rapidly.

| Metric | What It Measures | Source | Priority |
|--------|-----------------|--------|----------|
| **AI Adoption Rate** | % team members with AI coding tool licenses | Swarmia, LinearB | **P2** |
| **AI-Assisted PR Rate** | % PRs created with Copilot/Cursor/Claude Code | Swarmia, LinearB | **P2** |
| **AI Code Quality** | Rework rate on AI-assisted vs human PRs | LinearB | **P3** |
| **AI Suggestion Acceptance** | Ratio of accepted vs dismissed AI suggestions | LinearB | **P3** |

---

## 4. Gap Category B: Missing Features & Capabilities

### B1. Industry Benchmarks

**Gap:** DevRank has thresholds for rating individual metrics but no cross-organization benchmark data.

| Competitor | Benchmark Data |
|-----------|---------------|
| **LinearB** | 8.1M+ PRs, 4,800 teams, 42 countries — 4 tiers (Elite/Strong/Fair/Needs Improvement) |
| **Code Climate** | 15 metrics benchmarked with quartile data from hundreds of orgs |
| **Pluralsight Flow** | Typical vs Leading benchmarks for all major metrics |
| **Swarmia** | Industry benchmarks for DORA and flow metrics |

**Recommendation:** Build a benchmark dataset from anonymized customer data. Even curated open-source repo benchmarks (e.g., apache/superset, kubernetes/kubernetes) would be valuable.

### B2. Trend Analysis & Time-Series Tracking

**Gap:** DevRank computes point-in-time metrics per assessment. Competitors track metrics **over time** with trend visualization.

| Feature | Who Has It |
|---------|-----------|
| Complexity trends per file over commit history | CodeScene, maat-scripts |
| Metric trend dashboards with directional indicators | All major platforms |
| Sprint-over-sprint comparison | Swarmia, LinearB, Pluralsight Flow |
| Annotation system for marking process changes | Code Climate Velocity |

**Recommendation:** Store assessment results in time-series format to enable trend analysis and regression detection.

### B3. Working Agreements & Goal Setting

**Gap:** DevRank has no mechanism for teams to define target values and track progress.

| Feature | Who Has It |
|---------|-----------|
| Working agreements (team-defined targets with tracking) | Swarmia (10+ agreement types) |
| Goal setting with progress dashboards | LinearB, Code Climate |
| Supervised refactoring goals | CodeScene |
| Sprint targets vs actuals | Swarmia, Pluralsight Flow |

### B4. Architectural Analysis

**PARTIAL GAP:** DevRank has primitive architectural awareness via `module_area_breadth` and `detect_module_boundary()` (maps file paths to top-level directories or "root"). However, it lacks configurable architectural grouping and architecture-level analysis.

| Feature | Who Has It |
|---------|-----------|
| Logical building block mapping (file paths → architectural layers) | CodeScene, code-maat `--group` |
| Architecture-level hotspots and coupling | CodeScene |
| Service maps / dependency visualization | CodeSee, Sourcegraph |
| Technical sprawl (language distribution per component) | CodeScene |
| Conway's Law alignment (team boundaries vs code boundaries) | CodeScene |

**Recommendation:** Add support for a group mapping file (like code-maat's `--group`) that maps file paths to logical components. All existing metrics could then be aggregated at the architectural level.

### B5. Team-Level Aggregation

**Gap:** DevRank is individual-focused. Many organizations need team-level views.

| Feature | Who Has It |
|---------|-----------|
| Team dashboards with aggregated metrics | All major platforms |
| Team-to-team comparison | LinearB, Swarmia, Code Climate |
| Team mapping (authors → teams via CSV/config) | code-maat, CodeScene |
| Team coupling / cohesion metrics | CodeScene |
| Normalized throughput (per FTE) | Swarmia |

### B6. Developer Experience Surveys

**Gap:** DevRank is purely behavioral data. Several competitors combine quantitative metrics with qualitative surveys.

| Feature | Who Has It |
|---------|-----------|
| DevEx surveys (32 built-in questions, SPACE-aligned) | Swarmia |
| Developer Experience Index (DXI) — 14-question survey, benchmarkable | DX (getdx.com) |
| DevSat measurement with triage workflow | DX |
| Experience sampling (continuous lightweight data collection) | DX |

**Note:** This is a significant product expansion. Each 1-point DXI improvement saves 13 minutes/developer/week (DX research from 40,000 developers).

### B7. Notification & Alerting System

**Gap:** DevRank has no real-time alerting.

| Feature | Who Has It |
|---------|-----------|
| Slack/Teams notifications for review requests | Swarmia, LinearB WorkerB |
| Daily digest of in-progress work | Swarmia, LinearB |
| PR risk alerts (large PR, stale, unreviewed) | Code Climate, LinearB |
| Working agreement violation alerts | Swarmia |
| CI failure notifications with logs | Swarmia |

### B8. Project/Initiative Tracking

**Gap:** DevRank doesn't connect engineering metrics to project delivery.

| Feature | Who Has It |
|---------|-----------|
| Cross-team initiative monitoring | Swarmia Initiatives |
| Monte Carlo delivery forecasting | LinearB, Jellyfish |
| Sprint/iteration analytics | Swarmia, Pluralsight Flow |
| Planning accuracy (planned vs delivered) | LinearB |
| Scope creep measurement | Swarmia |
| Capacity planning | Jellyfish |

### B9. Off-boarding & Simulation

**Gap:** DevRank cannot simulate team changes.

| Feature | Who Has It |
|---------|-----------|
| Off-boarding simulator (simulate developer departure impact) | CodeScene |
| Team restructuring impact analysis | CodeScene |
| Language expertise risk (departing staff with exclusive language skills) | CodeScene |

### B10. PR Workflow Automation

**Gap:** DevRank is analysis-only. Several competitors include automation.

| Feature | Who Has It |
|---------|-----------|
| Auto-assign reviewers based on code expertise | GitStream, CodeSee |
| Auto-approve safe changes (docs, formatting, tests) | GitStream |
| Auto-label PRs by language/area/size | GitStream |
| Knowledge-share reviewer assignment (30-60% familiarity) | GitStream |
| PR quality gates (block merge on health degradation) | CodeScene, GitStream |

---

## 5. Gap Category C: What Competitors Do Better

### C1. CodeScene — Best at Codebase Health & Risk

**What they do better:**
- **Code Health Score**: 25-30 factor aggregate (brain classes, DRY violations, bumpy road, large methods, nested complexity) scored 1-10 per file. DevRank does function-level trivial detection but doesn't quantify complexity.
- **Hotspot prioritization**: Combines revision frequency with code health to identify where tech debt hurts most. DevRank doesn't track revision frequency at all.
- **Knowledge distribution**: Full suite — knowledge islands, knowledge loss (departed contributors), bus factor, off-boarding simulation. DevRank has zero knowledge ownership metrics.
- **Temporal coupling**: Detects hidden dependencies between files that change together. DevRank only looks at individual PRs.
- **Delivery risk prediction**: ML-based per-commit risk scoring (1-10) considering code changed, developer experience, diffusion across subsystems. Identical changes scored differently based on author experience.

**Why:** CodeScene is built on 10+ years of research (Adam Tornhill's "Your Code as a Crime Scene"). Their analysis is forensic — looking at behavioral patterns in version control history, not just current snapshots.

### C2. LinearB — Best at Cycle Time Decomposition & Benchmarks

**What they do better:**
- **Cycle time granularity**: 4 sub-phases (coding/pickup/review/deploy) each benchmarked independently. DevRank has `cycle_time` as a single number plus `coding_time_to_pr` and `merge_delay`.
- **Industry benchmarks**: 8.1M+ PRs across 4,800 teams provide statistically significant benchmark tiers. DevRank has threshold-based ratings but no external benchmark data.
- **Work breakdown**: Classifies all code changes into New/Refactor/Rework using 21-day line age analysis. DevRank's `rework_rate` only covers rework, not the full breakdown.
- **Investment strategy**: Automatic classification of engineering effort into 5 strategic categories from issue tracker data. DevRank has no project management integration.

**Why:** LinearB has the largest dataset for benchmarking (network effect) and deep PM tool integration that DevRank lacks.

### C3. Pluralsight Flow — Best at Impact Measurement Formula

**What they do better:**
- **Diff Delta (Impact)**: 6 multiplicative factors (noise filter, base score, length weight, time factor, context, redistribution) providing a sophisticated single-number "cognitive load" estimate. DevRank's `net_code_contribution` is raw add-del without weighting.
- **Knowledge Sharing Index**: 0-1 scale measuring review distribution across team. DevRank's `review_breadth` counts distinct authors reviewed but doesn't normalize for team size or distribution evenness.
- **20 Behavioral Patterns**: Codified pattern library (Domain Champion, Heroing, Bit Twiddling, Rubber Stamping, etc.) that maps metric combinations to actionable narratives. DevRank has metrics but no pattern detection layer.
- **tt100 (Time to 100)**: Novel productivity metric — time to write 100 lines of productive code, normalized for active coding time.

**Why:** GitPrime/Flow had a 7+ year head start on refining their impact formula through research partnerships and customer feedback.

### C4. Swarmia — Best at Team Process & Developer Experience

**What they do better:**
- **Working agreements**: 10+ configurable team-level commitments (WIP targets, review time targets, batch size targets) with daily Slack digests and exception tracking. DevRank has no team process layer.
- **Investment balance**: Visual allocation tracking (roadmap vs bugs vs tech debt) with manual or automatic categorization plus AI-powered auto-categorization. DevRank has no work categorization.
- **Developer surveys**: 32 SPACE-aligned questions with psychometrics validation, correlation with system metrics. DevRank is purely behavioral.
- **Scope creep measurement**: Tracks child issues added after work started. DevRank has no project-level visibility.

**Why:** Swarmia is team-process-first rather than individual-metrics-first. Different philosophy but addresses real organizational needs.

### C5. Code Climate Velocity — Best at Review Analytics & Benchmarks

**What they do better:**
- **Review analytics depth**: Review Coverage (% hunks commented), Review Influence (% comments leading to code changes), Discussion Cycles (alternating-person exchanges), comment size classification (Large/Regular/Trivial). DevRank has `review_comment_substance` and `inline_comment_density` but not these specific angles.
- **File Hotspots (2×2)**: Places every file in a quadrant: change frequency × number of people who understand it. Simple but effective prioritization.
- **Productive Impact**: `Impact × (1 - Rework Rate)` — separates gross output from net productive output.
- **PR Risks**: Configurable real-time risk detection (High Rework, Too Many Cooks, Large Change, Inactive).
- **15 benchmarked metrics with quartile data** from hundreds of organizations.

### C6. Sourcegraph — Best at Code Ownership Signal Fusion

**What they do better:**
- **Multi-signal ownership**: Combines CODEOWNERS files + recent contributors (90-day git commits) + recent views (IDE telemetry) + manual assignments, all ranked with a composite formula: `-(100000×ownershipReasons + 1000×reasons + 10×contributions + views)`.
- **Search-based insights**: Any Sourcegraph search query becomes a trackable time-series metric. This makes metrics infinitely extensible without code changes.
- **Cross-repo analysis**: Works across thousands of repositories simultaneously.

**DevRank opportunity:** The multi-signal ownership ranking approach could inform how DevRank computes expertise scores.

### C7. Academic Research — Untapped Differentiation

**What academia has that nobody productionized:**
- **Developer social network centrality** (degree, betweenness, closeness, eigenvector) — proven correlation with productivity but no SaaS tool computes these in production.
- **7 predictive team metrics** including Cognitive Load Distribution Index, Psychological Safety Index, Learning Velocity Coefficient, and Technical Debt Emotional Load (commit message sentiment). These predict performance 4-6 months before traditional indicators.
- **DX Core 4** framework unifying DORA + SPACE + DevEx into 4 dimensions (Speed, Effectiveness, Quality, Business Impact) with the Developer Experience Index survey instrument.

**DevRank opportunity:** Being the first tool to productionize graph-based developer centrality metrics would be a genuine market differentiator. The tooling exists (NetworkX) and the data is already in DevRank's GitHub fetcher.

---

## 6. Detailed Competitor Breakdown

### 6.1 CodeScene / code-maat

**Open-source:** code-maat (Clojure, 18 CLI analyses), maat-scripts (Python, 8 companion analyses)

**Analyses DevRank lacks (26 total in code-maat + maat-scripts):**
1. `revisions` — change frequency per file (hotspot detection)
2. `authors` — distinct author count per file
3. `coupling` — temporal coupling between file pairs: `shared_revisions / avg_revisions × 100`
4. `soc` — sum of coupling per entity
5. `age` — months since last modification
6. `abs-churn` — add/del per date (trend)
7. `author-churn` — add/del per author
8. `entity-churn` — add/del per file
9. `entity-ownership` — per-author contribution % per file
10. `entity-effort` — revision distribution per author per file
11. `main-dev` — primary developer by lines added + ownership ratio
12. `main-dev-by-revs` — primary developer by revision count
13. `refactoring-main-dev` — developer who removed most lines
14. `communication` — Conway's Law: author pair co-work frequency
15. `fragmentation` — Herfindahl index: `1 - Σ(author_revs/total)²`
16. `messages` — commit message word frequency per file
17. Whitespace complexity analysis (language-neutral)
18. Complexity trend over git history
19. Complexity delta (added vs removed complexity per diff)
20. Change proximity (distance between changed lines)

**Commercial CodeScene adds:**
21. Code Health Score (25-30 factors, 1-10 scale)
22. X-Ray function-level analysis (method hotspots, internal coupling, clone detection)
23. Knowledge islands / knowledge loss / bus factor / off-boarding simulation
24. Social network analysis (developer collaboration graph)
25. Team-code alignment (team coupling + team cohesion)
26. Delivery risk prediction (ML-based, 1-10 per commit)
27. Delta analysis / PR integration with quality gates
28. Branch analysis (duration, lead time, contributing authors)
29. Goal setting with supervised refactoring
30. Complexity biomarkers (A-E grades with trend)

### 6.2 LinearB

**Open-source:** gitStream (YAML PR automation engine, not metrics)

**Metrics DevRank lacks:**
1. Pickup time (PR open → first non-author review)
2. Deploy time (merge → production)
3. Work breakdown: New/Refactor/Rework percentage (21-day threshold)
4. New Code Ratio (`new_lines / total_changed`)
5. PR Maturity Ratio (branch state at creation vs closure)
6. Active/stale/deleted/merged/deployed branch tracking
7. All 4 DORA metrics with benchmark tiers
8. Investment Strategy (5 categories from PM tools)
9. Monte Carlo project forecasting
10. AI adoption rate (24+ tools tracked)
11. AI code quality comparison (AI vs human rework rates)

**Unique features:**
- WorkerB bot (Slack notifications for PR risks)
- MCP Server for natural language metric queries
- 8.1M+ PR benchmark dataset

### 6.3 Swarmia

**Open-source:** None (fully closed)

**Metrics/features DevRank lacks:**
1. Working agreements (10+ types with daily digests)
2. Investment balance (activity-based + FTE-based allocation)
3. Developer Effort FTE calculation (weighted activities, HR time-off integration)
4. Scope creep measurement (child issues added / original)
5. Flow efficiency (% active days / total issue lifetime)
6. Sprint metrics (scope, carryover, completion)
7. AI adoption/usage metrics
8. CI run time and failure rate
9. DevEx surveys (32 SPACE-aligned questions)
10. Signals (AI-powered insight detection)
11. Normalized throughput (per FTE)
12. Work log with anti-pattern detection (solo work, task stickiness, context switching)

### 6.4 Code Climate Velocity / Qlty

**Open-source:** Qlty CLI (Rust, 2,967 stars, Apache 2.0). Source code cloned and analyzed at function level.

**Key implementation details (from source):**
- **Cognitive complexity**: Full SonarSource spec — nesting-level increments for control flow, boolean operator change tracking, recursive call detection. 13 languages via tree-sitter.
- **LCOM4**: Connected component analysis within classes — tracks `self.method()` calls and `self.field` accesses, merges methods sharing fields/calls via transitive closure. Excludes constructors/destructors/static methods.
- **AST duplication**: Structural hashing (MD5 of node kind + children hashes) with mass threshold, identical vs similar classification, subtree pruning to avoid double-reporting.
- **Technical debt effort**: `effort_minutes = base_minutes + (value_delta × minutes_per_delta)` with per-check constants (e.g., function-complexity: 10 base + 5/delta).
- **6 structural checks**: boolean-logic, file-complexity, function-complexity, nested-control-flow, function-parameters, return-statements.
- **9 coverage parsers**: Clover, Cobertura, Go coverprofile, dotCover, JaCoCo, LCOV, SimpleCov, Xccov, Qlty native.
- **45 Engineering Data Platform record types** defining the Velocity product data model (Repository, PullRequest, Deployment, Incident, Sprint, etc.).

**Metrics DevRank lacks:**
1. Review Coverage (% hunks receiving comments)
2. Review Influence (% comments leading to code changes)
3. Discussion Cycles (alternating-person exchanges)
4. Impact formula (weighted cognitive load estimate)
5. Productive Impact (`Impact × (1 - Rework Rate)`)
6. Innovation Rate (% new feature PRs)
7. Pull Request Success Rate (merged vs closed without merge)
8. PR Risks detection (High Rework, Too Many Cooks, Large Change, Inactive)
9. Issue Cycle Time, Lead Time, Rework Rate (from PM tools)
10. Traceability (% code changes linked to tickets)
11. File Hotspots (2×2: change freq × understanding)
12. Knowledge Silos visualization
13. Review Network visualization
14. All 4 DORA metrics
15. Health Check scorecard (current vs previous 3 iterations)
16. Industry benchmarks (15 metrics with quartile data)

### 6.5 Pluralsight Flow

**Open-source:** None

**Metrics DevRank lacks:**
1. Diff Delta / Impact formula (6 multiplicative factors)
2. Efficiency metric (% productive non-rework code)
3. tt100 (time to write 100 productive lines)
4. Help Others work type (modifying someone else's recent code)
5. Knowledge Sharing Index (0-1 review distribution scale)
6. Review Radar visualization
7. Reviewer Involvement (% org PRs reviewed)
8. Reviewer Influence (follow-on commit ratio)
9. Iterated PRs (% PRs with follow-on commits after initial review)
10. Queue Time (waiting state duration)
11. Ticket Jitter (unclear requirements indicator)
12. Commit Risk (composite: size, files, lines)
13. Proficiency Report (efficiency by programming language)
14. 20 Behavioral Patterns detection

### 6.6 Sourcegraph

**Open-source:** Full platform (Go, 2M+ LOC)

**Features DevRank lacks:**
1. Multi-signal ownership ranking (CODEOWNERS + contributors + views + manual)
2. Search-based metrics (any search query → time series)
3. Code Insights (trend tracking with backfill pipeline)
4. Cross-repository analysis at scale
5. CODEOWNERS integration and parsing

### 6.7 Open-Source Tools (Cloned & Analyzed)

| Tool | Language | Stars | Key Capabilities DevRank Lacks |
|------|----------|-------|-------------------------------|
| **PyDriller** | Python | ~5K | Process metrics framework: ChangeSet, CodeChurn, ContributorsExperience, HistoryComplexity (entropy), HunksCount (fragmentation), DMM (Delta Maintainability Model via lizard) |
| **Apache DevLake** | Go | ~2.5K | Most comprehensive DORA implementation: 5-phase lead time breakdown (PrCodingTime, PrPickupTime, PrReviewTime, PrDeployTime, PrCycleTime), deployment generation from CI/CD, incident-deploy connection |
| **Four Keys** | SQL/Python | ~2K | Google's DORA reference implementation with BigQuery SQL, 3-month bucketing, industry benchmark thresholds |
| **git-of-theseus** | Python | ~2K | Code survival/cohort analysis: lines surviving over time grouped by year/quarter added (code half-life), author attribution over time, domain/org attribution |
| **Middleware** | Python | ~1K | Open-source DORA platform with weekly trends, lead time breakdown (5 sub-metrics), incident-deployment mapping |
| **OpenDORA** | Go | ~500 | Backstage plugin for DORA with weekly/monthly/quarterly aggregations and benchmark comparison |

### 6.8 Emerging Platforms (Deep Research from Docs)

#### Jellyfish — Engineering Management Platform

**21 structured developer productivity metrics across 3 categories:**

| Category | Metrics |
|----------|---------|
| **Efficiency** (7) | Lead Time, Cycle Time, PR Review Time, Deployment Frequency, Time to First Review, Time to Resolve Blocked Work, Build Success Rate |
| **Effectiveness** (7) | Bug Escape Rate, MTTR, Test Coverage, Feature Adoption Rate, Technical Debt Ratio, Rollback Frequency, Reopen Rate |
| **Experience** (7) | Developer Satisfaction Surveys, Time Spent in Meetings, Tool Satisfaction, Context Switching Frequency, Onboarding Time, On-Call Burden, Work-Life Balance Signals |

**Unique capabilities:** Patented multi-source allocations model (AI-powered work categorization), capacity planner with scenario modeling, executive-ready investment dashboards, AI coding tool adoption tracking (Copilot/Cursor/Sourcegraph comparison).

#### DX — Developer Intelligence Platform

**DX Core 4 framework (unified DORA + SPACE + DevEx):**
- **Speed**: development velocity and delivery pace
- **Effectiveness**: Developer Experience Index (DXI) — 14-question survey, 4M+ data points from 800+ orgs
- **Quality**: software reliability and stability
- **Business Impact**: engineering work → organizational value

**Financial proof:** 1-point DXI increase = 13 minutes/week/developer saved. Top-quartile DXI teams show 4-5× greater speed and quality, 43% higher engagement.

**Unique capabilities:** Direct Benchmarking™ (compare vs named peer companies), experience sampling during active work, AI readiness assessment, agent ops tools, internal developer portal.

#### Sleuth — Deployment-Centric DORA

**Lead time decomposed into 4 phases** with working-hours-aware calculation:
- Coding (first commit → PR open), Review Lag (PR open → first review), Review Time (first review → merge), Deploying (merge → deployment)
- Working hours adjustment: clock runs while any author/reviewer is in working hours

**Unique capabilities:** Feature flags as first-class changes (LaunchDarkly integration), 4 failure classification types (Incident/Rolled Back/Unhealthy/Ailing), WIP risk detection (flags items >30% above average), goals with smart escalation (75% = soft nudge, 90% = @mention).

#### Haystack — Delivery Ops with Burnout Prevention

**24 granular metrics:** 11 issue metrics + 8 PR metrics + 4 sprint metrics + 1 deployment metric.

**Notable metrics DevRank lacks:**
- **Commitment Reliability Rate**: Sprint completion excluding injected work
- **PRs Unlinked**: PRs not linked to issue tracker (enforces traceability)
- **Idle Completion Time**: Time from rework complete to merge (post-review idle)
- **Sprint Injection**: Issues added after sprint starts

**85th percentile** recommended over median for time-based metrics (captures tail latency).

#### Other Platforms

| Platform | Unique Capability DevRank Lacks |
|----------|-------------------------------|
| **Faros AI** | 70+ data connectors, canonical data model, natural language chat analytics (Clara), on-premises deployment |
| **Waydev** | Core 4 unified model, custom metrics builder with user-defined formulas |
| **CodeSee** | Code maps, service maps, PR impact visualization, ownership maps (acquired by GitKraken May 2024) |

---

## 7. Priority Roadmap

### Tier 1: High-Impact, Feasible Now (Data Already Available)

These can be built with data DevRank already fetches from GitHub.

| # | Metric/Feature | Effort | Impact | Inspiration |
|---|---------------|--------|--------|-------------|
| 1 | **Bus Factor** per file/module | Medium | Very High | Truck-Factor algorithm, code-maat `fragmentation` |
| 2 | **Knowledge Islands** (95%+ single-author files) | Low | Very High | CodeScene |
| 3 | **Temporal Coupling** between files | Medium | Very High | code-maat `coupling` algorithm |
| 4 | **Hotspot Detection** (revision frequency × complexity) | Medium | Very High | code-maat `revisions` + maat-scripts `merge_comp_freqs.py` |
| 5 | **Developer Centrality Metrics** (degree, betweenness) | Medium | Very High | NetworkX, academic DSN research |
| 6 | **Work Type Breakdown** (New/Refactor/Rework/Help Others) | Medium | High | Pluralsight Flow, LinearB (21-30 day line age) |
| 7 | **Unreviewed PR Rate** *(PARTIAL — `self_merge_rate` is close)* | Low | Medium | Code Climate, Pluralsight Flow |
| 8 | **Pickup Time (authored)** *(PARTIAL — `review_turnaround_time` covers reviewer side)* | Low | Medium | LinearB, Swarmia |
| 9 | **Knowledge Sharing Index** (review distribution evenness) | Low | High | Pluralsight Flow |
| 10 | **Code Age** per file/module | Low | Medium | code-maat `age` |

### Tier 2: High-Impact, Moderate Effort

| # | Metric/Feature | Effort | Impact | Inspiration |
|---|---------------|--------|--------|-------------|
| 11 | **Entity Fragmentation** (Herfindahl index) | Low | High | code-maat |
| 12 | **Communication/Conway's Law Analysis** | Medium | High | code-maat `communication` |
| 13 | **Main Developer Identification** per file | Low | Medium | code-maat `main-dev` |
| 14 | **Knowledge Loss** (departed contributor code) | Medium | High | CodeScene |
| 15 | **Complexity Trend Tracking** | Medium | High | maat-scripts, CodeScene |
| 16 | **Code Health Score** (multi-factor, building on tree-sitter) | High | Very High | CodeScene |
| 17 | **Benchmark Data** from open-source repos | Medium | High | LinearB, Code Climate |
| 18 | **Architectural Grouping** (file path → logical layer mapping) | Medium | High | code-maat `--group` |
| 19 | **Team Mapping & Aggregation** | Medium | High | code-maat team mapper, all platforms |
| 20 | **Review Coverage** (% hunks commented) | Medium | Medium | Code Climate |

### Tier 3: Strategic Investments (New Data Sources or Major Features)

| # | Metric/Feature | Effort | Impact | Inspiration |
|---|---------------|--------|--------|-------------|
| 21 | **DORA Metrics** (requires CI/CD + incident integration) | Very High | Very High | All platforms |
| 22 | **Behavioral Pattern Detection** (map metric combos to named patterns) | High | High | Pluralsight Flow 20 Patterns |
| 23 | **Delivery Risk Scoring** (ML-based per-commit) | High | High | CodeScene |
| 24 | **Working Agreements** (team targets with tracking) | High | Medium | Swarmia |
| 25 | **Developer Experience Surveys** | Very High | High | Swarmia, DX |
| 26 | **Investment Balance** (requires PM tool integration) | Very High | Medium | Swarmia, LinearB |
| 27 | **AI-Assisted Development Metrics** | Medium | Medium | Swarmia, LinearB |
| 28 | **Estimated Review Time** (ML model) | High | Medium | GitStream |
| 29 | **Off-boarding Simulator** | Medium | Medium | CodeScene |
| 30 | **Notification/Alerting System** (Slack/Teams) | High | Medium | Swarmia, LinearB |

### Implementation Notes

**Quick wins (< 1 week each):**
- Unreviewed PR Rate — simple count from existing PR data
- Pickup Time — delta between PR open and first non-author review event (data exists in timeline)
- Knowledge Islands — aggregate author contribution % per file from commit data
- Code Age — months since last modification from commit timestamps
- Entity Fragmentation — Herfindahl formula on existing per-file contribution data

**Medium effort (1-3 weeks each):**
- Bus Factor — Truck-Factor algorithm (DOA-based, iterate removal)
- Temporal Coupling — co-change analysis across PRs in same time window
- Hotspot Detection — requires adding revision frequency tracking + complexity measurement
- Work Type Breakdown — classify lines by age (>21 days = legacy) and authorship
- Developer Centrality — build collaboration graph from review interactions, compute with NetworkX

**Major investments (1-3 months each):**
- Code Health Score — extend tree-sitter analysis to quantify all 25-30 CodeScene factors
- DORA Metrics — requires new data source integrations (CI/CD, incident management)
- Behavioral Pattern Detection — rule engine mapping metric combinations to named patterns
- Benchmark System — collect and anonymize data across deployments

---

## Appendix: Metric Count Comparison

| Platform | Total Metrics | Individual | Team | DORA | Deployment | Code Quality |
|----------|--------------|-----------|------|------|-----------|-------------|
| **DevRank** | 46 | 46 | 0 | 0 | 0 | 6 |
| **LinearB** | 35+ | 10+ | 15+ | 4 | 4 | 7 |
| **Code Climate Velocity** | 35+ | 10+ | 10+ | 4 | 4 | 10+ |
| **Pluralsight Flow** | 30+ | 15+ | 10+ | 4 | 4 | 3 |
| **Swarmia** | 26+ | 5 | 15+ | 4 | 3 | 0 |
| **CodeScene** | 30+ (analyses) | 5 | 15+ | 0 | 0 | 25-30 |
| **Sourcegraph** | Unlimited (search-based) | 0 | 0 | 0 | 0 | 0 |

**DevRank's unique position:** Deepest individual-contributor metrics (46) but shallowest team/deployment/organizational coverage (0). The roadmap above addresses this asymmetry while preserving DevRank's individual-depth advantage.

---

---

## 8. Ranked TODO List

Ordered by combined score of **impact** (how much value it adds), **differentiation** (how unique it makes DevRank vs competitors), and **wow factor** (how impressive it is to users/buyers). Items marked with feasibility indicators.

### Legend
- **Impact**: H (High) / M (Medium) / L (Low)
- **Differentiation**: unique = no competitor has this in production; strong = few competitors; standard = table stakes
- **Wow**: the "demo moment" factor
- **Feasibility**: data-ready = can build with current GitHub data; needs-infra = requires new data sources or major architecture

---

### Tier S — Game Changers (do these first)

| # | TODO | Impact | Diff | Wow | Feasibility | Notes |
|---|------|--------|------|-----|-------------|-------|
| 1 | **Developer Social Network Graph with Centrality Metrics** — Build a collaboration graph from review interactions (who reviews whom, who co-authors with whom). Compute degree, betweenness, closeness, and eigenvector centrality per developer. Identify bridge developers, isolated contributors, and influence hubs. | H | **unique** | very high | data-ready (NetworkX) | No SaaS tool has shipped this in production. Academic research proves correlation with productivity. This alone would make DevRank the only tool that answers "who is the most influential engineer and why?" with mathematical proof. First-mover advantage. |
| 2 | **Bus Factor & Knowledge Islands** — For each file/module, compute the minimum number of developers whose departure would leave it unmaintainable. Flag files where 95%+ of code was written by a single contributor. Surface knowledge concentration risk across the entire codebase. | H | strong | very high | data-ready | 65% of GitHub projects have bus factor ≤ 2. This is the metric CTOs lose sleep over but can't currently measure. CodeScene charges enterprise pricing for this. |
| 3 | **Codebase Hotspot Detection** — Combine revision frequency per file with complexity measurement to identify where tech debt hurts most. Rank files by "pain" = change frequency × complexity. | H | strong | high | data-ready (needs complexity addition) | CodeScene's flagship feature. Research: hotspots have 15× more defects, 2× slower development. Visualizable as a treemap — instant executive impact. |
| 4 | **Temporal/Logical Coupling** — Detect files that always change together (hidden architectural dependencies). `coupling_degree = shared_revisions / avg_revisions × 100`. Flag surprising couplings that indicate design problems. | H | strong | high | data-ready | Reveals architecture decay that no amount of code review catches. code-maat algorithm is well-documented and proven over 10+ years. |

### Tier A — High-Value Additions

| # | TODO | Impact | Diff | Wow | Feasibility | Notes |
|---|------|--------|------|-----|-------------|-------|
| 5 | **Conway's Law Communication Analysis** — Map implicit communication needs between developers based on shared code ownership. Compute communication strength: `shared_commits / ceil(avg(my_commits, peer_commits)) × 100`. Visualize as a developer collaboration network. | H | **unique** | high | data-ready | Nobody productionizes this. Combined with centrality (#1), creates a full "organizational X-ray" that no competitor offers. |
| 6 | **Knowledge Loss Detection** — Identify code where 50%+ was written by departed/inactive contributors. Combine with hotspot data to prioritize: "this critical file was mostly written by someone who left 6 months ago." | H | strong | high | data-ready | Off-boarding risk quantified. Pairs naturally with Bus Factor (#2) for a complete "people risk" dashboard. |
| 7 | **Code Health Score** (multi-factor, 1-10) — Extend existing tree-sitter infrastructure to compute cognitive complexity, cyclomatic complexity, nesting depth, function length, parameter count, LCOM4 per file. Aggregate into a single 1-10 health score. | H | strong | high | data-ready (tree-sitter exists) | DevRank already has tree-sitter for 5 languages. Qlty's open-source Rust code proves the exact algorithms work on tree-sitter. This is an extension, not a rebuild. |
| 8 | **Work Type Breakdown** (New/Refactor/Rework/Help Others) — Classify every code change by line age (>21-30 days = legacy) and authorship (same author = rework, different = help others). Show % distribution per developer and per team. | H | standard | medium | data-ready | Table stakes metric that every competitor has. DevRank's `rework_rate` is partial. Completing the full breakdown fills a visible gap in any comparison. |
| 9 | **Behavioral Pattern Detection** — Map combinations of existing 46 metrics to named behavioral patterns: "Domain Champion" (deep expert, knowledge island risk), "Heroing" (last-minute fixes bypassing review), "Rubber Stamping" (fast approvals, no substance), "Bit Twiddling" (high churn, low impact). Generate narrative insights. | H | **unique** | very high | data-ready (metrics exist) | Pluralsight Flow's 20 Patterns is one of their most-cited features. DevRank already has the underlying metrics — this is a presentation/intelligence layer on top. Turns numbers into stories. |
| 10 | **Knowledge Sharing Index** — Measure how evenly code reviews are distributed across the team. Scale 0-1 (0 = siloed, 1 = well-distributed). Uses review interaction data already in the ledger. | M | strong | medium | data-ready | Simple to compute, high signal. Extends `review_breadth` (count of unique authors) into a normalized distribution metric. |
| 11 | **Entity Fragmentation** (Herfindahl Index) — Per-file authorship concentration: `1 - Σ(author_revs/total)²`. Range 0 (single author) to ~1 (many authors). Superior to raw author count because it discounts minor contributions. | M | strong | medium | data-ready | Feeds into Bus Factor and Knowledge Islands. Quick win — pure math on existing data. |
| 12 | **Complexity Trend Tracking** — Track whitespace-based or tree-sitter-based complexity per file over time. Show whether complexity is growing, stable, or being refactored down. Alert on files with rising complexity trends. | M | strong | medium | needs time-series storage | Requires storing historical data per assessment. CodeScene's complexity trend is one of their most actionable features. |

### Tier B — Strong Additions

| # | TODO | Impact | Diff | Wow | Feasibility | Notes |
|---|------|--------|------|-----|-------------|-------|
| 13 | **Code Survival / Cohort Analysis** — Track what % of a developer's contributed lines are still alive over time. Shows code durability — whose code survives vs gets rewritten. Compute per-developer "code half-life." | M | **unique** | high | data-ready (git blame) | git-of-theseus algorithm. Nobody offers this as a per-developer metric. Answers "whose code actually sticks?" — powerful for hiring/performance conversations. |
| 14 | **Team Mapping & Aggregation** — Add author-to-team mapping (CSV or config). Aggregate all 46 individual metrics to team-level views. Enable team-vs-team comparison. | H | standard | medium | needs-infra (DB team models exist) | Every competitor has this. Currently DevRank's biggest structural gap for enterprise sales. DB already has team/membership models — need to wire them into the metrics pipeline. |
| 15 | **Industry Benchmarks** — Compute benchmark percentiles from anonymized customer data or curated open-source repos. Show each metric relative to industry (e.g., "your cycle time is in the 75th percentile"). | H | standard | medium | needs data collection | LinearB has 8.1M+ PRs for benchmarking. Start with open-source repo baselines (kubernetes, react, django, etc.) as a proxy. |
| 16 | **Architectural Grouping** — Support a group mapping file (like code-maat's `--group`) that maps file paths to logical components via regex. Elevate all file-level metrics to architecture level. | M | strong | medium | data-ready | Extends existing `detect_module_boundary()`. Enables "your API layer has 3× the churn of your data layer" type insights. |
| 17 | **Main Developer Identification** — Per-file: who added the most lines (main-dev) and who has the most revisions (main-dev-by-revs). Compute ownership ratio. Also identify "refactoring main dev" (most lines removed). | M | standard | low | data-ready | Foundation for Knowledge Islands and Bus Factor. code-maat has three flavors of this metric. |
| 18 | **Review Coverage** — % of PR files/hunks receiving at least one review comment. Identifies PRs where reviewers only looked at part of the changeset. | M | standard | medium | data-ready | Code Climate's version. DevRank has `inline_comment_density` (avg comments per PR) but not per-hunk coverage. |
| 19 | **Code Age** per file/module — Months since last modification. Flag code that's neither very old (stable) nor very fresh (actively maintained) — the dangerous middle ground. | M | standard | low | data-ready | Quick win. Inspired by Dan North's "software half-life" concept. Useful input for hotspot analysis. |
| 20 | **Discussion Cycles** — Count alternating-person comment exchanges (A→B→A = 2 cycles). Different from `review_iterations` (change-request rounds). High discussion cycles = contentious or under-specified PRs. | M | standard | medium | data-ready | Code Climate Velocity metric. Data is already in PR comments/timeline. |
| 21 | **Productive Impact** — `net_code_contribution × (1 - rework_rate)`. Separates gross output from net productive output. | M | standard | medium | data-ready | Simple composite of two existing metrics. Code Climate's version. Immediate value. |
| 22 | **Idle Completion Time** — Time from last commit/rework to merge. Captures post-review waiting/idle — the "it's approved but nobody merged it" problem. | M | standard | medium | data-ready | Haystack metric. DevRank has `merge_delay` (approval to merge) which is close but not identical. |

### Tier C — Nice to Have

| # | TODO | Impact | Diff | Wow | Feasibility | Notes |
|---|------|--------|------|-----|-------------|-------|
| 23 | **DORA Metrics** — Deployment Frequency, Lead Time for Changes, Change Failure Rate, MTTR. | H | standard | medium | needs-infra (CI/CD + incident integrations) | Table stakes for enterprise. Every competitor has this. Requires GitHub Actions/releases integration at minimum, plus incident management (PagerDuty/etc.) for MTTR/CFR. High effort but expected by buyers. |
| 24 | **AI-Assisted Development Metrics** — Detect AI-authored PRs (Co-authored-by trailers for Copilot/Cursor/Claude Code). Track AI adoption rate, AI vs human rework rates. | M | standard | medium | data-ready (co-author trailers) | Hot topic in 2025-2026. Simple to detect from commit metadata. Swarmia and LinearB both ship this. |
| 25 | **Off-boarding Simulator** — Select a developer to simulate departure. Show which files/modules become at-risk, which knowledge is lost, which bus factors drop to 0. | M | strong | very high | data-ready (builds on #2, #6) | CodeScene's most compelling demo feature. Requires Bus Factor + Knowledge Loss as prerequisites. Pure presentation layer on top of existing data. |
| 26 | **Working Agreements** — Let teams define target values for any metric (e.g., "cycle time < 48 hours", "review turnaround < 4 hours"). Track compliance over time. | M | standard | medium | needs-infra (UI + persistence) | Swarmia's core differentiator. Requires frontend and notification infrastructure. More of a product feature than a metric. |
| 27 | **Change Proximity** — Sum of distances between changed lines within a file. Scattered changes = higher risk. Concentrated changes = safer. | L | strong | low | data-ready (hunk data exists) | maat-scripts algorithm. Niche but interesting risk signal. |
| 28 | **Cognitive Load Distribution Index** — How evenly complex work distributes across team. >80% by 2-3 people predicts burnout. | M | **unique** | medium | data-ready (needs team mapping) | From predictive research. Requires team mapping (#14) as prerequisite. |
| 29 | **Estimated Review Time** — Heuristic or ML-based estimate of minutes to review a PR, based on size, file types, complexity. | M | standard | medium | data-ready (heuristic), needs ML for full | GitStream uses ML (closed). A heuristic version (size + file type + complexity) is achievable. |
| 30 | **PR Maturity Ratio** — How much a PR changes between open and merge. High maturity = PR evolved significantly during review. Low = shipped as-is. | L | standard | low | data-ready | LinearB metric. Compares branch state at creation vs closure. |
| 31 | **Innovation Rate** — % of merged PRs representing new feature work (detected via title/label/commit type). | L | standard | low | data-ready | `pr_category_diversity` already classifies "feat" PRs. This just surfaces the % as a standalone metric. |
| 32 | **Context Switch Frequency** — Count of distinct repo/project/area switches per developer per day. >5 = productivity risk. | M | strong | medium | data-ready (commit timestamps + repos) | Distinct from `burstiness`. Requires intra-day commit timestamp analysis across repos. |
| 33 | **Sum of Coupling (SoC)** — Per-entity total coupling score across all changesets. Prioritizes the most entangled files in the codebase. | L | standard | low | data-ready | Companion to Temporal Coupling (#4). Simple aggregation. |
| 34 | **Developer Experience Surveys** — Built-in survey instrument (SPACE-aligned questions) with correlation to system metrics. | M | standard | medium | needs-infra (survey UI, storage, analysis) | DX and Swarmia both have this. Major product investment. Each 1-point DXI improvement = 13 min/dev/week saved. |
| 35 | **Investment Balance** — Categorize engineering time: roadmap vs bugs vs tech debt vs unplanned. | M | standard | medium | needs-infra (PM tool integration) | Requires Jira/Linear integration. Swarmia and LinearB both have this. |
| 36 | **Notification/Alerting System** — Slack/Teams integration for review requests, stale PRs, working agreement violations, daily digests. | M | standard | medium | needs-infra (webhook infrastructure) | Every competitor has this. More ops/product work than metrics work. |
| 37 | **Delivery Risk Scoring** — Per-commit risk score (1-10) based on code changed, files touched, diffusion across subsystems, developer experience with those files. | M | strong | high | data-ready (builds on #3, #17) | CodeScene's ML-based version is impressive. A heuristic version using hotspot data + developer familiarity is achievable. |
| 38 | **Code Survival by Author** — What % of each developer's historical code is still alive in the current codebase. Ranks developers by code durability. | L | **unique** | medium | needs git blame at scale | Extension of #13. Answers "who writes code that lasts?" More of a hiring/evaluation signal. |
| 39 | **tt100 (Time to 100)** — Time for an engineer to write 100 lines of productive (non-rework) code. Normalized for active coding time only. | L | standard | low | data-ready | Pluralsight Flow metric. Niche productivity signal. |
| 40 | **Commit Message Mining** — Regex search of commit messages per file to estimate defect distribution. Map "bug", "fix", "error" frequency to specific files. | L | standard | low | data-ready | code-maat `messages`. `bug_fix_focus_rate` already does PR-level keyword detection. This extends to per-file granularity. |

---

**Total: 40 prioritized TODOs**

- **Tier S** (4 items): Game changers that would make DevRank genuinely unique in the market. All are data-ready.
- **Tier A** (8 items): High-value additions that fill critical gaps or create strong differentiators.
- **Tier B** (10 items): Strong additions that round out the platform and close competitive gaps.
- **Tier C** (18 items): Nice-to-have features ranging from table stakes (DORA, team mapping) to niche signals.

**Key insight**: The top 5 items (Developer Social Network, Bus Factor, Hotspots, Temporal Coupling, Conway's Law) are all data-ready, buildable with current GitHub data, and would collectively make DevRank the **only tool in the market that provides organizational network intelligence combined with individual contributor metrics**. No competitor — not CodeScene, not LinearB, not Swarmia — offers this combination.

---

*Generated by deep analysis of 14 platforms, 10 open-source codebases (code-maat, maat-scripts, Sourcegraph, GitStream, Code Climate/Qlty, PyDriller, Apache DevLake, Four Keys, git-of-theseus, Middleware), and 20+ academic papers. All OSS repos cloned locally and parsed at source-code level. March 2026.*
