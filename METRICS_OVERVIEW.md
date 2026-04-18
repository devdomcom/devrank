# DevRank Metrics Overview v5

**78 implemented | 73 planned | 17 PM tool | 11 methodology-specific | 3 CI/CD | 9 AI advanced | 11 competitive gap | 6 new AI era | 208 total**

_Updated: April 2026 — incorporates competitive analysis (GitKraken Insights, March 2026), 2026 AI metrics research, and PM tool integration design._

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
| **DORA** | DevOps Research & Assessment — the 4 core metrics for software delivery performance | Deployment Frequency, Lead Time for Changes, Change Failure Rate, MTTR |
| **SPACE** | Developer productivity framework covering Satisfaction, Performance, Activity, Communication, Efficiency | Coding Days, Review Turnaround Time, Flow Efficiency, On-Call Burden |
| **CodeScene** | Adam Tornhill's research on code evolution, knowledge ownership, and hotspots | Hotspot Detection, Temporal Coupling, Bus Factor, Code Age, Knowledge Islands |
| **Lean** | Lean manufacturing principles applied to software — flow, waste reduction, cycle times | Cycle Time, Pickup Time, Deploy Time, Rework Rate, Flow Efficiency, tt100 |
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
| ★5 | Developer Experience Index (DXI) | Survey on perceived productivity, interruptions, AI usefulness | 🆕 New | 🌐 Universal | SPACE | P1 |
| ★6 | Feature Throughput to Customer | Real features → business goals achieved | 🆕 New | 🌐 Universal | DevRank | P1 |

---

## Implemented — Authored (60)

Engineer-owned PRs and activity. All metrics work with the user-centric fetch pipeline (`impact/providers/github_live.py`) unless flagged in [Data Scope Caveats](#data-scope-metrics-requiring-repo-wide-data).

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 1 | AI-Assisted PR Rate | % PRs created with AI tools (Copilot/Cursor/Claude) | 🔄 Evolved | 🌐 Universal | DevRank |
| 2 | AI Adoption Rate | Per-engineer AI tool usage (inferred from commit/PR signatures) | 🔄 Evolved | 🌐 Universal | DevRank |
| 3 | AI Code Quality | Rework rate on AI-assisted vs human PRs | ✅ Unchanged | 🌐 Universal | DevRank |
| 4 | AI Phantom Ownership | Code primarily touched by AI with low human review depth | ⚠️ Caution | 🌐 Universal | DevRank |
| 5 | AI Suggestion Acceptance | Ratio of accepted vs dismissed AI suggestions from review bots | 🔄 Evolved | 🌐 Universal | DevRank |
| 6 | PR Throughput | PRs merged in the period | ⚠️ Caution | 🌐 Universal | SPACE • Lean |
| 7 | Delivery Volume | Total lines added+deleted across merged PRs | ⚠️ Caution | 🌐 Universal | SPACE |
| 8 | Net Code Contribution | Lines added minus lines deleted | ⚠️ Caution | 🌐 Universal | SPACE |
| 9 | Cycle Time | Time from PR creation to merge | 🔄 Evolved | 🌐 Universal | DORA • SPACE • Lean |
| 10 | Coding Time To PR | Time from first commit to PR creation | ✅ Unchanged | 🌐 Universal | Lean |
| 11 | Merge Delay | Time from first approval to merge | ✅ Unchanged | 🌐 Universal | Lean |
| 12 | Pickup Time | PR opened to first non-author review activity | ✅ Unchanged | 🌐 Universal | Lean |
| 13 | PR Size Distribution | Statistical distribution of PR sizes | 🔄 Evolved | 🌐 Universal | DevRank |
| 14 | Trivial Contribution Rate | % of PRs that are trivial | 🔄 Evolved | 🌐 Universal | DevRank |
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
| 25 | PR Merge Effectiveness | Ratio of merged PRs to total PRs opened | ✅ Unchanged | 🌐 Universal | SPACE |
| 26 | PR Body Quality Score | Quality of PR descriptions | ✅ Unchanged | 🌐 Universal | DevRank |
| 27 | Conventional Commit Rate | % of commits following conventional commit format | ✅ Unchanged | 🌐 Universal | DevRank |
| 28 | Test File Ratio | Ratio of test file changes to production file changes | ⚠️ Caution | 🌐 Universal | Traditional |
| 29 | Documentation Touch Rate | % of PRs that include documentation changes | ✅ Unchanged | 🌐 Universal | DevRank |
| 30 | Dependency Change Rate | % of PRs modifying dependency/manifest files | ✅ Unchanged | 🌐 Universal | DevRank |
| 31 | Module / Area Breadth | Number of distinct modules/areas touched | ✅ Unchanged | 🌐 Universal | DevRank |
| 32 | PR Category Diversity | Diversity of PR types | ✅ Unchanged | 🌐 Universal | DevRank |
| 33 | Bug Fix Focus Rate | % of PRs addressing bug fixes | ✅ Unchanged | 🌐 Universal | DevRank |
| 34 | Coding Days | Number of days with commit activity | ✅ Unchanged | 🌐 Universal | SPACE |
| 35 | Active Weeks | Number of weeks with at least one contribution | ✅ Unchanged | 🌐 Universal | SPACE |
| 36 | Off-Hours Activity Rate | % of commits made outside business hours | ✅ Unchanged | 🌐 Universal | SPACE |
| 37 | Burstiness | Ratio of max weekly activity to average | ✅ Unchanged | 🌐 Universal | DevRank |
| 38 | Revert Introduction Rate | % of PRs that introduce reverts | 🔄 Evolved | 🌐 Universal | DevRank |
| 39 | Hotspot Detection | Files with highest revision frequency × complexity | 🔄 Evolved | 🌐 Universal | CodeScene |
| 40 | Bus Factor | Min developers who could leave before code is unmaintainable | 🔄 Evolved | 🌐 Universal | CodeScene |
| 41 | Knowledge Islands | Files where 95%+ written by one person | ⚠️ Caution | 🌐 Universal | CodeScene |
| 42 | Knowledge Loss | Code where 50%+ written by departed contributors | ✅ Unchanged | 🌐 Universal | CodeScene |
| 43 | Code Familiarity | % of codebase known by current active team | ⚠️ Caution | 🌐 Universal | CodeScene |
| 44 | Main Developer (by lines) | Primary author per file by lines added | 🔄 Evolved | 🌐 Universal | CodeScene |
| 45 | Main Developer (by revisions) | Primary author per file by commit count | ✅ Unchanged | 🌐 Universal | CodeScene |
| 46 | Entity Ownership | Per-author contribution percentages per file | 🔄 Evolved | 🌐 Universal | CodeScene |
| 47 | Contributor Experience | Relative share of codebase activity by the target developer | 🔄 Evolved | 🌐 Universal | CodeScene |
| 48 | Temporal / Logical Coupling | Files that always change together | ✅ Unchanged | 🌐 Universal | CodeScene |
| 49 | Entity Fragmentation | Herfindahl-like index of author scatter per file | ✅ Unchanged | 🌐 Universal | CodeScene |
| 50 | Complexity Trend | Whitespace-based complexity tracked per file over time | ✅ Unchanged | 🌐 Universal | Traditional |
| 51 | Change Proximity | Sum of distances between changed lines within a file | ✅ Unchanged | 🌐 Universal | CodeScene |
| 52 | Sum of Coupling | Per-entity total coupling score across all revisions | ✅ Unchanged | 🌐 Universal | CodeScene |
| 53 | Absolute Churn Trend | Lines added/deleted per date | ✅ Unchanged | 🌐 Universal | CodeScene |
| 54 | Commit Message Mining | Regex search of commit messages for defect indicators | ✅ Unchanged | 🌐 Universal | CodeScene |
| 55 | Code Survival | % of contributed lines still alive over time | 🔄 Evolved | 🌐 Universal | CodeScene |
| 56 | Flow Efficiency | Active coding time / total lead time for merged PRs | ✅ Unchanged | 🌐 Universal | Lean |
| 57 | WIP Load | Concurrent open PRs per day | ✅ Unchanged | 🌐 Universal | Lean |
| 58 | Review Coverage | % of PR files/hunks with at least one review comment | ⚠️ Caution | 🌐 Universal | DevRank |
| 59 | Delivery Risk Score (1-10) | Per-commit risk based on code, file count, diffusion, experience | 🔄 Evolved | 🌐 Universal | DevRank |
| 60 | Time to Restore (MTTR) | Time from revert to fix on the same files | 🔁 Renamed | 🌐 Universal | DORA |

## Implemented — Influence (17)

Impact on others' work — reviewing, mentorship, network position.

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 61 | Reviews Given | Total reviews submitted for others' PRs | 🔄 Evolved | 🌐 Universal | SPACE |
| 62 | Review Turnaround Time | Time from review request to submission | 🔄 Evolved | 🌐 Universal | SPACE • Lean |
| 63 | Unblock Time | Time to unblock others via reviews/approvals | ✅ Unchanged | 🌐 Universal | Lean |
| 64 | Inline Comment Density | Ratio of inline comments to total review comments | ✅ Unchanged | 🌐 Universal | DevRank |
| 65 | Review Comment Substance | Pygments-based scoring of code content in review comments | 🔄 Evolved | 🌐 Universal | DevRank |
| 66 | Review Leverage | Lines of code influenced per review given | ✅ Unchanged | 🌐 Universal | DevRank |
| 67 | Review Breadth | Number of distinct PR authors reviewed | ✅ Unchanged | 🌐 Universal | SPACE • Network |
| 68 | Review Demand | How sought-after as a reviewer (requests received) | ✅ Unchanged | 🌐 Universal | Network |
| 69 | PR Merge Rate | % of reviewed PRs that ultimately merged | ✅ Unchanged | 🌐 Universal | DevRank |
| 70 | Approval To Merge Ratio | Ratio of approvals given to actual merges | ⚠️ Caution | 🌐 Universal | DevRank |
| 71 | Change-Inducing Review Rate | % of reviews that led to code changes | ✅ Unchanged | 🌐 Universal | DevRank |
| 72 | Blocking Comment Rate | % of review comments that block merge | ✅ Unchanged | 🌐 Universal | DevRank |
| 73 | First Reviewer Rate | % of reviews where person was first reviewer | ✅ Unchanged | 🌐 Universal | DevRank |
| 74 | Mentorship Signal | Reviews targeting PRs from low-activity contributors | ✅ Unchanged | 🌐 Universal | SPACE • Network |
| 75 | Knowledge Sharing Index | How evenly reviews distribute across team (0-1 entropy-based) | ✅ Unchanged | 🌐 Universal | Network |
| 76 | Degree Centrality | Number of direct collaborators | ✅ Unchanged | 🌐 Universal | Network |
| 77 | Betweenness Centrality | Whether developer bridges disconnected teams | ✅ Unchanged | 🌐 Universal | Network |

## Implemented — Mixed (1)

Both authored and influence signals.

| # | Metric | Description | AI Status | Structure | Framework |
|---|--------|-------------|-----------|-----------|-----------|
| 78 | Co-Author Contribution Rate | % of commits with co-author trailers | 🔄 Evolved | 🌐 Universal | SPACE • Network |

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

## New PM Tool Metrics (17)

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
| PM9 | Estimation Accuracy | Original estimates vs actual time spent | 🆕 New | 🔀 Variant | P2 |
| PM10 | Unplanned Work Rate (ex Injected Work Rate) | % of unplanned work | 🆕 New | 🔀 Variant | P2 |
| PM11 | Requirements Change Rate | Frequency of requirements changes after work begins | 🆕 New | 🌐 Universal | P2 |

**Variant notes:**
- **Estimation Accuracy**: Scrum = story points estimated vs real. Kanban = real cycle time vs historical percentile (p50/p85). Waterfall = phase duration estimated vs real.
- **Unplanned Work Rate**: Scrum = work injected mid-sprint. Kanban = expedited tickets bypassing the flow. Waterfall = change requests after requirements sign-off.

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

## Methodology Variant Metrics (3)

These metrics exist for all teams but the calculation changes based on the methodology configured in `team_config.methodology`. The ETL applies the right formula. Superset receives the precomputed value.

| # | Metric | Scrum | Kanban | Waterfall |
|---|--------|-------|--------|-----------|
| V1 | **Planning Reliability** (ex CRR) | % of planned work completed in sprint | Throughput stability per week (coefficient of variation) | Adherence to phase milestones |
| V2 | **Unplanned Work Rate** (ex PM10) | % of work injected mid-sprint | % of expedited/urgent tickets that bypass the flow | % change requests after requirements sign-off |
| V3 | **Estimation Accuracy** (PM9) | Story points estimated vs real time | Real cycle time vs historical percentile (p50/p85) | Phase duration estimated vs real |

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

## Planned — A1. Codebase Evolution (4)

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A1-1 | Code Age | Months since last modification per file | ✅ Unchanged | 🌐 Universal | CodeScene | P1 |
| A1-2 | History Complexity (Entropy) | Normalized entropy of changes across files | ✅ Unchanged | 🌐 Universal | CodeScene | P2 |
| A1-3 | Hunks Count | Median diff hunks per file | ✅ Unchanged | 🌐 Universal | CodeScene | P3 |
| A1-4 | Delta Maintainability Model | Per-function cyclomatic complexity | ✅ Unchanged | 🌐 Universal | Traditional | P2 |

## Planned — A3. Graph/Network Collaboration (4)

Two centrality measures (degree, betweenness) are already implemented. Remaining four require advanced graph analysis.

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A3-1 | Closeness Centrality | How quickly a developer can reach entire org | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-2 | Eigenvector Centrality | Influence through association with influential devs | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-3 | Communication Strength | Conway's Law heuristic via shared commits | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-4 | Review Network Density | How interconnected the review graph is | ✅ Unchanged | 🌐 Universal | Network | P2 |

_Note: closeness, eigenvector, communication strength require custom D3.js/Cytoscape.js component (not natively supported by Superset)._

## Planned — A3bis. Graph/Network in Superset (3)

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A3-5 | Collaboration Asymmetry Index | Help-giving vs help-receiving ratio | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-6 | Team Coupling | Overlap in commits to same code by different teams | ✅ Unchanged | 🌐 Universal | Network | P2 |
| A3-7 | Team Cohesion | Whether team members work in same code areas | ✅ Unchanged | 🌐 Universal | Network | P2 |

## Planned — A4. DORA & Deployment (5)

> Deployment data (releases + deployments) is now collected by the fetcher. Time to Restore is implemented (see #60). The metrics below remain blocked on production-grade deployment status tracking.

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A4-1 | Deployment Frequency | How often code deploys to production | 🔄 Evolved | 🌐 Universal | DORA | P1 |
| A4-2 | Lead Time for Changes | Commit to production duration | ✅ Unchanged | 🌐 Universal | DORA | P1 |
| A4-3 | Change Failure Rate | % deployments causing failures | 🔄 Evolved | 🌐 Universal | DORA | P0 |
| A4-4 | Time to Deploy | PR merge to production deployment | ✅ Unchanged | 🌐 Universal | DORA | P2 |
| A4-5 | Deploy Time | Merge to production release | ✅ Unchanged | 🌐 Universal | DORA | P2 |

## Planned — A5. Cycle Time Sub-Phases (1)

> Pickup Time is implemented (see #12).

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A5-1 | Time to Approve | First review activity to first approval | 🔄 Evolved | 🌐 Universal | Lean | P2 |

## Planned — A6. Code Quality & Complexity (7)

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A6-1 | Code Health Score (1-10) | 25-30 factor aggregate | ✅ Unchanged | 🌐 Universal | Traditional | P0 |
| A6-2 | Cognitive Complexity | How difficult code is for a human to understand | 🔄 Evolved | 🌐 Universal | Traditional | P2 |
| A6-3 | Cyclomatic Complexity | Linearly independent paths through code | ✅ Unchanged | 🌐 Universal | Traditional | P2 |
| A6-4 | LCOM4 | Connected components within a class — God Class detector | ✅ Unchanged | 🌐 Universal | Traditional | P2 |
| A6-5 | Technical Debt Ratio | Remediation time / estimated rewrite time | 🔄 Evolved | 🌐 Universal | Traditional | P2 |
| A6-6 | AST-Based Duplication | Structural hashing for duplicate code blocks | 🔄 Evolved | 🌐 Universal | Traditional | P3 |
| A6-7 | Maintainability Rating (A-F) | Based on technical debt ratio thresholds | ✅ Unchanged | 🌐 Universal | Traditional | P3 |

## Planned — A7. Work Classification (5)

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A7-1 | Work Type Breakdown | New Work vs Refactor vs Rework vs Help Others | ✅ Unchanged | 🌐 Universal | Lean | P1 |
| A7-2 | Innovation Rate | % merged PRs representing new feature work | ✅ Unchanged | 🌐 Universal | DevRank | P2 |
| A7-3 | Defect Rate | % merged PRs addressing defects | 🔄 Evolved | 🌐 Universal | DevRank | P2 |
| A7-4 | Investment Balance | Time allocation: roadmap vs bugs vs tech debt | ✅ Unchanged | 🌐 Universal | Lean | P2 |
| A7-5 | Inefficiency Pool | PR idle time, friction, wasted effort | 🔄 Evolved | 🌐 Universal | Lean | P3 |

## Planned — A8. Developer Experience (8)

> Flow Efficiency is implemented (see #56).

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A8-1 | Context Switch Frequency | Intra-day switches between repos/projects | ✅ Unchanged | 🌐 Universal | SPACE | P2 |
| A8-2 | Cognitive Load Distribution | How evenly complex work distributes | 🔄 Evolved | 🌐 Universal | SPACE | P2 |
| A8-3 | Decision Latency | Time from problem identification to decision | ✅ Unchanged | 🌐 Universal | DevRank | P3 |
| A8-4 | Collaboration Asymmetry | Help-given vs help-received ratio | ✅ Unchanged | 🌐 Universal | Network | P3 |
| A8-5 | On-Call Burden | Time/frequency of on-call rotations | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-6 | Time Spent in Meetings | Meeting load as productivity drain | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-7 | Onboarding Time | Time for new hires to reach first productive contribution | ✅ Unchanged | 🌐 Universal | SPACE | P3 |
| A8-8 | Work-Life Balance Signals | Late-night/weekend patterns | ✅ Unchanged | 🌐 Universal | SPACE | P3 |

## Planned — A9. PR Quality & Risk (8)

> Discussion Cycles, Delivery Risk Score, Review Coverage are implemented (see #19, #58, #59).

| # | Metric | Description | AI Status | Structure | Framework | Priority |
|---|--------|-------------|-----------|-----------|-----------|----------|
| A9-1 | Estimated Review Time | ML-based minutes estimate per PR | 🔄 Evolved | 🌐 Universal | DevRank | P2 |
| A9-2 | PR Maturity Ratio | How much a PR changes between open and merge | ✅ Unchanged | 🌐 Universal | DevRank | P2 |
| A9-3 | Unreviewed PR Rate | % PRs merged with zero review | ⚠️ Caution | 🌐 Universal | DevRank | P2 |
| A9-4 | Productive Impact | Impact × (1 - Rework Rate) | 🔄 Evolved | 🌐 Universal | Lean | P2 |
| A9-5 | tt100 (Time to 100) | Time to write 100 lines of productive code | ✅ Unchanged | 🌐 Universal | Lean | P3 |
| A9-6 | Idle Completion Time | Time from rework complete to merge | ✅ Unchanged | 🌐 Universal | Lean | P2 |
| A9-7 | PRs Unlinked | % PRs not linked to issue tracker | ✅ Unchanged | 🌐 Universal | DevRank | P2 |
| A9-8 | Batch Size Classification | Small/Medium/Large/Gigantic weighted blend | 🔄 Evolved | 🌐 Universal | DevRank | P3 |

---

## CI/CD Build Metrics (3)

Standard CI/CD metrics. Require connection to GitHub Actions, GitLab CI, or Jenkins.

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CI1 | Build Count | Number of builds per period (day/week/month), by team/repo/branch. Includes both CI builds (test, lint) and deployment pipelines. Measures team integration rhythm — a team with few builds integrates rarely, a signal of too-large batch size or fear of merging. | 🆕 New | 🌐 Universal | P1 |
| CI2 | Build Duration | Average/median/p95 pipeline execution time from start to completion. Measured per pipeline type (CI, deploy, test suite). A growing trend signals codebase complexity or slow tests — both slow developer feedback and impact cycle time. | 🆕 New | 🌐 Universal | P1 |
| CI3 | Build Success Rate | % of builds completed successfully vs failed, per team/repo/period. Correlated with Change Failure Rate (DORA) but more granular — CFR only measures production deploys, Build Success Rate covers all pipelines. High failure rate on CI indicates flaky tests, broken configs, or unstable code being pushed too often. | 🆕 New | 🌐 Universal | P1 |

---

## Advanced AI Metrics (9)

Emerging in 2026, measuring real AI impact on development. Beyond adoption rate — they measure cost, efficiency, quality, and ROI. No competitor has all of them.

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

## Competitive Gap Metrics — GitKraken Insights (11)

Metrics identified from competitive analysis of GitKraken Insights official documentation (March 2026). Close specific gaps GitKraken uses as sales messaging that are becoming market standard.

### Group 1 — Pull Request extended (2)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG1 | PR Comments Count | Total comments left on PRs per period, per team/repo/dev. Measures review engagement and feedback depth. Complements Inline Comment Density (#64, ratio) and Review Comment Substance (#65, quality) — PR Comments Count is raw volume. High volume can indicate thorough reviews or PRs too large to grok. Correlate with PR Size Distribution to interpret correctly. | 🆕 New | 🌐 Universal | P2 |
| CG2 | Code Review Hours (Cumulative) | Average hours spent in review per committer (Total review hours ÷ committers). Differs from Review Turnaround Time (#62, single-review duration) and Reviews Given (#61, count). CRH measures cumulative review load — reveals if a team has few "review bottleneck" devs doing most of the work or if load is distributed. Lets a leader balance workload and identify dev overloaded with review vs dev focused on writing code. | 🆕 New | 🌐 Universal | P1 |

### Group 2 — Velocity extended (2)

| # | Metric | Description | AI Status | Structure | Priority |
|---|--------|-------------|-----------|-----------|----------|
| CG3 | Commit Count | Total commits pushed per period across all connected repos. Volume metric — not productivity standalone (more commits ≠ more productive, can indicate too-small commits) but useful as denominator/context for other metrics. Every competitor (GitKraken, LinearB, Jellyfish) exposes it explicitly despite low standalone analytical value. Include for completeness and competitive parity. | ⚠️ Caution | 🌐 Universal | P2 |
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

---

## Lead Time Percentiles (extension of an existing metric)

| Metric | What changes | Structure |
|--------|--------------|-----------|
| **Lead Time Percentiles** (extension of Ticket Lead Time PM14) | Not a new metric. Adds percentile calculation (p50, p75, p85, p95) on top of average. Box Plot in Superset. | 🌐 Universal |

---

## Deferred: Missing Data Sources

Following metrics are blocked by missing data in the current pipeline. Domain models exist or are easy to add — the gating factor is upstream data ingestion.

### D1. Issue Tracker / PM Tool Data Required

PM-tool metrics (PM1–PM15, V1–V3, K1–K6, S1–S2, W1–W3) require Jira/Linear/Asana adapter. Define an `IssueRecord` domain model with fields for issue type, sprint, status transitions, and timestamps. Partial inference from PR labels and conventional commit prefixes (`fix:`, `feat:`, `chore:`) is possible but insufficient for sprint-level accuracy.

### D2. CI/CD Pipeline Data Required

CI/CD metrics (CI1–CI3) require GitHub Actions / Jenkins / GitLab CI integration. The fetcher's `fetch_workflow_runs()` exists but is not wired (no metric currently consumes CI data). Re-enable and add the three metrics above.

### D3. AI Tool API Data Required

Advanced AI metrics (AI-A1 through AI-A9, ★1–★6 partially) require integration with Copilot Metrics API, Cursor API, or Claude Code billing logs. ROI metrics (AI-A1, AI-A2, AI-A3) need both license costs and token consumption.

### D4. External Platform Integration Required

Some Developer Experience metrics need data from systems entirely outside Git/code-review:

| Metric | Needs |
|--------|-------|
| On-Call Burden (A8-5) | PagerDuty, OpsGenie, or Grafana OnCall schedule data |
| Onboarding Time (A8-7) | Employee start dates from HR system or directory service |
| Time Spent in Meetings (A8-6) | Calendar data from Google Calendar, Outlook, etc. |
| Comprehension Debt (★3), DXI (★5) | Survey infrastructure |

> **Lowest-effort approach:** accept CSV/YAML imports for on-call schedules, employee directories, and survey results.

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
| Planned (A1-A9) | 40 |
| New AI-Era (★1–★6) | 6 |
| New PM Tool (Groups 1+2+3) | 17 |
| Methodology Variants (V1–V3) | 3 |
| Kanban-only (K1–K6) | 6 |
| Scrum-only (S1–S2) | 2 |
| Waterfall-only (W1–W3) | 3 |
| New CI/CD Build (CI1–CI3) | 3 |
| Advanced AI (AI-A1–AI-A9) | 9 |
| Competitive Gap GitKraken (CG1–CG11) | 11 |
| Lead Time Percentiles (extension) | 1 |
| **Total** | **179** |

| By structural category | Count |
|------------------------|-------|
| 🌐 Universal | ~163 |
| 🔀 Variant | 3 |
| 📋 Kanban-only | 6 |
| 🏃 Scrum-only | 2 |
| 🏗️ Waterfall-only | 3 |
| Custom graph component (D3.js/Cytoscape) | 4 of A3 |

| By data source | Count |
|----------------|-------|
| Git only (GitHub/GitLab) | ~110 |
| PM Tool (Jira/Linear) | ~17 |
| Git + PM Tool combined | 4 |
| CI/CD (GitHub Actions/Jenkins/GitLab CI) | 3 |
| AI Tool API (Copilot/Cursor/Claude Code) | ~9 |
| Survey (DXI, Comprehension Debt) | ~2 |
| Custom graph component | 4 |

---

## Framework Coverage Summary

> Note: metrics can belong to multiple frameworks (e.g., Cycle Time is DORA + SPACE + Lean). Totals exceed metric count due to multi-tagging.

| Framework | Implemented | Planned | Total | % of All |
|-----------|-------------|---------|-------|----------|
| DORA | 2 | 5 | 7 | ~9% |
| SPACE | 12 | 8 | 20 | ~25% |
| CodeScene | 18 | 4 | 22 | ~28% |
| Lean | 13 | 9 | 22 | ~28% |
| Traditional | 1 | 8 | 9 | ~11% |
| Network | 5 | 7 | 12 | ~15% |
| DevRank | 35 | 11 | 46 | ~58% |

---

*DevRank Metrics Overview v5 — updated April 2026*
*Sources: previous METRICS_OVERVIEW.md + competitive analysis of GitKraken Insights (March 2026 official docs) + 2026 emerging AI metrics research + PM tool integration design + live metric registry verification*
