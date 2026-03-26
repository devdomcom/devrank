# DevRank Metrics Overview

**53 implemented | 67 planned | 120 total**

---

## Implemented Metrics (53)

### Authored (38)

| # | Metric | Description |
|---|--------|-------------|
| 1 | PR Throughput | Number of PRs merged in the period |
| 2 | Delivery Volume | Total lines added+deleted across merged PRs |
| 3 | Net Code Contribution | Lines added minus lines deleted |
| 4 | Cycle Time | Time from PR creation to merge |
| 5 | Coding Time To PR | Time from first commit to PR creation |
| 6 | Merge Delay | Time from first approval to merge |
| 7 | PR Size Distribution | Statistical distribution of PR sizes (small/medium/large/XL) |
| 8 | Trivial Contribution Rate | % of PRs that are trivial (auto-generated, tiny, boilerplate) |
| 9 | Code Churn Rate | % of recently written lines overwritten within the period |
| 10 | Rework Rate | % of changes that rewrite the author's own recent code |
| 11 | First-Time Approval Rate | % of PRs approved without change requests |
| 12 | Review Iterations | Number of review round-trips before merge |
| 13 | Follow-Up Commit Rate | % of PRs with post-review follow-up commits |
| 14 | Self-Merge Rate | % of PRs merged without non-author approval |
| 15 | Abandoned PR Rate | % of PRs closed without merging |
| 16 | PR Merge Effectiveness | Ratio of merged PRs to total PRs opened |
| 17 | PR Body Quality Score | Quality of PR descriptions (length, structure, links) |
| 18 | Conventional Commit Rate | % of commits following conventional commit format |
| 19 | Test File Ratio | Ratio of test file changes to production file changes |
| 20 | Documentation Touch Rate | % of PRs that include documentation changes |
| 21 | Dependency Change Rate | % of PRs modifying dependency/manifest files |
| 22 | Module / Area Breadth | Number of distinct modules/areas touched |
| 23 | PR Category Diversity | Diversity of PR types (features, fixes, refactors, etc.) |
| 24 | Bug Fix Focus Rate | % of PRs addressing bug fixes |
| 25 | Coding Days | Number of days with commit activity |
| 26 | Active Weeks | Number of weeks with at least one contribution |
| 27 | Off-Hours Activity Rate | % of commits made outside business hours |
| 28 | Burstiness | Ratio of max weekly activity to average — pacing/sustainability |
| 29 | Revert Introduction Rate | % of PRs that introduce reverts |
| 30 | Hotspot Detection | Files with highest revision frequency × complexity |
| 31 | Temporal / Logical Coupling | Files that always change together (hidden dependencies) |
| 32 | Entity Fragmentation | Herfindahl-like index of author scatter per file |
| 33 | Complexity Trend | Whitespace-based complexity tracked per file over time |
| 34 | Change Proximity | Sum of distances between changed lines within a file |
| 35 | Sum of Coupling | Per-entity total coupling score across all revisions |
| 36 | Absolute Churn Trend | Lines added/deleted per date — detects integration bottlenecks |
| 37 | Commit Message Mining | Regex search of commit messages for defect indicators |
| 38 | Code Survival | % of a developer's contributed lines still alive over time |

### Influence (14)

| # | Metric | Description |
|---|--------|-------------|
| 39 | Reviews Given | Total reviews submitted for others' PRs |
| 40 | Review Turnaround Time | Time from review request to review submission |
| 41 | Unblock Time | Time taken to unblock others via reviews/approvals |
| 42 | Inline Comment Density | Ratio of inline (file-level) comments to total review comments |
| 43 | Review Comment Substance | Pygments-based scoring of code content in review comments |
| 44 | Review Leverage | Lines of code influenced per review given |
| 45 | Review Breadth | Number of distinct PR authors reviewed |
| 46 | Review Demand | How sought-after as a reviewer (requests received) |
| 47 | PR Merge Rate | % of reviewed PRs that ultimately merged |
| 48 | Approval To Merge Ratio | Ratio of approvals given to actual merges |
| 49 | Change-Inducing Review Rate | % of reviews that led to code changes |
| 50 | Blocking Comment Rate | % of review comments that block merge |
| 51 | First Reviewer Rate | % of reviews where this person was the first reviewer |
| 52 | Mentorship Signal | Reviews targeting PRs from low-activity contributors |

### Mixed (1)

| # | Metric | Description |
|---|--------|-------------|
| 53 | Co-Author Contribution Rate | % of commits with co-author trailers |

---

## Planned Metrics (67)

### A1. Codebase Evolution

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 1 | Code Age | Months since last modification per file | P1 |
| 2 | History Complexity (Entropy) | Normalized entropy of changes across files | P2 |
| 3 | Hunks Count (Change Fragmentation) | Median diff hunks per file — scattered hunks = higher risk | P3 |
| 4 | Delta Maintainability Model | Per-function cyclomatic complexity via lizard | P2 |
| 5 | Contributor Experience | % of lines by the top contributor per file | P3 |

### A2. Knowledge Ownership & Risk

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 6 | Bus Factor | Min developers who could leave before code is unmaintainable | P0 |
| 7 | Knowledge Islands | Files/modules where 95%+ written by one person | P0 |
| 8 | Knowledge Loss | Code where 50%+ written by departed/inactive contributors | P1 |
| 9 | Main Developer (by lines) | Primary author per file by lines added | P1 |
| 10 | Main Developer (by revisions) | Primary author per file by commit count | P2 |
| 11 | Entity Ownership | Per-author contribution percentages per file | P2 |
| 12 | Knowledge Sharing Index | How evenly reviews distribute across team (0-1) | P1 |
| 13 | Code Familiarity | % of codebase known by current active team | P2 |

### A3. Graph/Network Collaboration

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 14 | Degree Centrality | Number of direct collaborators | P0 |
| 15 | Betweenness Centrality | Whether a developer bridges disconnected teams | P0 |
| 16 | Closeness Centrality | How quickly a developer can reach the entire org | P1 |
| 17 | Eigenvector Centrality | Influence through association with influential devs | P2 |
| 18 | Communication Strength | Conway's Law heuristic via shared commits | P1 |
| 19 | Review Network Density | How interconnected the review graph is | P1 |
| 20 | Collaboration Asymmetry Index | Help-giving vs help-receiving ratio | P2 |
| 21 | Team Coupling | Overlap in commits to same code by different teams | P2 |
| 22 | Team Cohesion | Whether team members work in the same code areas | P2 |

### A4. DORA & Deployment

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 23 | Deployment Frequency | How often code deploys to production | P1 |
| 24 | Lead Time for Changes | Commit to production duration | P1 |
| 25 | Change Failure Rate | % deployments causing failures | P1 |
| 26 | Mean Time to Recovery | Incident recovery duration | P1 |
| 27 | Time to Deploy | PR merge to production deployment | P2 |

### A5. Cycle Time Sub-Phases

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 28 | Pickup Time | PR opened to first non-author review activity | P1 |
| 29 | Time to Approve | First review activity to first approval | P2 |
| 30 | Deploy Time | Merge to production release | P2 |

### A6. Code Quality & Complexity

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 31 | Code Health Score (1-10) | 25-30 factor aggregate (brain classes, DRY, nesting, etc.) | P1 |
| 32 | Cognitive Complexity | How difficult code is for a human to understand (SonarSource spec) | P2 |
| 33 | Cyclomatic Complexity | Linearly independent paths through code (McCabe) | P2 |
| 34 | LCOM4 (Lack of Cohesion) | Connected components within a class — God Class detector | P2 |
| 35 | Technical Debt Ratio | Remediation time / estimated rewrite time | P2 |
| 36 | AST-Based Duplication | Structural hashing to find duplicate/similar code blocks | P3 |
| 37 | Maintainability Rating (A-F) | Based on technical debt ratio thresholds | P3 |

### A7. Work Classification

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 38 | Work Type Breakdown | New Work vs Refactor vs Rework vs Help Others (by line age) | P1 |
| 39 | Innovation Rate | % merged PRs representing new feature work | P2 |
| 40 | Defect Rate | % merged PRs addressing defects (keyword detection) | P2 |
| 41 | Investment Balance | Time allocation: roadmap vs bugs vs tech debt vs unplanned | P2 |
| 42 | Inefficiency Pool | PR idle time, friction, wasted effort | P3 |

### A8. Developer Experience

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 43 | Context Switch Frequency | Intra-day switches between repos/projects | P2 |
| 44 | Cognitive Load Distribution | How evenly complex work distributes across team | P2 |
| 45 | Flow Efficiency | % of days an issue was actively worked on vs total lifetime | P2 |
| 46 | Decision Latency | Time from problem identification to decision | P3 |
| 47 | Collaboration Asymmetry | Help-given vs help-received ratio | P3 |
| 48 | On-Call Burden | Time/frequency of on-call rotations | P3 |
| 49 | Time Spent in Meetings | Meeting load as productivity drain | P3 |
| 50 | Onboarding Time | Time for new hires to reach first productive contribution | P3 |
| 51 | Work-Life Balance Signals | Late-night/weekend patterns beyond off-hours rate | P3 |

### A9. PR Quality & Risk

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 52 | Estimated Review Time | ML-based minutes estimate per PR | P2 |
| 53 | PR Maturity Ratio | How much a PR changes between open and merge | P2 |
| 54 | Unreviewed PR Rate | % PRs merged with zero review (partial gap — close to self_merge_rate) | P2 |
| 55 | Delivery Risk Score (1-10) | Per-commit risk based on code, file count, diffusion, experience | P2 |
| 56 | Review Coverage | % of PR files/hunks with at least one review comment | P2 |
| 57 | Discussion Cycles | Alternating-person comment exchanges | P2 |
| 58 | Productive Impact | Impact minus rework: Impact × (1 - Rework Rate) | P2 |
| 59 | tt100 (Time to 100) | Time to write 100 lines of productive code | P3 |
| 60 | Idle Completion Time | Time from rework complete to merge | P2 |
| 61 | PRs Unlinked | % PRs not linked to issue tracker | P2 |
| 62 | Commitment Reliability Rate | Sprint completion excluding injected issues | P3 |
| 63 | Batch Size Classification | Small/Medium/Large/Gigantic weighted blend | P3 |

### A10. AI-Assisted Development

| # | Metric | Description | Priority |
|---|--------|-------------|----------|
| 64 | AI Adoption Rate | % team members with AI coding tool licenses | P2 |
| 65 | AI-Assisted PR Rate | % PRs created with Copilot/Cursor/Claude Code | P2 |
| 66 | AI Code Quality | Rework rate on AI-assisted vs human PRs | P3 |
| 67 | AI Suggestion Acceptance | Ratio of accepted vs dismissed AI suggestions | P3 |
