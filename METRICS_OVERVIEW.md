# DevRank Metrics Overview

**57 implemented | 66 planned | 123 total**

---

## Implemented Metrics (57)

### Authored (42)

| # | Metric | Description | Framework |
|---|--------|-------------|-----------|
| 1 | AI-Assisted PR Rate | % PRs created with AI assistance (Copilot/Cursor/Claude/etc.) | DevRank |
| 2 | PR Throughput | Number of PRs merged in the period | SPACE • Lean |
| 3 | Delivery Volume | Total lines added+deleted across merged PRs | SPACE |
| 4 | Net Code Contribution | Lines added minus lines deleted | SPACE |
| 5 | Cycle Time | Time from PR creation to merge | DORA • SPACE • Lean |
| 6 | Coding Time To PR | Time from first commit to PR creation | Lean |
| 7 | Merge Delay | Time from first approval to merge | Lean |
| 8 | PR Size Distribution | Statistical distribution of PR sizes (small/medium/large/XL) | DevRank |
| 9 | Trivial Contribution Rate | % of PRs that are trivial (auto-generated, tiny, boilerplate) | DevRank |
| 10 | Code Churn Rate | % of recently written lines overwritten within the period | SPACE • CodeScene |
| 11 | Rework Rate | % of changes that rewrite the author's own recent code | SPACE • Lean |
| 12 | First-Time Approval Rate | % of PRs approved without change requests | SPACE • DevRank |
| 13 | Review Iterations | Number of review round-trips before merge | Lean • DevRank |
| 14 | Follow-Up Commit Rate | % of PRs with post-review follow-up commits | DevRank |
| 15 | Self-Merge Rate | % of PRs merged without non-author approval | DevRank |
| 16 | Abandoned PR Rate | % of PRs closed without merging | Lean |
| 17 | PR Merge Effectiveness | Ratio of merged PRs to total PRs opened | SPACE |
| 18 | PR Body Quality Score | Quality of PR descriptions (length, structure, links) | DevRank |
| 19 | Conventional Commit Rate | % of commits following conventional commit format | DevRank |
| 20 | Test File Ratio | Ratio of test file changes to production file changes | Traditional |
| 21 | Documentation Touch Rate | % of PRs that include documentation changes | DevRank |
| 22 | Dependency Change Rate | % of PRs modifying dependency/manifest files | DevRank |
| 23 | Module / Area Breadth | Number of distinct modules/areas touched | DevRank |
| 24 | PR Category Diversity | Diversity of PR types (features, fixes, refactors, etc.) | DevRank |
| 25 | Bug Fix Focus Rate | % of PRs addressing bug fixes | DevRank |
| 26 | Coding Days | Number of days with commit activity | SPACE |
| 27 | Active Weeks | Number of weeks with at least one contribution | SPACE |
| 28 | Off-Hours Activity Rate | % of commits made outside business hours | SPACE |
| 29 | Burstiness | Ratio of max weekly activity to average — pacing/sustainability | DevRank |
| 30 | Revert Introduction Rate | % of PRs that introduce reverts | DevRank |
| 31 | Hotspot Detection | Files with highest revision frequency × complexity | CodeScene |
| 32 | Bus Factor | Min developers who could leave before code is unmaintainable | CodeScene |
| 33 | Temporal / Logical Coupling | Files that always change together (hidden dependencies) | CodeScene |
| 34 | Entity Fragmentation | Herfindahl-like index of author scatter per file | CodeScene |
| 35 | Complexity Trend | Whitespace-based complexity tracked per file over time | Traditional |
| 36 | Change Proximity | Sum of distances between changed lines within a file | CodeScene |
| 37 | Sum of Coupling | Per-entity total coupling score across all revisions | CodeScene |
| 38 | Absolute Churn Trend | Lines added/deleted per date — detects integration bottlenecks | CodeScene |
| 39 | Commit Message Mining | Regex search of commit messages for defect indicators | CodeScene |
| 40 | Code Survival | % of a developer's contributed lines still alive over time | CodeScene |
| 41 | Time to First Review | Median time from PR creation to initial reviewer feedback | Lean |
| 42 | Slow Review Response | Median author response time to changes-requested reviews | Lean |

### Influence (14)

| # | Metric | Description | Framework |
|---|--------|-------------|-----------|
| 41 | Reviews Given | Total reviews submitted for others' PRs | SPACE |
| 42 | Review Turnaround Time | Time from review request to review submission | SPACE • Lean |
| 43 | Unblock Time | Time taken to unblock others via reviews/approvals | Lean |
| 44 | Inline Comment Density | Ratio of inline (file-level) comments to total review comments | DevRank |
| 45 | Review Comment Substance | Pygments-based scoring of code content in review comments | DevRank |
| 46 | Review Leverage | Lines of code influenced per review given | DevRank |
| 47 | Review Breadth | Number of distinct PR authors reviewed | SPACE • Network |
| 48 | Review Demand | How sought-after as a reviewer (requests received) | Network |
| 49 | PR Merge Rate | % of reviewed PRs that ultimately merged | DevRank |
| 50 | Approval To Merge Ratio | Ratio of approvals given to actual merges | DevRank |
| 51 | Change-Inducing Review Rate | % of reviews that led to code changes | DevRank |
| 52 | Blocking Comment Rate | % of review comments that block merge | DevRank |
| 53 | First Reviewer Rate | % of reviews where this person was the first reviewer | DevRank |
| 54 | Mentorship Signal | Reviews targeting PRs from low-activity contributors | SPACE • Network |

### Mixed (1)

| # | Metric | Description | Framework |
|---|--------|-------------|-----------|
| 55 | Co-Author Contribution Rate | % of commits with co-author trailers | SPACE • Network |

---

## Planned Metrics (66)

### AI-Era Prioritization Rationale

In the AI-assisted development era, metric priorities shift significantly from traditional software engineering:

**P0 (Critical) - Implement First:**
1. **AI Transparency** - Without visibility into AI contribution (AI-Assisted PR Rate, AI Code Quality), teams cannot assess AI tool ROI or risks
2. **Knowledge Risk** - AI-generated code creates "phantom ownership" where no human truly understands the code. Bus Factor and Knowledge Islands become existential risks
3. **DORA Fundamentals** - Lead Time and Deployment Frequency remain the ultimate measures of delivery performance
4. **Quality Gates** - With AI producing code faster, Delivery Risk Score and Review Coverage prevent quality collapse

**P1 (High) - Implement Second:**
1. **Work Classification** - Understanding what work AI does vs humans enables resource allocation decisions
2. **Flow Efficiency** - AI-human handoffs in the PR process create new bottlenecks
3. **Code Quality** - Cognitive complexity matters more when AI generates verbose solutions
4. **Network Health** - Collaboration patterns reveal if AI is isolating developers or enhancing teamwork

**P2/P3 (Medium/Low) - Implement Later:**
- Traditional complexity metrics (cyclomatic, LCOM4) - AI-generated code often has different complexity patterns
- Meeting time tracking - Less relevant as AI reduces certain coordination needs
- Code age/entropy - Long-term metrics that matter less in rapidly evolving AI-assisted codebases

### A1. AI-Assisted Development (The New Foundation)

| # | Metric | Description | Priority | Framework | Status |
|---|--------|-------------|----------|-----------|--------|
| 1 | AI-Assisted PR Rate | % PRs created with Copilot/Cursor/Claude Code detected via commit/PR signatures | P0 | DevRank | **Implemented** |
| 2 | AI Code Quality | Rework rate on AI-assisted vs human PRs | P0 | DevRank | Planned |
| 3 | AI Suggestion Acceptance | Ratio of accepted vs dismissed AI suggestions | P1 | DevRank | Planned |
| 4 | AI Adoption Rate | % team members with AI coding tool licenses | P1 | DevRank | Planned |

### A2. Knowledge Ownership & Risk (Critical with AI-Generated Code)

| # | Metric | Description | Priority | Framework | Status |
|---|--------|-------------|----------|-----------|--------|
| 5 | Bus Factor | Min developers who could leave before code is unmaintainable | P0 | CodeScene | **Implemented** |
| 6 | Knowledge Islands | Files/modules where 95%+ written by one person | P0 | CodeScene | Planned |
| 7 | AI Phantom Ownership | Code primarily touched by AI with low human review depth | P0 | DevRank | Planned |
| 8 | Knowledge Loss | Code where 50%+ written by departed/inactive contributors | P1 | CodeScene | Planned |
| 9 | Knowledge Sharing Index | How evenly reviews distribute across team (0-1) | P1 | Network | Planned |
| 10 | Code Familiarity | % of codebase known by current active team | P1 | CodeScene | Planned |
| 11 | Main Developer (by lines) | Primary author per file by lines added | P2 | CodeScene | Planned |
| 12 | Main Developer (by revisions) | Primary author per file by commit count | P2 | CodeScene | Planned |
| 13 | Entity Ownership | Per-author contribution percentages per file | P2 | CodeScene | Planned |
| 14 | Contributor Experience | % of lines by the top contributor per file | P3 | CodeScene | Planned |

### A3. DORA & Deployment (The Delivery Foundation)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 15 | Lead Time for Changes | Commit to production duration (end-to-end) | P0 | DORA |
| 16 | Deployment Frequency | How often code deploys to production | P0 | DORA |
| 17 | Change Failure Rate | % deployments causing failures | P0 | DORA |
| 18 | Mean Time to Recovery | Incident recovery duration | P0 | DORA |
| 19 | Time to Deploy | PR merge to production deployment | P1 | DORA • Lean |

### A4. PR Quality & Risk (AI-Era Quality Gates)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 20 | Delivery Risk Score (1-10) | Per-commit risk based on code, file count, diffusion, experience | P0 | DevRank |
| 21 | Review Coverage | % of PR files/hunks with at least one review comment | P0 | DevRank |
| 22 | PR Maturity Ratio | How much a PR changes between open and merge | P1 | DevRank |
| 23 | Discussion Cycles | Alternating-person comment exchanges | P1 | DevRank |
| 24 | Estimated Review Time | ML-based minutes estimate per PR | P1 | DevRank |
| 25 | Idle Completion Time | Time from rework complete to merge | P1 | Lean |
| 26 | Productive Impact | Impact minus rework: Impact × (1 - Rework Rate) | P1 | Lean |
| 27 | PRs Unlinked | % PRs not linked to issue tracker | P2 | DevRank |
| 28 | tt100 (Time to 100) | Time to write 100 lines of productive code | P2 | Lean |
| 29 | Commitment Reliability Rate | Sprint completion excluding injected issues | P2 | Lean |

> **Notes:**
> - Unreviewed PR Rate is covered by Self-Merge Rate
> - Batch Size Classification is covered by PR Size Distribution

### A5. Code Quality & Complexity (Scale Requires Quality)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 30 | Code Health Score (1-10) | 25-30 factor aggregate (brain classes, DRY, nesting, etc.) | P0 | Traditional |
| 31 | Cognitive Complexity | How difficult code is for a human to understand | P1 | Traditional |
| 32 | Technical Debt Ratio | Remediation time / estimated rewrite time | P1 | Traditional |
| 33 | Cyclomatic Complexity | Linearly independent paths through code (McCabe) | P2 | Traditional |
| 34 | LCOM4 (Lack of Cohesion) | Connected components within a class — God Class detector | P2 | Traditional |
| 35 | Delta Maintainability Model | Per-function complexity via lizard | P2 | Traditional |
| 36 | AST-Based Duplication | Structural hashing to find duplicate/similar code blocks | P3 | Traditional |
| 37 | Maintainability Rating (A-F) | Based on technical debt ratio thresholds | P3 | Traditional |

### A6. Work Classification (Understanding AI vs Human Work)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 38 | Work Type Breakdown | New Work vs Refactor vs Rework vs Help Others | P0 | Lean |
| 39 | Innovation Rate | % merged PRs representing new feature work | P1 | DevRank |
| 40 | Defect Rate | % merged PRs addressing defects | P1 | DevRank |
| 41 | Investment Balance | Time allocation: roadmap vs bugs vs tech debt vs unplanned | P1 | Lean |
| 42 | Inefficiency Pool | PR idle time, friction, wasted effort | P2 | Lean |

### A7. Cycle Time Sub-Phases (Flow Optimization)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 43 | Pickup Time | PR opened to first non-author review activity | P0 | Lean |
| 44 | Time to Approve | First review activity to first approval | P1 | Lean |
| 45 | Deploy Time | Merge to production release | P1 | DORA • Lean |

### A8. Graph/Network Collaboration (Human-AI Team Dynamics)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 46 | Degree Centrality | Number of direct collaborators | P1 | Network |
| 47 | Betweenness Centrality | Whether a developer bridges disconnected teams | P1 | Network |
| 48 | Communication Strength | Conway's Law heuristic via shared commits | P1 | Network |
| 49 | Review Network Density | How interconnected the review graph is | P1 | Network |
| 50 | Collaboration Asymmetry Index | Help-giving vs help-receiving ratio | P2 | Network |
| 51 | Closeness Centrality | How quickly a developer can reach the entire org | P2 | Network |
| 52 | Eigenvector Centrality | Influence through association with influential devs | P2 | Network |
| 53 | Team Coupling | Overlap in commits to same code by different teams | P2 | Network |
| 54 | Team Cohesion | Whether team members work in the same code areas | P2 | Network |

### A9. Developer Experience (AI Should Reduce Load)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 55 | Flow Efficiency | % of days an issue was actively worked on vs total lifetime | P1 | Lean |
| 56 | Cognitive Load Distribution | How evenly complex work distributes across team | P1 | SPACE |
| 57 | Context Switch Frequency | Intra-day switches between repos/projects | P2 | SPACE |
| 58 | Decision Latency | Time from problem identification to decision | P2 | DevRank |
| 59 | On-Call Burden | Time/frequency of on-call rotations | P2 | SPACE |
| 60 | Collaboration Asymmetry | Help-given vs help-received ratio | P2 | Network |
| 61 | Onboarding Time | Time for new hires to reach first productive contribution | P2 | SPACE |
| 62 | Work-Life Balance Signals | Late-night/weekend patterns beyond off-hours rate | P2 | SPACE |
| 63 | Time Spent in Meetings | Meeting load as productivity drain | P3 | SPACE |

### A10. Codebase Evolution (Long-Term Health)

| # | Metric | Description | Priority | Framework |
|---|--------|-------------|----------|-----------|
| 64 | Code Age | Months since last modification per file | P2 | CodeScene |
| 65 | History Complexity (Entropy) | Normalized entropy of changes across files | P3 | CodeScene |
| 66 | Hunks Count (Change Fragmentation) | Median diff hunks per file — scattered hunks = higher risk | P3 | CodeScene |

---

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

### Notes on Framework Overlap

- **DORA metrics** are a subset of SPACE (Performance/Efficiency) and Lean (flow metrics)
- **CodeScene metrics** often overlap with SPACE's Communication dimension (knowledge sharing)
- **Lean metrics** heavily overlap with SPACE's Efficiency and Activity dimensions
- **Network metrics** primarily map to SPACE's Communication & Collaboration dimension
- **DevRank-specific metrics** fill gaps in existing frameworks, particularly around AI-assisted development and PR-level quality signals

### Framework Coverage Summary

| Framework | Implemented | Planned | Total | % of All |
|-----------|-------------|---------|-------|----------|
| DORA | 1 | 5 | 6 | 5% |
| SPACE | 25 | 10 | 35 | 27% |
| CodeScene | 9 | 10 | 19 | 15% |
| Lean | 12 | 19 | 31 | 24% |
| Traditional | 2 | 8 | 10 | 8% |
| Network | 3 | 11 | 14 | 11% |
| DevRank | 28 | 22 | 50 | 39% |

> **Note:** Metrics can belong to multiple frameworks (e.g., Cycle Time is DORA + SPACE + Lean). Totals exceed 130 due to multi-tagging.
> **New in AI Era:** Added "AI Phantom Ownership" metric to track code primarily touched by AI with low human review depth.
