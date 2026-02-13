# Proposed Metrics — Unimplemented

Status: 42 metrics currently implemented (30 authored, 12 influence).
All 12 metrics from the previous proposal round have been implemented or intentionally dropped (First-Time Approval Rate + Coding Time To PR + Coding Days + Rework Rate + Merge Delay now live).

The gaps below were identified by cross-referencing our metric set against
DORA (2025), LinearB benchmarks (8.1M PRs), SPACE, Pluralsight Flow, GitClear,
Swarmia, and Graphite. Only metrics derivable purely from GitHub PR/commit/review
data are listed.

---

## Authored

### 1. First-Time Approval Rate

% of an engineer's PRs that receive approval on the first review round
(zero CHANGES_REQUESTED reviews before the first APPROVED review).

- **Frameworks:** LinearB (top-5 KPI), Swarmia, Graphite
- **Distinct from:** `review_iterations` (average rounds — a continuous number, not
  a pass/fail ratio), `pr_merge_effectiveness` (average back-and-forth count).
  Neither gives the direct "% first-pass" number that managers universally ask for.
- **Data:** For each merged PR, check if any CHANGES_REQUESTED review exists
  before the first APPROVED review. Binary per-PR → aggregate ratio.
- **Feasibility:** YES — `ReviewRecord.state` (APPROVED, CHANGES_REQUESTED) and
  `ReviewRecord.submitted_at` are both present in our data model and sample dumps.
- **Why it matters:** The single most requested code-quality-at-submission metric.
  LinearB benchmark: elite teams achieve >80%.

### 2. Coding Time To PR (implemented)

Median time from an engineer's first commit on a branch to PR creation.

- **Frameworks:** LinearB (one of their 4 cycle-time sub-phases), GitClear
- **Distinct from:** `cycle_time` (PR open → merge, which is the post-PR portion).
  Coding Time measures the pre-PR development window — how long code is worked
  on before it's opened for review.
- **Data:** For each PR, find the earliest commit timestamp in that PR's commit
  list, then compute `pr.created_at - earliest_commit.date`.
- **Feasibility:** YES — `Commit.date`, `Commit.pull_request_number`, `Commit.idx`
  (ordering), and `PullRequest.created_at` are all present. (See `coding_time_to_pr.py`.)
- **Why it matters:** Long coding times reveal excessive batching or blocked
  engineers. LinearB benchmark: elite <1h, strong <8h.

### 3. Coding Days (implemented)

Number of distinct calendar days with at least one commit, expressed as a ratio
of working days in the period.

- **Frameworks:** Pluralsight Flow (core metric), GitClear (core metric)
- **Distinct from:** `active_weeks` (weekly granularity, measures max gap).
  Coding Days is daily-granularity engagement, much more informative than
  weekly gaps for spotting inconsistency.
- **Data:** Deduplicate commit dates per engineer → count unique days.
- **Feasibility:** YES — `Commit.date` and `Commit.author.login` are present.
  Timezone-aware day bucketing possible via `CanonicalBundle.user_timezone`. (See `coding_days.py`.)
- **Why it matters:** Industry-standard engagement metric.
  GitClear benchmark: median developer averages 156 active days/year (~60%).

### 4. Rework Rate (implemented)

% of code changes in an engineer's PRs that modify lines the same author wrote
within the previous 21 days (self-rework on recently written code).

- **Frameworks:** DORA (5th official metric as of 2025), LinearB, GitClear,
  Pluralsight Flow ("Efficiency" = inverse of rework)
- **Distinct from:** `code_churn_rate` (max weekly churn — an aggregate volatility
  signal, not self-rework), `revert_introduction_rate` (only explicit reverts).
  Neither captures the "wasted effort" dimension of rewriting your own recent code.
- **Data:** For each changed file in a PR, compare modified line ranges against
  the same author's changes in PRs merged within the prior 21 days. Requires
  file-level diff analysis across PRs. (Plumbing added: FileRecord.patch + hunk parser in utils.)
- **Feasibility:** YES — now enabled via patch loading and _parse_hunk_lines.
- **Why it matters:** THE breakout metric of 2025. Being DORA's 5th metric gives
  it institutional weight. DORA benchmark: elite teams <2%, median 8-16%.
  Directly measures wasted engineering effort.

### 5. Merge Delay (implemented)

Median time from last approval to actual merge (approval → merge gap).

- **Frameworks:** Graphite (core metric), LinearB (deploy-phase proxy)
- **Distinct from:** `cycle_time` (PR open → merge, the full window). Merge Delay
  isolates the post-approval bottleneck — CI queues, merge-train waits,
  manual deployment gates, or simple inattention.
- **Data:** For each merged PR, find the latest APPROVED review timestamp,
  then compute `pr.merged_at - latest_approval.submitted_at`. (See `merge_delay.py`.)
- **Feasibility:** YES — `ReviewRecord.state` (APPROVED), `ReviewRecord.submitted_at`,
  and `PullRequest.merged_at` are all present.
- **Why it matters:** A PR approved but not merged is dead inventory. This
  metric surfaces process friction that cycle_time obscures by averaging
  with development time.

---

## Summary

| # | Metric               | Category | Feasible? | Distinct From                      | Key Frameworks          |
|---|----------------------|----------|-----------|------------------------------------|-------------------------|
| 1 | First-Time Approval  | Authored | YES       | review_iterations, pr_merge_eff.   | LinearB, Swarmia        |
| 2 | Coding Time          | Authored | YES       | cycle_time                         | LinearB, GitClear       |
| 3 | Coding Days          | Authored | YES       | active_weeks                       | Pluralsight, GitClear   |
| 4 | Rework Rate          | Authored | NOT YET   | code_churn_rate, revert_intro_rate | DORA, LinearB, GitClear |
| 5 | Merge Delay          | Authored | YES       | cycle_time                         | Graphite, LinearB       |

4 of 5 are immediately implementable with our current data model.
Rework Rate (#4) requires adding `patch` field to `FileRecord` and updating the
adapter — the raw data exists in `files.jsonl` but is not loaded. It is also the
most complex (file-level cross-PR diff analysis) but the highest-value addition
as the official 5th DORA metric.

### Not proposed (require external data)

These industry-standard metrics were evaluated but excluded because they need
data sources beyond GitHub:

- **Deployment Frequency** — DORA #1; requires CI/CD pipeline data
- **Lead Time for Changes** (full) — DORA #2; requires deployment timestamps
- **Change Failure Rate** — DORA #3 / DX Core 4; requires incident tracking
- **Failed Deployment Recovery Time** — DORA #4; requires incident management
- **Developer Satisfaction (DXI)** — SPACE / DX Core 4; requires surveys
- **Investment Balance** — Jellyfish, Swarmia; requires issue tracker categorization
- **Flow Efficiency** — Swarmia; requires issue tracker status transitions
