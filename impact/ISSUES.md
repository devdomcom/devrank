# Metrics Audit — Known Issues

## Critical

### 1. Zero Activity Rewarded as "Excellent" (8 metrics)

When there is no data, metrics default to `0.0`, and the threshold lambdas rate `0.0` as "excellent":

| Metric | Default | Threshold |
|--------|---------|-----------|
| `cycle_time` | `0.0h` | `<= 1` = excellent |
| `time_to_first_review` | `0.0h` | `<= 1` = excellent |
| `slow_review_response` | `0.0h` | `<= 2` = excellent |
| `review_turnaround_time` | `0.0h` | `<= 12` = excellent |
| `review_iterations` | `0.0` | `<= 1` = excellent |
| `revert_introduction_rate` | `0.0%` | `<= 5` = excellent |
| `trivial_contribution_rate` | `0.0/day` | `<= 0.05` = excellent |
| `unblock_time` | `0.0h` | Has a guard (`no_cr_activity` flag), **only metric protected** |

An engineer with zero activity would receive "excellent" on 7+ metrics.

**Fix**: Add a `no_data` / `insufficient_data` guard to all metrics, like `unblock_time` already has.

---

### 2. `PullRequest` Model Missing `draft` Field — Draft Filtering Is Dead Code

The `PullRequest` model in `impact/domain/models.py` has no `draft` field. The utility `filter_prs_for_contribution` in `impact/metrics/utils.py:340` uses `getattr(pr, "draft", False)` which always returns `False`. Every metric calling `filter_prs_for_contribution(..., exclude_drafts=True)` silently fails to exclude drafts:

- `bug_fix_focus_rate`
- `co_author_contribution_rate`
- `dependency_change_rate`
- `module_area_breadth`
- `pr_body_quality`
- `pr_size_distribution`
- `test_file_ratio`
- `trivial_contribution_rate`

**Fix**: Add `draft: bool = False` to the `PullRequest` model and ensure the fetcher populates it.

---

### 3. `burstiness` Computes Average Over Active Weeks Only

In `burstiness.py:49`: `avg_weekly = total_activities / active_weeks`. Inactive weeks are excluded. An engineer who works 1 week out of 52 with 10 activities gets `burst_ratio = 10/10 = 1.0` ("perfectly steady"), while an engineer working 10 of 13 weeks with varying intensity scores worse. The metric paradoxically rewards extreme inactivity.

**Fix**: Compute the average over all weeks in the period, not just active weeks.

---

### 4. `inline_comment_density` Measures the Wrong Thing

In `inline_comment_density.py:20-31`, the metric fetches PRs authored by the user, then counts inline comments from all reviewers on those PRs. It measures "how many comments do others leave on my PRs" — not "how thoroughly do I review," which the name and description imply. There is no filter on `rev.user.login == context.user_login`.

**Fix**: Either rename the metric to reflect what it measures, or change it to count inline comments the user gives on others' PRs.

---

### 5. `is_bug_fix_indicator` Massively Over-Matches

In `utils.py:300-304`, patterns like `"error"`, `"crash"`, `"regression"` are plain substring matches. Any commit containing "error" anywhere (e.g., "Add error handling", "Improve error messages", "Add error boundary") gets classified as a bug fix. This inflates `bug_fix_focus_rate` significantly.

**Fix**: Use word-boundary regex matching instead of plain `in` substring checks.

---

## High

### 6. `co_author_contribution_rate` — Wrong Definition of "Co-Author"

In `co_author_contribution_rate.py:30`: `sum(1 for c in pr_commits if c.author.login != pr.user.login)` counts any commit by someone other than the PR author — not actual co-authored commits (which use `Co-authored-by:` trailers). Merge commits, bot commits, and branch merges all count as "co-authorship."

**Fix**: Parse `Co-authored-by:` commit trailers instead of comparing commit author to PR author.

---

### 7. Artificial `0.5` Floor Fabricates Data

In `co_author_contribution_rate.py:45-46`: if `total_co_events == 0 and period_days <= 30`, `collab_per_week` is forced to `0.5`. Two engineers with zero collaboration — one measured over 29 days (gets `0.5`) and one over 31 days (gets `0.0`) — receive different scores. One day's difference flips the rating.

**Fix**: Remove the artificial floor. Use a `no_data` guard instead.

---

### 8. `pr_throughput` — Merge Ratio Divides Unrelated Populations

In `pr_throughput.py:34-43`: `opened_count` filters by `created_at` while `merged_count` filters by `merged_at`. These are disjoint populations. A PR opened in October but merged in November appears only in the November merged set. The ratio can trivially exceed 1.0 or be 0.0 when work did ship.

**Fix**: Either filter both by the same date field, or rename the metric to clarify it is not a true ratio.

---

### 9. `review_iterations` vs `slow_review_response` — Inconsistent "Change Request" Definition

- `review_iterations` uses `is_change_request()` (`utils.py:115-122`) which counts any "commented" review with inline comments as a change request.
- `slow_review_response` (`review_quality.py:133`) only counts formal `CHANGES_REQUESTED` reviews.

These metrics are in the same file and intended to work together, but disagree on what constitutes a "change request." Iterations will always be inflated relative to response times.

**Fix**: Use the same definition of "change request" in both metrics.

---

### 10. `approval_was_final` — Merge Commits Break the Check

In `utils.py:278`: `later_commits = [c for c in commits if c.date > rev_time]` rejects any approval followed by commits, including merge commits. In squash-merge or merge-commit workflows, the merge itself creates a commit after approval, causing `approval_was_final` to return `False` for nearly all approvals. This severely deflates `approval_to_merge_ratio`.

**Fix**: Filter out merge commits from the `later_commits` check.

---

### 11. `review_led_to_merge` — NoneType Crash Risk

In `utils.py:221-227`: `pr = ledger.get_pr(pr_num)` can return `None`, but the code accesses `pr.user.login` without a null check. Same issue in `review_led_to_commit` at `utils.py:248-253`. This affects `pr_merge_rate` and `change_inducing_review_rate`.

**Fix**: Add null checks for the `get_pr` return value.

---

### 12. `collect_pr_interactions` — Timeline Dedup Is Ineffective

In `utils.py:73-81`: the `seen_ts_ids` set uses keys like `("review", actor, timestamp)` but timeline entries use `("timeline", actor, timestamp)`. These keys never collide, so reviews appearing as both review objects and timeline events get double-counted, inflating `pr_merge_effectiveness` back-and-forth counts.

**Fix**: Normalize the dedup key to exclude the event kind, or match timeline events against existing reviews by actor+timestamp.

---

### 13. `change_inducing_review_rate` — Denominator Includes Approvals

In `change_inducing_review_rate.py:42`: `total_reviews = len(reviews)` includes approvals in the denominator, but approvals can never "induce changes." An engineer giving 8 approvals and 2 effective change requests scores `2/10 = 0.20` instead of `2/2 = 1.00`. This penalizes engineers who also approve good code.

**Fix**: Exclude `APPROVED` reviews from the denominator.

---

### 14. `unblock_time` — Wrong Commit Filter

In `unblock_time.py:38`: filters to `c.author.login != context.user_login` (not the reviewer), but this includes commits by anyone who isn't the reviewer — bots, other contributors, co-authors. The intent is to find commits by the PR author, but the filter is "not the reviewer" rather than "is the PR author."

**Fix**: Filter to `c.author.login == pr.user.login` instead.

---

### 15. Inconsistent Time Windows Across "Effectiveness" Definitions

| Function | Window | Used By |
|----------|--------|---------|
| `review_led_to_commit` | 24h | `change_inducing_review_rate` |
| `review_led_to_merge` | 48h | `pr_merge_rate` |
| `_is_effective_change_request` | 72h | `review_leverage` |
| `approval_was_final` | 48h | `approval_to_merge_ratio` |

No documentation explains why these windows differ. A review at hour 25 counts for merge-rate but not change-inducing-rate.

**Fix**: Document the rationale for each window, or standardize to a single configurable window.

---

## Medium

### 16. Hard Cliff Effects in Thresholds

All thresholds in `thresholds.py` use hard lambdas with zero interpolation. `cycle_time` of `1.00h` = excellent, `1.01h` = good. A one-minute difference changes the rating. Same pattern across all 27 metrics.

**Fix**: Consider using continuous scoring (0-100) with interpolation bands instead of hard cutoffs.

---

### 17. `bug_fix_focus_rate` Double-Counts PRs and Commits

In `bug_fix_focus_rate.py:37`: `total_bug = len(bug_prs) + len(bug_commits)`. A PR titled "fix: crash" with 3 commits each containing "fix" counts as 4 bug items out of 4 total = 100%, when it's conceptually one fix.

**Fix**: Deduplicate by associating commits to their parent PR and counting each fix once.

---

### 18. `revert_introduction_rate` — Penalizes the Wrong Person

In `revert_introduction_rate.py:29`: the revert is counted against whoever performed the revert, not whose code was reverted. An engineer cleaning up someone else's mess gets penalized.

**Fix**: Attribute the revert to the author of the original reverted commit, not the revert performer.

---

### 19. `review_leverage` — Effectiveness Scale Inconsistency

In `review_leverage.py:100`: returns 0-100 percentage, while all other influence metrics (`approval_to_merge_ratio`, `blocking_comment_rate`, `change_inducing_review_rate`, `pr_merge_rate`) use 0.0-1.0 ratios.

**Fix**: Standardize all influence metrics to the same scale (either all 0-1 or all 0-100).

---

### 20. `review_turnaround_time` — Measures from PR Creation, Not Review Request

In `review_turnaround_time.py:48`: `delta = first_user_review.submitted_at - pr.created_at`. If a PR sat for 3 weeks before the reviewer was assigned, and they reviewed within 2 hours, the metric reports ~504 hours.

**Fix**: Measure from when the reviewer was requested (if available), not from PR creation.

---

### 21. `time_to_first_review` vs `review_turnaround_time` — 12x Threshold Asymmetry

Authors are judged with excellent at `<= 1h` (`thresholds.py:65`) for receiving reviews, while reviewers get excellent at `<= 12h` (`thresholds.py:127`) for giving reviews. This 12x difference creates an asymmetric standard.

**Fix**: Align the thresholds or clearly document the rationale for the asymmetry.

---

### 22. `pr_body_quality` — Double-Awards Issue References

In `utils.py:366-371`: a PR body with `"Fixes #1234"` gets +15 (issue ref match) AND +10 (`#\d+` fallback) = 25 points from a single reference (25% of max score).

**Fix**: Use `elif` to prevent double-scoring issue references.

---

### 23. `pr_size_distribution` — Trivial PRs Double-Counted in Small

In `pr_size_distribution.py:77-84`: the `if` (not `elif`) structure means trivial PRs are added to both `trivial_prs` AND `small_prs`.

**Fix**: Use `elif` to make the size categories mutually exclusive.

---

### 24. Inconsistent Period Defaults When Dates Are `None`

| Metric | Fallback |
|--------|----------|
| `trivial_contribution_rate`, `co_author_contribution_rate`, `dependency_change_rate` | **30 days** |
| `reviews_given`, `blocking_comment_rate`, `review_turnaround_time` | **10 days** |

The 3x difference means identical raw counts produce dramatically different per-day rates.

**Fix**: Standardize all fallback periods to the same default.

---

### 25. `commit_message_clarity` — Name Is Misleading

The metric exclusively measures conventional commit format adherence (`type(scope): desc`). A perfectly clear message like "Refactored auth middleware to use JWT tokens" scores 0%. The metric name "Clarity" misrepresents what it measures.

**Fix**: Rename to `conventional_commit_rate` or `commit_format_adherence`.

---

### 26. No Self-Review Filtering in Influence Metrics

None of the influence metrics filter out reviews on the user's own PRs. A self-review inflates `pr_merge_rate` (reviewing and merging your own PR) and deflates `review_turnaround_time` (instant self-review lowers the median).

**Fix**: Filter out reviews where `pr.user.login == context.user_login` in all influence metrics.

---

### 27. `is_test_file` and `is_revert_indicator` — Substring False Positives

- `is_test_file`: `"test_"` matches `latest_testimonials.py`; `"jest"` matches `majesty.ts`
- `is_revert_indicator`: `"revert "` matches `"This will revert the approach"`

**Fix**: Use word-boundary regex or path-segment matching.

---

### 28. `active_weeks` — Can Count Weeks Outside Analysis Period

`pr.merged_at` dates added in `active_weeks.py:37-38` can fall outside the `start_date`/`end_date` window, potentially producing `active_ratio > 1.0`.

**Fix**: Clamp all date sources to the analysis window before computing week numbers.
