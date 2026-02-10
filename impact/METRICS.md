Here's my analysis based on the full exploration of your 27 existing metrics (19 authored, 8 influence). There are definitely 10+ additional metrics with clear separation. Here's what I'd propose:

Proposed New Metrics
Authored
1. Code Churn Rate — % of lines in an engineer's PRs that modify code they themselves wrote within the last 30 days. Measures self-rework and instability of one's own output.

Separation: revert_introduction_rate only tracks explicit reverts; pr_size_distribution categorizes size. Neither measures rework of recently written code.
Data: Git blame + PR diffs to identify self-authored line overwrites.
2. Self-Merge Rate — % of PRs merged by the author without any approval. Measures process discipline and review-culture adherence.

Separation: No existing metric tracks whether PRs bypass review entirely. pr_merge_effectiveness measures merge smoothness with reviews, not absence of reviews.
Data: PR reviews array — check for merged PRs with zero APPROVED reviews.
3. Stale PR Rate — % of opened PRs still open beyond a threshold (e.g., 14 days). Measures abandoned or blocked work.

Separation: cycle_time only measures merged PRs. This captures the unmerged/abandoned tail.
Data: PR created_at vs current time for state=open PRs.
4. Documentation Touch Rate — % of PRs that include changes to documentation files (.md, docs/, README, wiki pages, etc.).

Separation: commit_message_clarity measures commit message format; pr_body_quality_score measures PR description quality. Neither tracks actual documentation in the codebase.
Data: PR file lists filtered by doc-file patterns.
5. Net Code Contribution — Net lines added minus deleted over the period, plus ratio of additions to deletions. Reveals whether an engineer is a net creator, maintainer, or simplifier/refactorer.

Separation: pr_size_distribution categorizes individual PR sizes. pr_throughput counts PR volume. Neither captures the directional nature of code contribution.
Data: PR additions and deletions fields, aggregated.
6. Follow-Up Commit Rate — % of PRs where the author pushes additional commits after the initial push (self-initiated updates before or after review).

Separation: review_iterations counts change-request-driven cycles. This captures all follow-up pushes, including self-initiated refinements before any review, indicating iteration habits.
Data: PR commit timestamps — count PRs with commits pushed after initial creation.
7. Off-Hours Activity Rate — % of commits/PR events occurring outside standard working hours (weekends, late night). A sustainability and burnout-risk signal.

Separation: burstiness measures weekly volume variance; active_weeks measures engagement. Neither considers when during the day/week work happens.
Data: Commit/PR timestamps with hour-of-day and day-of-week analysis.
8. PR Category Diversity — Distribution across PR types (feature, fix, refactor, docs, chore, test, etc.) based on conventional commit prefixes or labels.

Separation: bug_fix_focus_rate only tracks the bug-fix slice. This captures the full distribution, showing whether an engineer's work is well-rounded or narrowly focused.
Data: PR titles/commit messages parsed for conventional prefixes, or GitHub labels.
Influence
9. Review Breadth (Unique Authors) — Number of distinct PR authors whose PRs this engineer reviews. Measures cross-team reach and mentorship surface area.

Separation: reviews_given counts total review volume; review_turnaround_time measures speed. Neither captures how widely an engineer distributes review attention across the team.
Data: Deduplicate PR authors from reviewed PRs.
10. Review Comment Substance Score — Average length and structural quality of review comments (code suggestions, questions, links, etc. vs. single-word approvals).

Separation: inline_comment_density counts positioned comments on own PRs; blocking_comment_rate measures blocking CRs (binary). Neither assesses the substance/depth of the feedback text itself.
Data: Review comment bodies — length, presence of code blocks, questions, and references.
11. Mentorship Signal — Rate of reviewing PRs from low-activity or newer contributors (those with fewer than N PRs in the period). Measures investment in growing the team.

Separation: review_breadth measures diversity of reviewees but doesn't weight by experience level. This specifically targets mentorship behavior.
Data: Cross-reference PR author activity counts with review targets.
12. Contested Review Rate — % of an engineer's reviews where their feedback is not followed (no subsequent commit, or another reviewer overrides). Measures alignment with team norms.

Separation: change_inducing_review_rate measures reviews that are followed. approval_to_merge_ratio measures approvals leading to merge. This captures the inverse — ignored or overridden feedback.
Data: Reviews not followed by author commits within a window, or merged despite changes-requested without re-review.
Summary
#	Metric	Category	Distinct From
1	Code Churn Rate	Authored	revert_introduction_rate, pr_size_distribution
2	Self-Merge Rate	Authored	pr_merge_effectiveness
3	Stale PR Rate	Authored	cycle_time
4	Documentation Touch Rate	Authored	commit_message_clarity, pr_body_quality
5	Net Code Contribution	Authored	pr_size_distribution, pr_throughput
6	Follow-Up Commit Rate	Authored	review_iterations
7	Off-Hours Activity Rate	Authored	burstiness, active_weeks
8	PR Category Diversity	Authored	bug_fix_focus_rate
9	Review Breadth	Influence	reviews_given
10	Review Comment Substance	Influence	inline_comment_density, blocking_comment_rate
11	Mentorship Signal	Influence	review_breadth, reviews_given
12	Contested Review Rate	Influence	change_inducing_review_rate, approval_to_merge_ratio
All 12 are feasible with the GitHub data your fetcher already pulls (PRs, commits, reviews, review comments, timelines). Each measures a clearly distinct dimension that no current metric covers. The strongest candidates for immediate impact would be Self-Merge Rate (process risk), Stale PR Rate (hidden WIP), Review Breadth (team health), and Code Churn Rate (code quality signal).