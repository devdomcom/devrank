# DevRank Metrics Proposal

This document proposes new metrics for the impact assessment pipeline, based on analysis of the sample GitHub data in `impact/samples/github_live_dump/`. The sample contains canonical JSONL files for commits, pull_requests, reviews, review_comments, issue_comments, files, timeline, etc., from the apache/superset repository. These provide rich data on PRs (titles, bodies, labels, stats like additions/deletions/changed_files, reviewers, assignees), commits (messages, authors, linked PRs), reviews (states like APPROVED/COMMENTED, bodies), comments (inline and general, from humans/bots, reactions), files (paths, patches, status), and timelines (events).

## Existing Metrics Summary
Authored metrics (from `impact/metrics/plugins/authored/`): ActiveWeeks, Burstiness, CycleTime, ModuleAreaBreadth, PRMergeEffectiveness, PRSizeDistribution, PRThroughput, ReviewIterations, SlowReviewResponse, TimeToFirstReview, TrivialContributionRate.

Influence metrics (from `impact/metrics/plugins/influence/`): ApprovalToMergeRatio, BlockingCommentRate, ChangeInducingReviewRate, PRMergeRate, ReviewLeverage, ReviewTurnaroundTime, ReviewsGiven, UnblockTime.

These cover productivity (throughput/size/trivial/active/burst), cycle/merge times, review quality/iterations/response, area breadth, and influence (reviews given/turnaround/unblock/leverage/rates for CRs/approvals/merges). Proposals below focus strictly on uncovered areas with high value, based on parsed impls and sample data (PR bodies/labels/files/commits/messages, review/comments/bot types, timelines). Drawn from DORA/SPACE/Google benchmarks. 12 unique new metrics.

## Proposed Metrics

### Bug Fix Focus Rate [DONE]
**Short description**: Percentage of PRs/commits focused on bug fixes.
**Long description**: Scan PR titles/bodies and commit messages (from pull_requests.jsonl/commits.jsonl) for patterns like "fix:", "bug:", "closes issue". Rate = (bug-related / total authored) * 100. Uses ledger PR/commit queries. Measures maintenance impact; industry: balanced fix/feature ratio signals reliability.

### Revert Introduction Rate [DONE]
**Short description**: Rate of reverts in authored commits/PRs.
**Long description**: Detect "Revert" in commit messages or matching original SHA patterns in commits.jsonl linked to PRs (via pull_request_number). Rate = (revert commits / total commits) * 100 for user. From commits data only. Indicates instability avoidance; best practice <5% for high-impact work.

### Test File Ratio [DONE]
**Short description**: Proportion of changes to test vs. non-test files (distinct from area breadth).
**Long description**: From files.jsonl in PRs, classify paths (e.g., /test/ or *.test.* vs. source); ratio = test changes / total changes per author PRs. Aggregate via get_files_for_pr. Promotes test discipline; real-world target 25%+ per quality benchmarks.

### PR Body Quality Score
**Short description**: Average score for PR description structure/completeness.
**Long description**: Parse PR body (pull_requests.jsonl) for sections (SUMMARY, TESTING, BEFORE/AFTER, ADDITIONAL via keyword regex). Score 0-100 (e.g., +25 per key section). Avg per authored PR. Encourages docs; from sample template patterns.

### Co-Author Contribution Rate
**Short description**: Percentage of PRs with multiple commit authors.
**Long description**: For authored PRs, check commits.jsonl for >1 distinct author.login (beyond primary). Rate = (co-author PRs / total PRs) * 100. Uses get_commits_for_pr. Measures collab; industry value in knowledge sharing.

### Bot Interaction Ratio
**Short description**: Ratio of bot vs. human reviews/comments on PRs.
**Short description**: Classify reviewers/comments (reviews.jsonl/review_comments.jsonl) by user.type=="Bot" or [bot] suffix; ratio for user's PRs. Distinct from human review metrics. Tracks automation reliance; best practice <50% for human oversight.

### Cross-Team Review Rate
**Short description**: Percentage of reviews given to non-team/PR-external contributors.
**Long description**: For reviews_given, compare reviewer org vs. PR author (from user/org fields in data). Rate via ledger get_reviews_for_user + PR lookup. Measures influence breadth; per SPACE framework.

### Hotfix Frequency
**Short description**: Rate of emergency/hotfix contributions.
**Long description**: Flag PRs/commits with "hotfix", "urgent", or post-merge labels in titles/bodies (pull_requests.jsonl/commits.jsonl). Freq normalized by period. From sample labels/events. Signals responsiveness; low ideal for proactive work.

### Dependency Change Rate
**Short description**: Frequency of dependency file modifications.
**Long description**: Count changes to package.json/requirements.txt/etc. paths in files.jsonl for authored PRs (via get_files_for_pr). Rate per PR or time. Distinct from general breadth; measures upkeep per open-source practices.

### Inline Comment Density
**Short description**: Average inline comments per reviewed PR/file.
**Long description**: From review_comments.jsonl, count per review/PR (path-specific), avg for influence/authored. Uses get_review_comments_for_review. Measures depth without duplicating iterations; industry 2-4 ideal.

### Label Diversity Score
**Short description**: Variety of labels used on authored PRs.
**Long description**: From pull_requests.jsonl labels, count unique types (size/*, plugins, etc. from sample) per user PRs. Score = unique / total possible. Promotes categorization; aids triage.

### Commit Message Clarity
**Short description**: Adherence to conventional commit formats in messages.
**Long description**: Parse commits.jsonl messages for prefix patterns (fix/feat/refactor/:). % compliant per author. From sample conventional commits. Improves maintainability; best practice in CI/CD teams.

## Implementation Notes
Add as plugins in authored/influence/ per base.py patterns (use ledger.get_*). All leverage existing data/models without overlap.
