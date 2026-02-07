# Impact Module Agents

This document details the agents (components) within the `impact/` module, focusing on the metrics pipeline for assessing engineering quality and impact.

## Pipeline Overview

The metrics pipeline processes GitHub data through ingestion, indexing, metric computation, and reporting. It's designed for deterministic, query-oriented analysis of developer activity.

### 1. Ingestion Agent (`ingestion/`)

**Purpose**: Load and validate raw GitHub data into a standardized canonical format.

- **DumpIngestion**: Reads filesystem dumps (JSONL files) and delegates to adapters.
- **Adapters** (`adapters/`):
  - **GitHubAdapter**: Parses GitHub-specific JSONL (PRs, reviews, commits, etc.) into CanonicalBundle.
  - Filters data to user actions within time windows.
  - Ensures data integrity and user-centric focus.

**Key Behaviors**:
- Validates manifest (user, dates, provider).
- Builds user caches to avoid duplication.
- Time-filters all records.
- Trims unrelated PRs (only those where user authored or acted).

### 2. Ledger Agent (`ledger/`)

**Purpose**: Provide efficient, read-only querying of canonical data.

- **Ledger**: In-memory store with pre-built indexes.
- **Indexing Strategy**:
  - User-centric: PRs, commits, reviews by user.
  - PR-centric: Reviews, comments, commits, files per PR.
  - Time-ordered: All lists sorted by creation/submission dates.

**Query Methods**:
- `get_prs_for_user()`: User's PRs in window.
- `get_merged_prs_for_user()`: Filtered to merged.
- `get_reviews_for_pr()`: PR's reviews.
- `get_commits_for_user()`: User's commits.
- Supports optional start/end date filtering.

**Performance**: Indexes enable O(1) lookup + O(log n) binary search for date ranges.

### 3. Metrics Agent (`metrics/`)

**Purpose**: Compute quantitative metrics from ledger data.

- **Base Metric Class**: Abstract interface with `slug`, `name`, `run()`.
- **Metric Plugins** (`plugins/`): Organized in `authored/` (own contributions) and `influence/` (impact on others); cleaned of duplicates.
  - **PRThroughput**: Opened/merged counts and ratio (handles backlog merges).
  - **CycleTime**: Median time from PR creation to merge.
  - **PRMergeEffectiveness**: Back-and-forth review rounds.
  - **ReviewLeverage**: Review impact (approvals, rejections; improved attribution).
  - **ReviewIterations**: Average review rounds per PR.
  - **TimeToFirstReview**: Median time to initial review.
  - **SlowReviewResponse**: Median response to review comments.
  - **ModuleAreaBreadth**: Avg unique areas per PR (true per-PR calc).
  - **UnblockTime**: Re-review speed after CR (excludes author lag).

- **Context**: `MetricContext` bundles ledger, user, dates for each run.
- **Results**: `MetricResult` with summary, details dict.

**Computation Patterns**:
- Aggregate over user's PRs/reviews/commits.
- Use percentiles for robust statistics (median, p75).
- Per-entity breakdowns for transparency.

### 4. Reporting Agent (`scripts/`)

**Purpose**: Orchestrate pipeline and generate human-readable reports.

- **GenerateReport Script**: CLI tool for end-to-end execution.
- **Pipeline Steps**:
  1. Optional async fetch via Celery.
  2. Ingest dump to bundle.
  3. Create ledger.
  4. Run selected metrics.
  5. Apply rating thresholds.
  6. Format and print report.

**Rating System**: Qualitative assessment (excellent/good/neutral/bad) based on configurable thresholds.

## Data Model (`domain/`)

- **CanonicalBundle**: Container for all entities (users, repos, PRs, etc.).
- **Models**: Pydantic classes for type safety and validation.
- **Enums**: Standardized states (e.g., ReviewState.APPROVED).

## Utilities (`utils.py`)

Helper functions for metrics:
- `percentile()`: Robust percentile calculation.
- `calculate_merge_time_hours()`: Duration from PR creation to merge.

## Exceptions (`exceptions.py`)

Custom exceptions for data validation, parsing, and manifest errors.

## Tasks (`tasks/`)

Celery task definitions for async operations (e.g., fetching).

## Testing (`tests/`)

Unit tests for each agent, ensuring correctness of ingestion, indexing, and metric calculations.

## Best Practices in Pipeline

- **Immutability**: Ledger is read-only; no modifications post-build.
- **Filtering**: All queries respect user and date windows.
- **Efficiency**: In-memory processing; avoid redundant computations.
- **Extensibility**: New metrics via plugin pattern; new providers via adapter pattern.
- **Reliability**: Comprehensive error handling and logging.
