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

- **Base Metric Class**: Abstract interface with `slug`, `name`, `category`, `run()`.
- **45 Metric Plugins** (`plugins/`): Organized in `authored/` (29 — own contributions), `influence/` (15 — impact on others), and `mixed/` (1 — collaborative).
- **Context**: `MetricContext` bundles ledger, user, dates for each run.
- **Results**: `MetricResult(metric_slug, summary, details)` with details dict.
- **Categories**: Every metric declares a `category` property validated against `impact/config/categories.py` at startup.

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

## Configuration (`config/`)

- **Categories** (`categories.py`): Central `Category(slug, name)` dataclass registry. `validate_metrics()` ensures every plugin has a valid category slug.
- **Role Configs** (`role_metrics.py` + `roles/*.yaml`): Per-role metric weighting and threshold overrides. `get_role_config(role_slug)` returns the active config.

## Thresholds (`thresholds.py`)

- **Discrete ratings**: Lambda functions mapping metric values → excellent/good/neutral/bad.
- **Continuous scoring**: `score_metric(slug, value)` → 0-100 via piecewise linear interpolation.
- **`get_continuous_score(slug, details, role_config)`**: Main entry point used by report generator and API.

## Utilities (`utils.py`)

Core helper functions and static analysis pipeline:

**Basic helpers**: `percentile()`, `calculate_merge_time_hours()`, `get_pr_size_category()`.

**Static Analysis (3 phases, deterministic)**:
- Phase 1: `is_generated_file()` (layered detection: basename, suffix, directory, markers, entropy), `_parse_hunk_lines()` (unified diff → line sets, handles `\ No newline` marker), `classify_diff_structure()` (test_code, new_class, new_function, conditional, import, error_handling).
- Phase 2: `score_comment_code_quality()` (Pygments-based code block scoring 0-25), `detect_module_boundary()` (manifest-aware, root-level → `"root"`).
- Phase 3: `parse_functions()` / `compute_trivial_ratio()` / `analyze_file_complexity()` (tree-sitter AST for Python, JS/TS, Go, Rust, Java). `_is_trivial_body()` handles expression-bodied arrow functions.

**Code churn & rework**: `compute_code_churn()` (line-level overlap detection, generated-file filtering, context-date period), `compute_rework_rate()` (self-rework within 21d window).

## Exceptions (`exceptions.py`)

Custom exceptions for data validation, parsing, and manifest errors.

## Tasks (`tasks/`)

Celery task definitions for async operations (e.g., fetching).

## Templates (`templates/`)

- **PDF Report** (`pdf_report.py`): ReportLab-based PDF generation with category grouping, top/low metrics, executive summary. ~1200 lines.

## Testing (`tests/`)

446 unit tests covering ingestion, indexing, metric calculations, API endpoints, static analysis, and categories. Run with: `uv run python -m pytest impact/tests/ -q`. Additional 6 migration tests in `db/tests/`.

## Best Practices in Pipeline

- **Immutability**: Ledger is read-only; no modifications post-build.
- **Filtering**: All queries respect user and date windows.
- **Efficiency**: In-memory processing; avoid redundant computations.
- **Extensibility**: New metrics via plugin pattern; new providers via adapter pattern.
- **Reliability**: Comprehensive error handling and logging.

## API Layer Best Practices

### FastAPI Endpoints

- **Use sync `def` for blocking work**: Endpoints that perform disk I/O, CPU-bound computation, or call synchronous libraries must use plain `def`, not `async def`. FastAPI runs sync endpoints in a thread pool automatically; `async def` with blocking code freezes the event loop.
- **Real dependency injection**: Wire dependencies through `Depends()` for request-scoped caching and testability via `dependency_overrides`. Calling dependency functions directly as plain functions defeats the purpose.
- **Single body consumer per endpoint**: Do not declare both a `Depends()` that consumes the body and a separate `Body()` parameter. The body gets parsed twice, causing errors or unexpected nesting.
- **Typed response models everywhere**: Use Pydantic `response_model=` on every endpoint. Raw dicts lose OpenAPI docs and output validation.
- **Cache static registries**: Metric metadata (slugs, names, descriptions) is static — compute once and cache rather than instantiating on every request.

### Security

- **Validate file paths from user input**: Any parameter that resolves to a filesystem path must be checked against an allowlist of base directories. Accepting arbitrary paths enables traversal attacks. Use env vars to configure allowed bases per environment.
- **Gate debug endpoints behind env flags**: Test or error-triggering endpoints must never be accessible in production. Use environment variables to control registration at module load time.
- **Friendly error messages**: Parse user-supplied values (dates, paths) with try/except and return clear, actionable error messages instead of raw Python exceptions.

### Exception Handling

- **Register specific handlers before base handlers**: With exception hierarchies, register subclass handlers first. Starlette matches by exact type; if the base handler is registered first, subclass exceptions may fall through.
- **Document handler ordering**: Add comments to registration code explaining the required order.

### Testing

- **Create standalone test apps for handler testing**: Don't rely on the production app for testing error handlers. Create minimal FastAPI apps in test fixtures with routes registered directly — this avoids module-caching issues with conditional route registration.
- **Update tests when behavior changes**: When metrics or endpoints change semantics, update corresponding tests immediately.

### Package Organization

- **Single package for all API code**: Keep app factory, routes, dependencies, schemas, and handlers in one package. Use thin re-export shims for backwards-compatible import paths if needed.
- **Separate utilities from dependencies**: Dependency functions should only extract data from the request. Shared business logic belongs in plain utility functions that dependencies call.

### Metrics

- **Guard zero-activity cases**: Every metric must handle empty input and set `details["no_data"] = True` to prevent rating zero activity as "excellent."
- **Combined period+count guards**: Use `if period_days < MIN and count < MIN: details["no_data"] = True`. Preserves genuine low-activity signals in long periods; suppresses noise in short periods.
- **Wire real ratings end-to-end**: Never leave placeholder ratings in API responses. If the rating system exists, use it everywhere.
- **Keep threshold keys in sync**: When renaming metrics or output keys, update threshold configuration to match. Mismatches silently produce "unknown" ratings.
- **Filter self-reviews in influence metrics**: Always check `pr.user.login != context.user_login`.
- **Period fallback**: Standardized to 30 days across all metrics when dates are not available.
- **Category required**: Every new metric must declare a `category` property matching a slug in `impact/config/categories.py`.

### Database & Migrations

- **Alembic for all schema changes**: Never modify the database schema by hand. Always create migrations via `alembic revision --autogenerate -m "description"`.
- **Dual-engine pattern**: Async engine (asyncpg) for FastAPI endpoints; sync engine (psycopg2) for Alembic and Celery workers. Both configured in `db/engine.py`.
- **URL from config, not alembic.ini**: `db/migrations/env.py` reads `DATABASE_URL_SYNC` from `config.py`, which supports env-var overrides for DinD/production.
- **Import all models in env.py**: New model files must be imported in `db/migrations/env.py` so autogenerate can detect them.
