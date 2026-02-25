# DevRank Agents Overview

This document outlines the high-level architecture and agents (components) of the DevRank impact assessment system.

## System Architecture

DevRank is built as a modular Python application that fetches, processes, and analyzes GitHub data to compute engineering impact metrics. The system is designed for scalability, with asynchronous fetching via Celery and in-memory data processing.

### Core Components

1. **Data Fetching Agent** (`impact/providers/`)
   - **GitHub Live Fetcher**: Asynchronously fetches data from GitHub API using Celery tasks.
   - **Configuration**: Supports token-based authentication, repository filtering, and date windows.
   - **Rate Limiting**: HTTP 429/403 handling with exponential backoff (8 retries, max 60s), proactive `_check_rate_limit()` when remaining < 50, 100ms blob throttle.

2. **Ingestion Agent** (`impact/ingestion/`)
   - **Dump Ingestion**: Parses pre-fetched JSONL dumps into canonical data structures.
   - **Adapters** (`impact/adapters/`): Provider-specific parsers (currently GitHub) that validate and transform raw data.

3. **Data Ledger Agent** (`impact/ledger/`)
   - **Ledger**: In-memory indexed store for efficient querying of canonical data.
   - **Indexing**: Builds time-ordered indexes for users, PRs, reviews, commits, etc.

4. **Metrics Computation Agent** (`impact/metrics/`)
   - **45 metric plugins** in `plugins/authored/` (29), `plugins/influence/` (15), and `plugins/mixed/` (1).
   - **Base Framework**: Abstract metric class with `slug`, `name`, `category`, `run()`.
   - **Static Analysis** (`utils.py`): Three-phase deterministic analysis:
     - Phase 1: Generated-file detection, hunk context parsing, patch-content test detection, structural diff classification.
     - Phase 2: Pygments-based review comment code scoring, manifest-aware module detection.
     - Phase 3: tree-sitter function identity + trivial detection (Python, JS/TS, Go, Rust, Java).
   - **Thresholds** (`impact/thresholds.py`): Discrete lambdas for ratings (excellent/good/neutral/bad) + continuous `score_metric()` for 0-100 scoring.

5. **Configuration Agent** (`impact/config/`)
   - **Categories** (`categories.py`): Central `Category(slug, name)` registry. Every metric plugin declares a `category` property validated at startup via `validate_metrics()`.
   - **Role Configs** (`role_metrics.py` + `roles/*.yaml`): Per-role metric weighting and threshold overrides.

6. **Report Generation Agent** (`impact/scripts/` + `impact/templates/`)
   - **Report Generator** (`generate_report.py`): Orchestrates pipeline from ingestion to metric computation, rating, and output.
   - **PDF Template** (`pdf_report.py`): ReportLab-based PDF report with category grouping, top/low metrics, executive summary.

7. **Database & Persistence Agent** (`db/`)
   - **Base** (`db/base.py`): SQLAlchemy `DeclarativeBase` — all ORM models inherit from this.
   - **Engine** (`db/engine.py`): Dual-driver setup — async (asyncpg for FastAPI) + sync (psycopg2 for Alembic/Celery).
   - **Models** (`db/models/`): 17 ORM models across dedicated files. See `ARCHITECTURE.md` for how they relate.
   - **Migrations** (`db/migrations/`): Alembic migration pipeline. Config in `alembic.ini`; env.py reads `settings.database_url_sync` from refactored `config.py` (now Pydantic Settings for validation/DRY).
   - **Filesystem** (`impact/persistence/filesystem.py`): `FileSystemDumpWriter` for JSONL dump output.
   - **DinD Compatible**: `scripts/dev-infra.sh` auto-detects Docker-in-Docker, substitutes `postgres`/`redis` hostnames. Alembic works both from host and inside worker container.

### Data Flow

1. **Fetch**: GitHub Live Fetcher downloads data to JSONL dump.
2. **Ingest**: Dump Ingestion + Adapter creates CanonicalBundle.
3. **Index**: Ledger builds query indexes.
4. **Compute**: Metrics run against the ledger.
5. **Report**: Generate formatted output with ratings.

### Asynchronous Processing

- Celery is used for long-running fetch operations.
- Redis serves as the message broker and result backend.

### Extensibility

- New providers: Add adapters in `impact/adapters/`.
- New metrics: Implement Metric subclasses in `impact/metrics/plugins/` (use `authored/`, `influence/`, or `mixed/` subdirs). Each metric must declare a `category` property matching a slug in `impact/config/categories.py`.
- Custom ingestion: Extend `Ingestion` base class.

### Best Practices

- **Modularity**: Each agent has a single responsibility.
- **Testing**: 452 tests across `impact/tests/` (446) and `db/tests/` (6).
- **Logging**: Structured logging throughout.
- **Error Handling**: Graceful failure with informative messages.
- **Performance**: In-memory processing for speed; async fetching for scale.
- **Keep `ARCHITECTURE.md` in sync**: When adding, removing, or changing ORM models, relationships, or domain constraints in `db/models/`, update `ARCHITECTURE.md` to reflect the change. It is the plain-English source of truth for how entities relate.

## General Best Practices

### FastAPI & API Design

- **Sync vs async endpoints**: Use plain `def` (not `async def`) for endpoints that call blocking/synchronous code (disk I/O, CPU-bound computation). FastAPI automatically runs sync endpoints in a thread pool. Using `async def` with blocking code freezes the entire event loop for all concurrent requests.
- **Dependency injection — use it for real**: When using FastAPI's `Depends()`, ensure the chain is actually wired through `Depends()`, not plain function calls. Calling a dependency function directly bypasses request-scoped caching, `dependency_overrides` for testing, and lifecycle management.
- **One body, one consumer**: Never declare both a `Depends()` dependency that consumes the request body AND a separate body parameter on the same endpoint. FastAPI will attempt to parse the body twice, causing nesting or parsing errors.
- **Pydantic models for all responses**: Use typed Pydantic response models (with `response_model=`) on every endpoint. Returning raw `dict` or `list[dict]` loses OpenAPI schema generation and output validation.
- **Cache static metadata**: If endpoint data is derived from static registries (e.g., metric slugs/names/descriptions), compute it once and cache it rather than re-instantiating on every request.
- **CORS middleware**: Always configure `CORSMiddleware` if the API may be called from a browser. Use env-var-driven origin lists so production can restrict while development stays permissive.

### Security

- **Validate user-supplied file paths**: Any API parameter that resolves to a filesystem path must be validated against an allowlist of base directories. Accepting arbitrary paths enables path traversal attacks.
- **Gate debug/test endpoints**: Never expose debug or error-triggering endpoints in production. Use environment variable flags or separate router registration so they are only available during development/testing.
- **Don't leak internal details in errors**: Error responses should be informative but should not expose stack traces, internal paths, or implementation details to callers.

### Exception Handling

- **Register specific handlers before base handlers**: When using exception hierarchies, register handlers for specific subclasses before the base class. Starlette matches by exact type first; if the base handler is registered first, subclass exceptions may be caught by the wrong handler.
- **Document resolution order**: Add comments or docstrings to handler registration code explaining why the order matters, so future maintainers don't accidentally reorder.

### Testing

- **Avoid module-level conditional registration in tests**: If production code uses env-var gating for route registration, tests that depend on those routes should create a standalone app with the routes registered directly, rather than relying on module-level `os.environ` (which is fragile due to Python's module caching).
- **Use dedicated test apps for handler testing**: Create minimal FastAPI apps in test fixtures rather than testing against the production app instance. This isolates handler behavior from routing and middleware concerns.
- **Update tests when behavior changes**: When a metric or endpoint's semantics change, update the corresponding tests immediately. Stale tests that assert old behavior mask real regressions.

### Package Organization

- **Single source of truth for API code**: Keep all API-related code (app factory, routes, dependencies, schemas, handlers) in one package. If backwards-compatible import paths are needed, use thin re-export shims rather than maintaining duplicate implementations.
- **Separate utilities from dependencies**: FastAPI dependencies (functions used with `Depends()`) should only contain request-extraction logic. Shared business logic (loading data, building contexts) should be plain utility functions that dependencies call.

### Metrics Design

- **Guard zero-activity from positive ratings**: Metrics with no data must set `details["no_data"] = True` to prevent rating zero activity as "excellent." Every metric should handle the empty-input case explicitly.
- **Combined period+count guards**: Use `if period_days < MIN and count < MIN: details["no_data"] = True`. This preserves genuine low-activity signals in long periods while suppressing noise in short periods. Applied to 9 metrics.
- **Wire real rating logic end-to-end**: Never leave placeholder ratings (e.g., hardcoded "neutral") in API responses. If the rating system exists, use it in every endpoint that returns ratings.
- **Threshold keys must match metric detail keys**: When renaming metrics or their output keys, update the corresponding threshold configuration to match. A mismatch silently produces "unknown" ratings.
- **Influence metrics must filter self-reviews**: Always check `pr.user.login != context.user_login` to avoid crediting a developer for reviewing their own PRs.
- **Period fallback**: Standardized to 30 days across all metrics when `start_date`/`end_date` are not available.
