# DevRank

DevRank is a Scenario-Driven Evaluation Framework for Engineering Excellence and Impact that vets candidates using real-world metrics.

## Overview

The impact assessment tool analyzes GitHub activity to compute metrics that quantify a developer's productivity, code quality, and collaboration effectiveness. It supports fetching fresh data from GitHub or using existing data dumps, and exposes a REST API for programmatic access.

## Features

- **45 Metrics**: 32 authored-work metrics + 13 influence/review metrics covering productivity, quality, collaboration, and sustainability.
- **Configurable Thresholds**: Rating thresholds in `impact/thresholds.py` with continuous 0-100 scoring via piecewise interpolation.
- **Role Configs**: YAML-based role profiles (`impact/config/roles/`) with per-metric rating overrides and restrictions.
- **Data Ingestion**: Parse GitHub data from JSONL dumps (canonical format).
- **Report Generation**: CLI reports with ratings, scores, and PDF export.
- **REST API**: FastAPI layer with Swagger UI, metrics computation, period comparison, role management, and dump upload.

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- GitHub token (for live fetching)

## Quick Start

```bash
git clone <repo-url>
cd devdom_eng_metrics
uv sync
bash scripts/dev-infra.sh start
uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

No `.env` file needed — `config.py` ships sensible localhost defaults for every setting. The app works out of the box.

## Setup

### 1. Install Dependencies

```bash
uv sync
```

### 2. Start Infrastructure (PostgreSQL + Redis)

```bash
bash scripts/dev-infra.sh start
```

The script auto-detects which environment you're in:

| Environment | How services are reached | What the script does |
|---|---|---|
| **Native Docker** (macOS / Linux host) | `localhost:5432` / `localhost:6379` | Starts containers, done |
| **Docker-in-Docker** (Dev Containers, Codespaces, Gitpod) | `postgres:5432` / `redis:6379` | Starts containers, connects your dev container to the `devrank` Docker network so service names resolve |

DinD is detected via `/.dockerenv`, cgroup markers, `/proc/self/mountinfo`, or environment variables set by Codespaces / Gitpod / VS Code Dev Containers.

On first run the script generates a `.env` from `.env.example` with the correct hostnames substituted. This is optional — `config.py` already has working defaults for native Docker. The `.env` is mainly useful in DinD where hostnames differ.

Other commands:

```bash
bash scripts/dev-infra.sh stop    # stop containers (keep data)
bash scripts/dev-infra.sh reset   # stop + delete volumes (fresh start)
bash scripts/dev-infra.sh status  # show container status
```

### 3. Start the API Server

```bash
uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

`--host 0.0.0.0` binds to all interfaces. This is required in DinD environments so the host machine can reach the server via port forwarding. In native setups it works identically to `localhost`.

### 4. Verify

**Liveness probe** (instant, no external dependencies):

```bash
curl http://localhost:8000/health
# {"status":"healthy","service":"devrank"}
```

**Infrastructure readiness** (checks Postgres + Redis connectivity, version, latency):

```bash
curl http://localhost:8000/health/infra
# {"status":"healthy","postgres":{"status":"healthy","latency_ms":2.1,"version":"PostgreSQL 16..."},"redis":{"status":"healthy","latency_ms":0.5,"version":"7.2.4"}}
```

If you're in a DinD environment and `localhost` doesn't reach the server, use the container's hostname or IP instead, or verify port forwarding is enabled in your IDE.

**Swagger UI**: http://localhost:8000/docs
**ReDoc**: http://localhost:8000/redoc

## Configuration

Configuration is now managed via Pydantic Settings in `config.py` (refactored for DRY, validation,
FastAPI best practices, and security warnings). See `config.py` docstring for details.

Every setting has a hardcoded default for local dev. Environment variables (prefixed
`DEVRANK_*` except legacy `GITHUB_TOKEN`) override defaults. Pydantic handles type
coercion, list parsing (CSV for CORS), and emits warnings for risky setups.

| Setting | Env var | Default |
|---|---|---|
| Async database URL | `DEVRANK_DATABASE_URL` | `postgresql+asyncpg://devrank:devrank@localhost:5432/devrank` |
| Sync database URL | `DEVRANK_DATABASE_URL_SYNC` | `postgresql://devrank:devrank@localhost:5432/devrank` |
| Redis URL | `DEVRANK_REDIS_URL` | `redis://localhost:6379/0` |
| Celery broker | `DEVRANK_CELERY_BROKER_URL` | `redis://localhost:6379/0` |
| Celery backend | `DEVRANK_CELERY_BACKEND_URL` | `redis://localhost:6379/1` |
| CORS origins | `DEVRANK_CORS_ORIGINS` | `*` |
| Allowed dump dirs | `DEVRANK_ALLOWED_DUMP_DIRS` | `/tmp`, `~/.devrank`, cwd |
| Debug mode | `DEVRANK_DEBUG` | `false` |
| Secret key | `DEVRANK_SECRET_KEY` | `local-dev-secret-change-in-production` |
| GitHub token | `DEVRANK_GITHUB_TOKEN` (or legacy `GITHUB_TOKEN`) | (none) |

To override:
- Set env vars directly
- Use `.env` file (loaded by Docker Compose/`dev-infra.sh`; Pydantic also reads `.env` directly)
- Docker Compose now uses DEVRANK_CELERY_* for consistency (see docker-compose.yml)

Warnings logged for `*` CORS or default SECRET_KEY.

## Usage

### CLI — Report from an Existing Data Dump

```bash
uv run python impact/scripts/generate_report.py \
  --existing-dump /path/to/dump/directory \
  --metrics pr_throughput cycle_time review_leverage
```

The dump directory should contain `dump_manifest.json` and a `canonical/` subdirectory with JSONL files.

### CLI — Report with Fresh GitHub Data

1. Ensure the Celery worker is running:
   ```bash
   docker compose up -d
   ```

2. Run the report:
   ```bash
   uv run python impact/scripts/generate_report.py \
     --dump-path /path/to/new/dump \
     --fetch-user <github-username> \
     --fetch-repos <owner/repo1>,<owner/repo2> \
     --fetch-token <your-github-token> \
     --metrics pr_throughput cycle_time review_leverage
   ```

   | Flag | Description |
   |---|---|
   | `--fetch-user` | GitHub username to assess |
   | `--fetch-repos` | Comma-separated repos (owner/repo) |
   | `--fetch-token` | GitHub PAT (or set `GITHUB_TOKEN` / `DEVRANK_GITHUB_TOKEN`) |
   | `--fetch-from` / `--fetch-to` | ISO date window (default: last 365 days) |
   | `--role` | Role config name (default: `default`) |
   | `--role-config` | Path to a custom role YAML |
   | `--export candidate.pdf` | Export as PDF |

### API Endpoints

**Platform (root-level)**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe (instant, no external calls) |
| GET | `/health/infra` | Readiness check (Postgres + Redis connectivity, version, latency) |

**Impact metrics (`/api/v1`)**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics/` | List all available metrics (slug, name, description) |
| GET | `/metrics/{slug}` | Compute a single metric (query params: `dump_path`, `user_login`, `start_date`, `end_date`, `role`) |
| POST | `/metrics/compute` | Compute a full metrics report from a dump |
| POST | `/metrics/compare` | Compare metrics across two time windows |
| GET | `/roles/` | List available role configs |
| GET | `/roles/{name}` | Get a specific role config |
| POST | `/dumps/upload` | Upload and validate a GitHub dump ZIP |

`user_login`, `start_date`, and `end_date` are inferred from the dump manifest when omitted.

## Available Metrics

Metrics are organized in `impact/metrics/plugins/` (`authored/` for own work, `influence/` for team impact).

### Authored Metrics (32)

Metrics computed from the engineer's own PRs, commits, and activity.

#### Productivity & Throughput

| Slug | Name | Description |
|------|------|-------------|
| `pr_throughput` | PR Throughput | PR volume (opened/merged counts) and success ratio |
| `cycle_time` | Cycle Time | Median time from PR open to merge |
| `coding_time_to_pr` | Coding Time To PR | Median hours from first commit to PR creation |
| `merge_delay` | Merge Delay | Median hours from latest approval to merge |
| `active_weeks` | Active Weeks | Active weeks, gaps, and ratio to detect disengagement |
| `coding_days` | Coding Days | % working days with at least one commit |
| `burstiness` | Burstiness | Burst ratio (max/avg activity per active week) for pacing |

#### Code Quality & Size

| Slug | Name | Description |
|------|------|-------------|
| `pr_size_distribution` | PR Size Distribution | PR size distribution and % large changes |
| `trivial_contribution_rate` | Trivial Contribution Rate | Daily rate of tiny PRs (<10 lines) |
| `code_churn_rate` | Code Churn Rate | % lines modifying own recent code (<=30d) |
| `rework_rate` | Rework Rate | % changes reworking own code from prior 21 days (DORA metric) |
| `revert_introduction_rate` | Revert Introduction Rate | % of commits that were reverted |
| `net_code_contribution` | Net Code Contribution | Net lines (add - del) + ratio over period |
| `test_file_ratio` | Test File Ratio | % test file changes (target >=25%) |

#### PR Hygiene & Process

| Slug | Name | Description |
|------|------|-------------|
| `pr_merge_effectiveness` | PR Merge Effectiveness | Merge speed combined with review interaction count |
| `first_time_approval_rate` | First-Time Approval Rate | % PRs approved on first review round |
| `review_iterations` | Review Iterations | Avg change-request cycles per merged PR |
| `time_to_first_review` | Time to First Review | Median time from PR creation to initial feedback |
| `slow_review_response` | Slow Review Response | Median author response time to changes-requested |
| `pr_body_quality_score` | PR Body Quality Score | PR body structure, length, and issue references |
| `conventional_commit_rate` | Conventional Commit Rate | % commits following conventional commit format |
| `follow_up_commit_rate` | Follow-Up Commit Rate | % PRs with additional commits after initial push |

#### Scope & Collaboration

| Slug | Name | Description |
|------|------|-------------|
| `module_area_breadth` | Module / Area Breadth | Avg distinct codebase areas touched per PR |
| `pr_category_diversity` | PR Category Diversity | Distinct PR categories (feat/fix/refactor/docs/etc.) |
| `bug_fix_focus_rate` | Bug Fix Focus Rate | % PRs/commits with bug-fix indicators |
| `dependency_change_rate` | Dependency Change Rate | Frequency of dependency file updates |
| `documentation_touch_rate` | Documentation Touch Rate | % PRs touching documentation files |
| `co_author_contribution_rate` | Co-Author Contribution Rate | Inbound/outbound co-author commit percentage |
| `inline_comment_density` | Inline Comment Density | Avg inline comments given per PR reviewed |

#### Risk & Sustainability

| Slug | Name | Description |
|------|------|-------------|
| `self_merge_rate` | Self-Merge Rate | % PRs merged by author without approval |
| `abandoned_pr_rate` | Abandoned PR Rate | % open PRs stale for >30 days |
| `off_hours_activity_rate` | Off-Hours Activity Rate | % activity on weekends/late nights (timezone-aware) |

### Influence Metrics (13)

Metrics computed from the engineer's reviews and impact on others' work.

| Slug | Name | Description |
|------|------|-------------|
| `reviews_given` | Reviews Given | Review volume (normalized by period) |
| `review_leverage` | Review Leverage | Effectiveness of change requests in driving updates |
| `review_turnaround_time` | Review Turnaround Time | Median hours to first action on opened PRs |
| `pr_merge_rate` | PR Merge Rate | % of reviews leading to merge |
| `change_inducing_review_rate` | Change-Inducing Review Rate | % reviews followed by immediate commit |
| `approval_to_merge_ratio` | Approval To Merge Ratio | % approvals that were final (no reworks, direct merge) |
| `blocking_comment_rate` | Blocking Comment Rate | % blocking change requests |
| `unblock_time` | Unblock Time | Median hours to re-review after blocking CR |
| `review_breadth` | Review Breadth | Distinct PR authors reviewed |
| `review_comment_substance` | Review Comment Substance | Avg quality score of review comments |
| `mentorship_signal` | Mentorship Signal | % reviews targeting low-activity contributors |
| `review_demand` | Review Demand | Count of review-requested events (normalized by period) |
| `first_reviewer_rate` | First Reviewer Rate | % reviews where user was the first reviewer |

## Data Format

Dumps are stored in a directory with:
- `dump_manifest.json`: Metadata including user, date range, and provider.
- `canonical/`: Subdirectory with JSONL files (pull_requests.jsonl, commits.jsonl, etc.).

### Thresholds

Metric rating thresholds are defined in `impact/thresholds.py`. Each metric has thresholds for excellent, good, neutral, and bad ratings plus continuous 0-100 scoring via piecewise interpolation. Modify the lambda functions to adjust criteria for your organization.

### Roles

Role configs live in `impact/config/roles/` as YAML files. Each role can override per-metric thresholds, restrict allowed ratings, or mark metrics as descriptive-only. Create a new YAML by copying `default.yaml` and customizing it.

Available roles: `default`, `senior_dev` (add more by dropping YAMLs in the roles directory).

## Development

### Database and Sample Data

Start services:
```bash
bash scripts/dev-infra.sh start
```

Seed RBAC and sample data (for testing /api/v1/organizations/, tenancy, etc.):
```bash
# Seed RBAC perms/roles (system/app roles)
uv run python scripts/init_rbac.py

# Load sample data (generic; orgs first, extensible to roles/assessments)
# Variations in status/timestamps; idempotent/graceful
uv run python scripts/load_sample_data.py

# Specific objects + drop first
uv run python scripts/load_sample_data.py --objects organizations --drop

# Custom YAML (keyed by artifact)
uv run python scripts/load_sample_data.py --config scripts/sample_data.yaml
```

Run tests:
```bash
uv run python -m pytest impact/tests/ -q
```

See `devrank/cli.py` (Typer-based) for unified CLI, or legacy scripts/ for direct use.

# Unified CLI (recommended)
```bash
# After `uv pip install -e .` (or uv sync for editable)
devrank --help
devrank seed load --objects organizations
devrank seed drop --objects organizations
devrank admin create --email admin@example.com --password secret
devrank rbac init
devrank report generate --user msyavuz --role senior_dev
# ... etc.
```

Deprecated legacy scripts preserved for backward compat (DRY delegation in CLI).

## License

See LICENSE file.
