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
uv sync && uv pip install -e .
uv run devrank init            # starts infra, runs migrations, seeds RBAC + sample data, creates admin
uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

No `.env` file needed — `config.py` ships sensible localhost defaults for every setting. The app works out of the box.

## Setup

### 1. Install Dependencies

```bash
uv sync && uv pip install -e .
```

This installs all dependencies and registers the `uv run devrank` CLI. Run `uv run devrank --help` to see all subcommands.

### 2. Initialize

```bash
uv run devrank init
```

This single command runs all bootstrap steps:

| Step | What it does |
|---|---|
| **[1/5] Infrastructure** | Starts PostgreSQL + Redis via Docker Compose |
| **[2/5] Migrations** | Runs `alembic upgrade head` to create/update tables |
| **[3/5] RBAC** | Seeds permissions and roles from `permissions.yaml` |
| **[4/5] Sample data** | Loads sample organizations and roles |
| **[5/5] Admin user** | Creates a superuser if none exists |

Options: `--skip-infra` (if Docker is already running), `--admin-email` / `--admin-password` (customize admin credentials, defaults to `admin@devrank.local` / `admin`).

### 3. Start the API Server

```bash
uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

### 4. Verify

```bash
# Liveness probe
curl http://localhost:8000/health

# Infrastructure readiness (Postgres + Redis)
curl http://localhost:8000/health/infra

# Login and get a JWT token
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "admin@devrank.local", "password": "admin"}'
# → {"access_token": "eyJ...", "token_type": "bearer"}

# Use the token on protected endpoints
curl http://localhost:8000/api/v1/metrics/ \
  -H "Authorization: Bearer eyJ..."
```

**Swagger UI**: http://localhost:8000/docs
**ReDoc**: http://localhost:8000/redoc

### Infrastructure Details

The `devrank init` command (or `bash scripts/dev-infra.sh start` directly) auto-detects your environment:

| Environment | How services are reached | What happens |
|---|---|---|
| **Native Docker** (macOS / Linux host) | `localhost:5432` / `localhost:6379` | Starts containers, done |
| **Docker-in-Docker** (Dev Containers, Codespaces, Gitpod) | `postgres:5432` / `redis:6379` | Starts containers, connects your dev container to the `devrank` Docker network |

DinD is detected via `/.dockerenv`, cgroup markers, `/proc/self/mountinfo`, or environment variables set by Codespaces / Gitpod / VS Code Dev Containers.

Infrastructure management:

```bash
bash scripts/dev-infra.sh stop    # stop containers (keep data)
bash scripts/dev-infra.sh reset   # stop + delete volumes (fresh start)
bash scripts/dev-infra.sh status  # show container status
```

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

**Public (no auth)**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe (instant, no external calls) |
| GET | `/health/infra` | Readiness check (Postgres + Redis connectivity, version, latency) |
| POST | `/auth/login` | Authenticate with email/password, returns JWT |

**Auth (`/auth`)**

| Method | Path | Permission | Description |
|--------|------|------------|-------------|
| GET | `/auth/me` | authenticated | Current user context (roles, permissions) |

**Organizations (`/api/v1/organizations`)**

| Method | Path | Permission | Description |
|--------|------|------------|-------------|
| GET | `/organizations/` | `organizations:list` | Cursor-paginated list of organizations |
| GET | `/organizations/{id_or_slug}` | `organizations:read` | Single organization detail (org-scoped RBAC) |
| POST | `/organizations/` | `organizations:create` | Create organization (creator becomes org admin) |

**Impact metrics (`/api/v1`)**

| Method | Path | Permission | Description |
|--------|------|------------|-------------|
| GET | `/metrics/` | `metrics:list` | List all available metrics (slug, name, description) |
| GET | `/metrics/{slug}` | `metrics:read` | Compute a single metric |
| POST | `/metrics/compute` | `metrics:compute` | Compute a full metrics report from a dump |
| POST | `/metrics/compare` | `metrics:compute` | Compare metrics across two time windows |
| GET | `/roles/` | `roles:list` | List available role configs |
| GET | `/roles/{name}` | `roles:read` | Get a specific role config |
| POST | `/dumps/upload` | `dumps:upload` | Upload and validate a GitHub dump ZIP |

`user_login`, `start_date`, and `end_date` are inferred from the dump manifest when omitted.

All protected endpoints require a `Authorization: Bearer <token>` header. Permissions are enforced via RBAC — see [Authentication & RBAC](#authentication--rbac) below.

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

## Authentication & RBAC

DevRank uses JWT-based authentication with a YAML-driven RBAC permission system.

### Permissions

Permissions follow a `resource:action` format and are defined in [`api/auth/rbac/permissions.yaml`](api/auth/rbac/permissions.yaml). The full set:

| Permission | Description |
|---|---|
| `metrics:list` | List available metrics catalog |
| `metrics:read` | Read individual metric results |
| `metrics:compute` | Compute metrics reports and comparisons |
| `roles:list` | List available role configurations |
| `roles:read` | Read role configuration details |
| `dumps:upload` | Upload and validate dump ZIP files |
| `organizations:list` | List organizations |
| `organizations:read` | Read organization details |
| `organizations:create` | Create new organizations |
| `system:debug` | Access debug/test endpoints |

### Roles

Roles are defined in the same YAML and seeded to the database via `devrank rbac init`.

| Role | Type | Scope | Permissions |
|---|---|---|---|
| `superuser` | system | global | All permissions (wildcard) |
| `user` | system | global | `organizations:create` |
| `analyst` | app | org | `metrics:list`, `metrics:read`, `metrics:compute`, `roles:list`, `roles:read` |
| `org_admin` | app | org | `organizations:read` |

- **System roles** apply platform-wide. Every user gets the `user` role on signup.
- **App roles** are scoped to an organization (and optionally a department).
- **Superuser** bypasses all permission checks.

### Flow

1. `POST /auth/login` with email + password returns a JWT access token
2. Include `Authorization: Bearer <token>` on subsequent requests
3. `require_permission("slug")` on each route checks the user's role-derived permissions
4. Creating an organization (`POST /organizations/`) auto-assigns the creator as `org_admin` for that org

## Development

### Tests

```bash
uv run python -m pytest impact/tests/ db/tests/ -q
```

### CLI Reference

After installing with `uv pip install -e .`, all commands are available via `uv run devrank`:

```bash
uv run devrank --help                                 # show all subcommands
uv run devrank init                                   # full bootstrap (infra + migrations + RBAC + samples + admin)
uv run devrank init --skip-infra                      # same but skip Docker start (infra already running)
uv run devrank rbac init                              # seed RBAC permissions and roles only
uv run devrank admin create --email admin@example.com --password secret
uv run devrank seed load                              # load all sample data
uv run devrank seed load --objects organizations      # load specific artifacts
uv run devrank seed drop --objects organizations      # drop sample data
uv run devrank report generate --user msyavuz --role senior_dev
uv run devrank fetch github --user msyavuz
uv run devrank api test --url http://localhost:8000   # smoke-test endpoints
```

## License

See LICENSE file.
