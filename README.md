# DevRank

DevRank is a Scenario-Driven Evaluation Framework for Engineering Excellence and Impact that vets candidates using real-world metrics.

## Overview

The impact assessment tool analyzes GitHub activity to compute metrics that quantify a developer's productivity, code quality, and collaboration effectiveness. It supports fetching fresh data from GitHub or using existing data dumps.

## Features

- **Metrics Calculation**: Compute various metrics like PR throughput, cycle time, review leverage, etc.
- **Configurable Thresholds**: Customize rating thresholds for metrics in `impact/thresholds.py`.
- **Data Ingestion**: Parse GitHub data from JSONL dumps.
- **Report Generation**: Generate human-readable reports with ratings and detailed breakdowns.

## Prerequisites

- Python 3.11+
- Docker and Docker Compose (for Celery-based fetching)
- GitHub token (for live fetching)

## Installation

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd devrank
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. For live fetching, start the Celery worker:
   ```bash
   docker compose up -d
   ```

## Usage

### Generating a Report from an Existing Data Dump

If you already have a data dump (e.g., from a previous fetch), you can generate a report directly:

```bash
uv run python impact/scripts/generate_report.py \
  --existing-dump /path/to/dump/directory \
  --metrics pr_throughput cycle_time review_leverage
```

Replace `/path/to/dump/directory` with the path to your dump (which should contain `dump_manifest.json` and a `canonical/` subdirectory with JSONL files).

### Generating a Report with Fresh Data

To fetch new data from GitHub and generate a report:

1. Ensure the Celery worker is running:
   ```bash
   docker compose up -d
   ```

2. Run the report generation script with fetch parameters:
   ```bash
   uv run python impact/scripts/generate_report.py \
     --dump-path /path/to/new/dump \
     --fetch-user <github-username> \
     --fetch-repos <owner/repo1>,<owner/repo2> \
     --fetch-token <your-github-token> \
     --metrics pr_throughput cycle_time review_leverage
   ```

   - `--fetch-user`: The GitHub username of the user to assess.
   - `--fetch-repos`: Comma-separated list of repositories (format: owner/repo).
   - `--fetch-token`: GitHub personal access token. Can also be set via `GITHUB_TOKEN` environment variable.
   - `--fetch-from` and `--fetch-to`: Optional ISO date strings to limit the fetch window (default: last 365 days).

The script will:
- Queue a fetch task via Celery.
- Wait for the fetch to complete.
- Ingest the data.
- Compute the specified metrics.
- Print a formatted report to stdout.

### Available Metrics

Metrics are organized in `impact/metrics/plugins/` (`authored/` for own work, `influence/` for team impact; duplicates cleaned).

- `pr_throughput`: Measures PR volume and merge ratio (backlog merges allowed, ratio can >1.0).
- `cycle_time`: Time from PR creation to merge.
- `pr_merge_effectiveness`: Effectiveness based on back-and-forth reviews.
- `pr_size_distribution`: Distribution of additions, deletions, and changes across PRs.
- `trivial_contribution_rate`: Daily rate of trivial PRs (< 10 lines changed) for anti-gaming.
- `module_area_breadth`: Avg distinct areas touched per PR (true per-PR average).
- `review_leverage`: Impact of reviews on team productivity (improved attribution).
- `review_iterations`: Average review iterations per PR.
- `time_to_first_review`: Median time to first review.
- `slow_review_response`: Median response time to review comments.
- `active_weeks`: Granular active/inactive weeks + gaps to detect disengagement/absences (lower max gap better).
- `burstiness`: Distribution of activity within active weeks (lower burst ratio = steadier contributions).
- `reviews_given`: Review volume given (collaboration; opinionated rate by period).
- `pr_merge_rate`: % of reviews leading to merge (proximity/no-intervening for clear influence).
- `change_inducing_review_rate`: % reviews inducing immediate commits (clear correlation).
- `approval_to_merge_ratio`: % approvals that were final (no reworks, direct merge).
- `review_turnaround_time`: Median time to act on opened PRs (fast response; period-balanced).
- `blocking_comment_rate`: % blocking CRs (ownership vs cosmetic feedback).
- `unblock_time`: Median time to re-review after blocking CR (excludes author lag).

## Data Format

Dumps are stored in a directory with:
- `dump_manifest.json`: Metadata including user, date range, and provider.
- `canonical/`: Subdirectory with JSONL files (pull_requests.jsonl, commits.jsonl, etc.).

## Configuration

Metric rating thresholds can be customized in `impact/thresholds.py`. Each metric has thresholds for excellent, good, neutral, and bad ratings based on specific keys in the metric details. Modify the lambda functions to adjust the criteria for your organization's standards.

## Development

Run tests:
```bash
uv run pytest
```

## License

See LICENSE file.
