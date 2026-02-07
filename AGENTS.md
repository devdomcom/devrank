# DevRank Agents Overview

This document outlines the high-level architecture and agents (components) of the DevRank impact assessment system.

## System Architecture

DevRank is built as a modular Python application that fetches, processes, and analyzes GitHub data to compute engineering impact metrics. The system is designed for scalability, with asynchronous fetching via Celery and in-memory data processing.

### Core Components

1. **Data Fetching Agent** (`impact/providers/`)
   - **GitHub Live Fetcher**: Asynchronously fetches data from GitHub API using Celery tasks.
   - **Configuration**: Supports token-based authentication, repository filtering, and date windows.

2. **Ingestion Agent** (`impact/ingestion/`)
   - **Dump Ingestion**: Parses pre-fetched JSONL dumps into canonical data structures.
   - **Adapters**: Provider-specific parsers (currently GitHub) that validate and transform raw data.

3. **Data Ledger Agent** (`impact/ledger/`)
   - **Ledger**: In-memory indexed store for efficient querying of canonical data.
   - **Indexing**: Builds time-ordered indexes for users, PRs, reviews, commits, etc.

4. **Metrics Computation Agent** (`impact/metrics/`)
   - **Metric Plugins**: Modular metric calculators (e.g., PR throughput, cycle time).
   - **Base Framework**: Abstract metric class for consistent implementation.

5. **Report Generation Agent** (`impact/scripts/`)
   - **Report Generator**: Orchestrates the pipeline from ingestion to metric computation and output.
   - **Rating System**: Applies thresholds to metric results for qualitative assessment.

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
- New metrics: Implement Metric subclasses in `impact/metrics/plugins/` (use `authored/` or `influence/` subdirs).
- Custom ingestion: Extend `Ingestion` base class.

### Best Practices

- **Modularity**: Each agent has a single responsibility.
- **Testing**: Comprehensive unit tests for all components.
- **Logging**: Structured logging throughout.
- **Error Handling**: Graceful failure with informative messages.
- **Performance**: In-memory processing for speed; async fetching for scale.
