"""Fixtures for DB migration tests — uses a dedicated test database.

Creates ``devrank_test`` on the same Postgres instance so migration tests
(which downgrade/upgrade repeatedly) never touch the dev database.
"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Test database name (separate from dev ``devrank``)
# ---------------------------------------------------------------------------
TEST_DB = "devrank_test"

# Base connection URL (to the ``postgres`` maintenance DB for CREATE/DROP)
_MAINTENANCE_URL = os.environ.get(
    "DEVRANK_DATABASE_URL_SYNC",
    "postgresql://devrank:devrank@localhost:5432/postgres",
)
# Derive test DB URL from the sync URL (swap DB name)
_base = os.environ.get(
    "DEVRANK_DATABASE_URL_SYNC",
    "postgresql://devrank:devrank@localhost:5432/devrank",
)
_TEST_DB_URL = _base.rsplit("/", 1)[0] + f"/{TEST_DB}"

# Override env BEFORE config/engine modules are imported by test files.
# This ensures ``config.settings.database_url_sync`` and ``db.engine.sync_engine``
# both point to the test database.
os.environ["DEVRANK_DATABASE_URL_SYNC"] = _TEST_DB_URL
os.environ["DEVRANK_DATABASE_URL"] = _TEST_DB_URL.replace(
    "postgresql://", "postgresql+asyncpg://"
)


# ---------------------------------------------------------------------------
# Session-scoped: create/drop the test database once per pytest session
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _ensure_test_database():
    """Create the test database before all tests, drop it after."""
    # Connect to maintenance DB (``postgres``) with AUTOCOMMIT for DDL
    maint_url = _base.rsplit("/", 1)[0] + "/postgres"
    engine = create_engine(maint_url, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": TEST_DB},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{TEST_DB}"'))

    yield

    # Teardown: drop the test database
    with engine.connect() as conn:
        # Terminate any lingering connections
        conn.execute(
            text(
                f"SELECT pg_terminate_backend(pid) "
                f"FROM pg_stat_activity "
                f"WHERE datname = '{TEST_DB}' AND pid <> pg_backend_pid()"
            )
        )
        conn.execute(text(f'DROP DATABASE IF EXISTS "{TEST_DB}"'))

    engine.dispose()
