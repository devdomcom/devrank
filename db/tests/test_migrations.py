"""Tests for Alembic migration pipeline.

Requires a running PostgreSQL instance (``docker compose up postgres``).
Each test starts from a clean ``base`` state and upgrades/downgrades
to validate the full migration chain.
"""
from __future__ import annotations

import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from db.engine import SyncSessionLocal, sync_engine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _alembic_cfg() -> Config:
    """Return an Alembic Config wired to our alembic.ini."""
    cfg = Config("alembic.ini")
    return cfg


def _current_rev() -> str | None:
    """Return the current Alembic revision applied to the database."""
    with sync_engine.connect() as conn:
        result = conn.execute(text("SELECT version_num FROM alembic_version"))
        row = result.fetchone()
        return row[0] if row else None


def _table_exists(name: str) -> bool:
    """Check if a table exists in the public schema."""
    insp = inspect(sync_engine)
    return name in insp.get_table_names()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_db():
    """Downgrade to base before each test so every test starts clean."""
    cfg = _alembic_cfg()
    command.downgrade(cfg, "base")
    yield
    # Cleanup: leave DB at base after each test.
    command.downgrade(cfg, "base")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMigrationUpgrade:
    """Upgrade to head creates expected schema."""

    def test_upgrade_creates_sample_table(self):
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        assert _table_exists("sample")
        assert _current_rev() is not None

    def test_upgrade_sample_table_columns(self):
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        insp = inspect(sync_engine)
        cols = {c["name"] for c in insp.get_columns("sample")}
        assert cols == {"id", "name", "created_at"}


class TestMigrationDowngrade:
    """Downgrade back to base removes schema cleanly."""

    def test_downgrade_removes_sample_table(self):
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        assert _table_exists("sample")

        command.downgrade(cfg, "base")
        assert not _table_exists("sample")

    def test_downgrade_clears_revision(self):
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")

        assert _current_rev() is None


class TestMigrationRoundTrip:
    """Full upgrade → downgrade → upgrade cycle is idempotent."""

    def test_round_trip(self):
        cfg = _alembic_cfg()

        command.upgrade(cfg, "head")
        rev1 = _current_rev()

        command.downgrade(cfg, "base")
        assert not _table_exists("sample")

        command.upgrade(cfg, "head")
        rev2 = _current_rev()

        assert rev1 == rev2
        assert _table_exists("sample")


class TestSampleTableUsable:
    """After migration, the ORM model can insert and query rows."""

    def test_insert_and_query(self):
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        row_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            session.execute(
                text("INSERT INTO sample (id, name) VALUES (:id, :name)"),
                {"id": str(row_id), "name": "test-row"},
            )
            session.commit()

        with SyncSessionLocal() as session:
            row = session.execute(
                text("SELECT name FROM sample WHERE id = :id"),
                {"id": str(row_id)},
            ).fetchone()
            assert row is not None
            assert row[0] == "test-row"
