"""Alembic environment configuration.

Uses the sync engine from ``db.engine`` (psycopg2 driver) and the shared
``Base.metadata`` so that ``--autogenerate`` can diff models against the DB.
"""
from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool

from config import DATABASE_URL_SYNC
from db.base import Base
import db.models  # noqa: F401 — ensure all models are registered on Base.metadata

# Alembic Config object — provides access to alembic.ini values.
config = context.config

# Override the URL from alembic.ini with the one from our config module.
config.set_main_option("sqlalchemy.url", DATABASE_URL_SYNC)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# MetaData for autogenerate support.
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (SQL script emission)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (live DB connection)."""
    from sqlalchemy import engine_from_config

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
