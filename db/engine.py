"""Database engine and session factories.

Provides both async (for FastAPI endpoints) and sync (for Alembic migrations
and Celery workers) engines.  Connection URLs come from ``config`` (which
reads env-var overrides on top of hardcoded dev defaults).
"""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from config import DATABASE_URL, DATABASE_URL_SYNC

# Async engine — used by FastAPI request handlers (asyncpg driver).
async_engine = create_async_engine(
    DATABASE_URL, echo=False, pool_pre_ping=True, connect_args={"timeout": 5},
)
AsyncSessionLocal = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False,
)

# Sync engine — used by Alembic migrations and Celery workers (psycopg2 driver).
sync_engine = create_engine(
    DATABASE_URL_SYNC, echo=False, pool_pre_ping=True, connect_args={"connect_timeout": 5},
)
SyncSessionLocal = sessionmaker(sync_engine, class_=Session)
