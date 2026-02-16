"""SQLAlchemy ORM models.

Sample model to validate the Alembic migration pipeline.
Real schemas (users, reports, metric snapshots, etc.) will be added here.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class Sample(Base):
    """Throwaway table used only to prove migrations work end-to-end.

    Will be removed once real models land.
    """

    __tablename__ = "sample"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )
