"""Position model - role in dept/org.

Positions fill roles; link to submissions (user takes for position/self).

DRY: rels to org/dept/role/submissions; status/timestamps.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class PositionStatus(str, Enum):
    """Position lifecycle."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DELETED = "DELETED"


class Position(Base):
    """Position entity (e.g., 'Senior Eng in Eng Dept')."""

    __tablename__ = "positions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dept_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("departments.id", ondelete="SET NULL"), nullable=True, index=True
    )
    role_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("roles.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    status: Mapped[PositionStatus] = mapped_column(
        SAEnum(
            PositionStatus,
            name="position_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'DRAFT'"),
    )
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (back to org/dept/role; submissions ref position_id)
    organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="positions"
    )
    department: Mapped[Optional["Department"]] = relationship("Department")
    role: Mapped["Role"] = relationship("Role")
    # submissions: Mapped[list["Submission"]] = ...  # via position_id

    __table_args__ = (
        # Unique slug; role per dept/org
        UniqueConstraint("org_id", "dept_id", "role_id", name="uq_org_dept_role"),
    )

    def __repr__(self) -> str:
        return f"<Position(id={self.id}, slug={self.slug}, role_id={self.role_id})>"
