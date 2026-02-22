"""Department model - org subunit.

Orgs have depts; depts have positions. Status lifecycle.

DRY: rels to org/positions; timestamps.

Default department: Every organization has exactly one default department
(``is_default=True``, slug ``general``) that is auto-created with the org.
The default department cannot be soft-deleted or deactivated via API — it
lives as long as the organization does (cascade-deleted with it).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, ForeignKey, String, UniqueConstraint, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values


class DepartmentStatus(str, Enum):
    """Dept lifecycle."""

    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    DELETED = "DELETED"


class Department(Base):
    """Department entity."""

    __tablename__ = "departments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    slug: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True  # unique per org (not global)
    )
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_default: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false"),
        doc="True for the auto-created default department; "
        "cannot be soft-deleted or deactivated via API.",
    )
    status: Mapped[DepartmentStatus] = mapped_column(
        SAEnum(
            DepartmentStatus,
            values_callable=enum_values,
            name="department_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
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
    activated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels
    organization: Mapped[Organization] = relationship(
        "Organization", back_populates="departments"
    )
    positions: Mapped[list[Position]] = relationship(
        "Position", back_populates="department", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # Unique slug per org
        UniqueConstraint("org_id", "slug", name="uq_org_dept_slug"),
    )

    def __repr__(self) -> str:
        return f"<Department(id={self.id}, slug={self.slug}, org_id={self.org_id})>"
