"""Organization model for multi-tenancy SaaS.

Core tenant entity: orgs have depts/positions/roles/users. Status lifecycle with timestamps.

DRY/FastAPI/SQLAlchemy 2026: dedicated file, rels to users/depts/positions, soft-delete timestamps.
Global roles can ref org_id=NULL.

Future: billing, settings in config-like JSON fields.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import DateTime, Enum as SAEnum, String, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class OrganizationStatus(str, Enum):
    """Org lifecycle (SaaS compliance)."""

    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    BANNED = "BANNED"
    DELETED = "DELETED"


class Organization(Base):
    """Organization (tenant) entity.

    Users link via UserOrganization assoc (multi-tenancy: multiple orgs/user).
    """

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    status: Mapped[OrganizationStatus] = mapped_column(
        SAEnum(
            OrganizationStatus,
            name="organization_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (DRY lifecycle pattern)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    activated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    banned_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (depts, positions; users link via direct FKs in positions/submissions per intent - no assoc table)
    departments: Mapped[list["Department"]] = relationship(
        "Department", back_populates="organization", cascade="all, delete-orphan"
    )
    positions: Mapped[list["Position"]] = relationship(
        "Position", back_populates="organization", cascade="all, delete-orphan"
    )
    # roles: org-specific (global roles have org_id=NULL)
    # users: via FKs in child tables (multi-tenancy kept simple)

    def __repr__(self) -> str:
        return f"<Organization(id={self.id}, slug={self.slug}, status={self.status})>"
