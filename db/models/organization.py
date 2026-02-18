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

from sqlalchemy import DateTime, String, func, text
from sqlalchemy import Enum as SAEnum
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
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
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
    activated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    banned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (depts, positions; users link via direct FKs in positions/submissions per intent - no assoc table)
    departments: Mapped[list[Department]] = relationship(
        "Department", back_populates="organization", cascade="all, delete-orphan"
    )
    positions: Mapped[list[Position]] = relationship(
        "Position", back_populates="organization", cascade="all, delete-orphan"
    )
    # user_memberships: via UserOrgDepartment junction for multi-user tenancy
    # (fixed backref mismatch surfaced during RBAC mapping; DRY with user_org_department.py)
    # users: via FKs in child tables (multi-tenancy kept simple) + RBAC assignments
    # roles: org-specific (global roles have org_id=NULL)
    user_memberships: Mapped[list["UserOrgDepartment"]] = relationship(
        "UserOrgDepartment", back_populates="organization", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Organization(id={self.id}, slug={self.slug}, status={self.status})>"
