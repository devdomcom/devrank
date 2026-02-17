"""Role model - global/org-specific configs for assessments/positions.

Slug unique; JSON config for metrics/thresholds (from impact/config/roles).
Version for compatibility; creator/org for tenancy (global=True ignores org_id).

DRY: rels to positions/submissions; soft-delete timestamps.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import Boolean, DateTime, Enum as SAEnum, ForeignKey, JSON, String, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class RoleStatus(str, Enum):
    """Role lifecycle."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DELETED = "DELETED"


class Role(Base):
    """Role entity (e.g., 'senior_dev'; config JSON for impact metrics)."""

    __tablename__ = "roles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    # Config: JSON for role-specific settings (DRY with impact/config/roles.yaml)
    config: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, doc="JSON config (thresholds, metrics etc.)"
    )
    version: Mapped[int] = mapped_column(
        nullable=False, server_default=text("1"), doc="Config version for compat"
    )
    creator: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    org_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=True,  # NULL=global
        index=True,
    )
    global_role: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"), doc="Usable across orgs"
    )
    status: Mapped[RoleStatus] = mapped_column(
        SAEnum(
            RoleStatus,
            name="role_status_enum",
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

    # Rels
    creator_user: Mapped["User"] = relationship("User")
    organization: Mapped[Optional["Organization"]] = relationship("Organization")
    # positions, submissions etc. backrefs

    def __repr__(self) -> str:
        return f"<Role(id={self.id}, slug={self.slug}, global={self.global_role})>"
