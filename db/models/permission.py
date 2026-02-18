"""Permission model for RBAC system.

Permissions represented by “resource:action” strings (e.g., `org:read`, `dept:settings:write`)
in the DB. Enforced at route and service layers with JWT-based auth.

DRY: shared across system_roles (SaaS admins) and app_roles (ORG/DEPT scoped) via
role_permissions junction. Status/soft-delete timestamps consistent with Organization/
Department/User models. Native PG enum per codebase pattern.

Follows 2026 FastAPI/SQLAlchemy best practices: modular file, Mapped typing,
string-annotated relationships to avoid circular imports.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, String, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values


class PermissionStatus(str, Enum):
    """Permission lifecycle (ACTIVE for use, DELETED for soft-remove)."""

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class Permission(Base):
    """Permission entity.

    Slug encodes resource:action for granular RBAC checks (e.g., in Depends deps
    or service guards). Matches spec; extensible for new resources.
    """

    __tablename__ = "permissions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True,
        doc="resource:action e.g. 'org:read', 'system:admin'"
    )
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    status: Mapped[PermissionStatus] = mapped_column(
        SAEnum(
            PermissionStatus,
            values_callable=enum_values,
            name="permission_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (DRY lifecycle pattern from org/dept/role models)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (many side of junction; back_populates in RolePermission)
    # role_permissions: Mapped[list["RolePermission"]] = relationship(
    #     "RolePermission", back_populates="permission", cascade="all, delete-orphan"
    # )
    # system_roles/app_roles via junction (polymorphic)

    def __repr__(self) -> str:
        return f"<Permission(id={self.id}, slug={self.slug})>"
