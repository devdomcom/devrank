"""SystemRole model for RBAC - SaaS-wide admin roles.

These roles have permissions all over the system (e.g., super admins, platform owners),
distinct from app_roles (ORG/DEPT scoped for tenant isolation).

DRY: Permissions via role_permissions junction (role_type=SYSTEM); status/timestamps
pattern matches Organization/Role/Department for consistency. is_system_wide flag
per spec (always True conceptually but explicit).

2026 FastAPI best practices: modular model file, UUID PKs, native enums, soft-delete.
Relationships string-annotated for Alembic/env.py compatibility.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, String, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class SystemRoleStatus(str, Enum):
    """System role lifecycle (ACTIVE/DELETED per spec; no DRAFT for prod roles)."""

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class SystemRole(Base):
    """SystemRole entity - SaaS administrator roles with global permissions.

    Enforced in auth deps/services; e.g., superuser for all endpoints.
    Distinct table from metric Role (impact/config) and app_roles for clean RBAC.
    """

    __tablename__ = "system_roles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # Per spec: flags system-wide (SaaS admin scope)
    is_system_wide: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    status: Mapped[SystemRoleStatus] = mapped_column(
        SAEnum(
            SystemRoleStatus,
            name="system_role_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (DRY soft-delete/lifecycle from other SaaS models)
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

    # Rels via junction (polymorphic)
    # permissions: Mapped[list["Permission"]] = relationship(
    #     secondary="role_permissions", ... but since junction typed, use RolePermission
    # )
    # role_permissions: Mapped[list["RolePermission"]] = relationship(...)

    def __repr__(self) -> str:
        return f"<SystemRole(id={self.id}, slug={self.slug}, system_wide={self.is_system_wide})>"
