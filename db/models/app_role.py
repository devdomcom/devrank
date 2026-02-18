"""AppRole model for RBAC - tenant-scoped roles (ORG/DEPT).

App roles provide permissions scoped to organization or department level,
complementing system_roles (SaaS-wide).

DRY: Shares permission assignment via role_permissions junction (role_type=APP);
scope_level enum per spec. Timestamps/status follow Department/Organization/UserOrgDepartment
pattern for tenancy consistency.

2026 FastAPI/SQLA best practices: dedicated file per entity, avoids bloat in existing
role.py (which is for impact/metrics config only), Pydantic-compatible fields.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, String, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column  # relationship later

from db.base import Base


class AppRoleStatus(str, Enum):
    """App role lifecycle (ACTIVE/DELETED; soft-delete for tenant roles)."""

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class ScopeLevel(str, Enum):
    """Scope for app roles per spec: ORG (org-wide) or DEPT (department-specific)."""

    ORG = "ORG"
    DEPT = "DEPT"


class AppRole(Base):
    """AppRole entity - ORG/DEPT scoped roles for multi-tenant RBAC.

    E.g., org_admin, dept_manager. Enforced in services (e.g., dept:read only if
    user's dept matches). Separate from system_roles for isolation; ties to
    UserOrgDepartment for assignment (future user_role_links table).
    """

    __tablename__ = "app_roles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # Per spec: ORG or DEPT scope for tenancy
    scope_level: Mapped[ScopeLevel] = mapped_column(
        SAEnum(
            ScopeLevel,
            name="scope_level_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
    )
    status: Mapped[AppRoleStatus] = mapped_column(
        SAEnum(
            AppRoleStatus,
            name="app_role_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (DRY from dept/org models)
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
    # role_permissions: ...

    def __repr__(self) -> str:
        return f"<AppRole(id={self.id}, slug={self.slug}, scope={self.scope_level})>"
