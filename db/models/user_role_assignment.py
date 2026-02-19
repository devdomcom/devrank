"""UserRoleAssignment junction for production-grade RBAC mapping.

Maps users to roles (system/app) with optional scoping to orgs/depts. Supports:
- System admins (SYSTEM role, no scope).
- Org/dept members with perms (APP role + scope_org_id/scope_dept_id).
- Standard users (no org/system role; default APP role with null scopes).

Industry-standard RBAC (e.g., hybrid with ABAC scoping like AWS IAM/GitHub): dedicated
assignment table enables multi-roles, tenancy isolation, defaults, auditing.

DRY: reuses system/app_roles/permissions; parallels user_org_departments (membership
vs. perms separation); status/timestamps/enums consistent. Polymorphic role_id.

No alterations to legacy user.role_enum (deprecate later in auth layer).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

# RoleType reused from RBAC (DRY; role_permission doesn't ref back, so safe)
from .role_permission import RoleType
from sqlalchemy import DateTime, ForeignKey, UniqueConstraint, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values


class AssignmentStatus(str, Enum):
    """Assignment lifecycle (ACTIVE for effective perms; DELETED soft-remove)."""

    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    DELETED = "DELETED"


class UserRoleAssignment(Base):
    """User <-> Role assignment with scope for RBAC.

    Enables all req cases:
    - System admin: role_type=SYSTEM, role_id=system_role.id, org/dept=null.
    - Org perms: role_type=APP (ORG scope), role_id=app_role.id, org_id=...
    - Dept perms: similar + dept_id.
    - Standard user: role_type=APP (default 'standard_user' role), scopes=null.

    Enforced in services/JWT claims. Unique constraint prevents dupes.
    """

    __tablename__ = "user_role_assignments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    role_type: Mapped[RoleType] = mapped_column(
        # Reuse from role_permission.py (DRY; imported via models)
        SAEnum(
            RoleType,
            values_callable=enum_values,
            name="role_type_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
    )
    # Polymorphic: SYSTEM -> system_roles.id; APP -> app_roles.id (enforced in app)
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    # Scope for APP roles (null for SYSTEM or standard/global APP roles; DRY nullable)
    org_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=True, index=True
    )
    dept_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("departments.id", ondelete="SET NULL"), nullable=True, index=True
    )
    status: Mapped[AssignmentStatus] = mapped_column(
        SAEnum(
            AssignmentStatus,
            values_callable=enum_values,
            name="assignment_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (DRY from user_org_dept/org models for auditing)
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
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (DRY back_populates; User will get backref)
    user: Mapped["User"] = relationship(
        "User", back_populates="role_assignments"
    )
    # Polymorphic role: app/system via type; org/dept for scope
    organization: Mapped["Organization | None"] = relationship("Organization")
    department: Mapped["Department | None"] = relationship("Department")
    # role resolved in service

    # No UniqueConstraint here (composite NULLs treated distinct in PG, allowing
    # dupes for standard users/null scopes). Instead, partial unique indexes in
    # migration (DB-enforced; DRY Postgres best practice for nullable uniqueness).
    # Covers: standard/system (null scopes) + scoped org/dept.
    __table_args__ = (
        # Partial uniques handled in migration for prod-grade RBAC.
    )

    def __repr__(self) -> str:
        return (
            f"<UserRoleAssignment(user_id={self.user_id}, "
            f"role_type={self.role_type}, role_id={self.role_id}, "
            f"org={self.org_id}, dept={self.dept_id})>"
        )
