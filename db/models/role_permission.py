"""RolePermission junction for RBAC many-to-many.

Links roles (system or app) to permissions with role_type discriminator for
polymorphic FK (role_id -> system_roles.id or app_roles.id). Unique constraint
prevents dupes.

DRY: Single junction for both role types (avoids duplicate tables); enforced
at DB (unique) + app layer (service guards/JWT claims). No timestamps/status
per spec (minimal assoc like implicit in other junctions); enum native PG.

FastAPI 2026 pattern: dedicated junction model (as in UserOrgDepartment) for
queryability/indexes. Permissions checked in deps/services, not ORM cascades yet.
"""

from __future__ import annotations

import uuid
from enum import Enum

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class RoleType(str, Enum):
    """Discriminator for polymorphic role_id (SYSTEM vs APP roles)."""

    SYSTEM = "SYSTEM"
    APP = "APP"


class RolePermission(Base):
    """Junction entity: role <-> permission (many-to-many with type).

    role_id is polymorphic (FK to system_roles.id if SYSTEM else app_roles.id) -
    enforced in services/auth layer (DB FK only on permission_id to avoid
    complex conditional constraints). Matches spec unique + role_type.

    Future: add user_role_assignments table tying users to roles (with JWT claims).
    """

    __tablename__ = "role_permissions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    role_type: Mapped[RoleType] = mapped_column(
        SAEnum(
            RoleType,
            name="role_type_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
    )
    # Polymorphic FK: role_id targets system_roles.id or app_roles.id based on role_type
    # No direct ForeignKey in model (would fail for dual targets); index only.
    # DB enforcement via app logic + unique; FK constraint omitted to prevent
    # create errors (consistent with clean SaaS patterns).
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    permission_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("permissions.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Rels (one-to-many from roles/perm; string annos for circulars)
    permission: Mapped["Permission"] = relationship("Permission")
    # For system/app: use hybrid property or separate queries; e.g.,
    # if role_type == RoleType.SYSTEM: system_role = relationship...

    __table_args__ = (
        # Unique per spec: prevents duplicate assignments
        UniqueConstraint(
            "role_type", "role_id", "permission_id", name="uq_role_type_role_permission"
        ),
    )

    def __repr__(self) -> str:
        return f"<RolePermission(role_type={self.role_type}, role_id={self.role_id}, perm_id={self.permission_id})>"
