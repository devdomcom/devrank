"""UserOrgDepartment model/table - connects users to orgs/depts.

Per req: users can belong to no/1+/multiple orgs + depts (multiple entries per user-org if multi-dept).
Supports self-eval/org tenancy; status/timestamps for lifecycle.

DRY/FastAPI/SQLAlchemy 2026: dedicated file; FKs to User/Org/Dept; rels.
Permissions in separate tables (future); org always has >=1 dept (enforced in API/business logic).

No dupe deleted_at (fixed); unique per user/org/dept.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, ForeignKey, UniqueConstraint, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values


class UserOrgDeptStatus(str, Enum):
    """Membership status (SaaS tenancy)."""

    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    DELETED = "DELETED"


class UserOrgDepartment(Base):
    """Join entity: User-Org-Department membership.

    Allows multiple depts per user/org; direct, no assoc bloat.
    """

    __tablename__ = "user_org_departments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dept_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("departments.id", ondelete="SET NULL"), nullable=True, index=True
    )
    status: Mapped[UserOrgDeptStatus] = mapped_column(
        SAEnum(
            UserOrgDeptStatus,
            values_callable=enum_values,
            name="user_org_dept_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
    )
    # Timestamps (deleted_at/deactivated_at per proposal; dupe deleted_at fixed to one)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (back to user/org/dept)
    user: Mapped[User] = relationship(
        "User", back_populates="org_dept_memberships"
    )
    organization: Mapped[Organization] = relationship(
        "Organization", back_populates="user_memberships"
    )
    department: Mapped[Department | None] = relationship("Department")

    __table_args__ = (
        # Unique per user/org/dept (allows multi-dept per user-org)
        UniqueConstraint("user_id", "org_id", "dept_id", name="uq_user_org_dept"),
    )

    def __repr__(self) -> str:
        return f"<UserOrgDepartment(user_id={self.user_id}, org_id={self.org_id}, dept_id={self.dept_id})>"
