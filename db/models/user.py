"""User model for multi-tenancy SaaS platform.

As of 2026 FastAPI/SQLAlchemy best practices:
- SQLAlchemy 2.0+ declarative mappings with Mapped[]
- PostgreSQL-native types where beneficial
- Enums for constrained string fields (type-safe, DB-enforced)
- UUIDv4 primary keys
- Timestamps with timezone awareness
- Unique constraints + indexes on email
- Optional password for SSO/OAuth flows (hash via passlib[bcrypt] in auth layer)
- Relationships: 1:N OAuthAccounts (multi-provider), 1:N created_assessments, 1:N created_scenarios,
  1:N submissions (assessments taken), org memberships (many-to-many via
  user_org_departments), RBAC roles (via user_role_assignments for system/org/dept
  perms mapping - prod-grade, supports standard users w/o orgs)
- DRY: inherit timestamps if base mixin added later; auth delegated to oauth_accounts;
  legacy user_role_enum deprecated (use RBAC system_roles/app_roles + assignments)

Future extensions (in dedicated files):
- db/models/organization.py
- db/models/user_organization.py (for tenant isolation, roles like owner/admin/member)
- db/models/assessment.py (replaces is_self_evaluating)
- db/models/submission.py (user-assessment join)
- db/models/user_role_assignment.py (RBAC perms mapping)
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from enum import Enum

from sqlalchemy import Boolean, Date, DateTime, String, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values
from .scenario import Scenario


class Gender(str, Enum):
    """Gender options - inclusive, DB-enforced."""

    MALE = "m"
    FEMALE = "f"
    OTHER = "other"
    PREFER_NOT_TO_SAY = "prefer_not_to_say"  # Added for better UX/privacy


class UserStatus(str, Enum):
    """Lifecycle status for users in SaaS platform."""

    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    BANNED = "BANNED"
    PENDING_VERIFICATION = "PENDING_VERIFICATION"  # Added for email/KYC flow


class UserRole(str, Enum):
    """Legacy global roles (deprecated).

    Retained for backward compat in existing data/migration; use full RBAC system
    (system_roles/app_roles + user_role_assignments) for new perms enforcement.
    DRY: avoid duplication; migrate off in auth layer/services.
    """

    USER = "user"
    ENGINEER = "engineer"  # Ties to impact/assessment domain
    ADMIN = "admin"
    SUPERUSER = "superuser"  # SaaS platform admin (maps to system_role)


class User(Base):
    """Core User entity for multi-tenancy SaaS.

    Supports:
    - Org/dept membership (0+ via user_org_departments - tenant isolation)
    - Assessments (self-eval or org-assigned)
    - RBAC roles/permissions (system admin, org/dept scoped, or standard user via
      user_role_assignments junction - prod-grade, maps to perms DB; legacy
      user_role_enum deprecated)
    - SSO/OAuth (password optional)
    - KYC/compliance fields
    - Audit timestamps

    All models in dedicated files per DRY/model-heavy forecast.
    """

    __tablename__ = "users"  # Conventional plural name for entities; avoids any PG/reserved word quirks

    # Primary key
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="UUIDv4 primary key - secure, distributed-friendly",
    )

    # Personal info
    name: Mapped[str] = mapped_column(
        String(100), nullable=False, doc="First/given name"
    )
    surname: Mapped[str] = mapped_column(
        String(100), nullable=False, doc="Last/family name"
    )
    nickname: Mapped[str | None] = mapped_column(
        String(50), nullable=True, doc="Preferred display name / handle"
    )
    avatar: Mapped[str | None] = mapped_column(
        String(500), nullable=True, doc="URL to profile avatar (S3/CDN)"
    )
    email: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True,
        doc="Unique email - used for auth, tenant invites",
    )
    gender: Mapped[Gender] = mapped_column(
        SAEnum(Gender, values_callable=enum_values, name="user_gender_enum", create_constraint=True, native_enum=True),
        nullable=False,
        server_default=text("'prefer_not_to_say'"),
        doc="Gender enum - inclusive defaults",
    )
    dob: Mapped[date | None] = mapped_column(
        Date, nullable=True, doc="Date of birth - for age-gated features if needed"
    )
    country: Mapped[str | None] = mapped_column(
        String(2), nullable=True, doc="ISO-3166-1 alpha-2 country code"
    )
    address: Mapped[str | None] = mapped_column(
        String(255), nullable=True, doc="Street address (GDPR-sensitive - encrypt at rest if reqd)"
    )
    zip: Mapped[str | None] = mapped_column(
        String(20), nullable=True, doc="Postal/ZIP code"
    )
    phone: Mapped[str | None] = mapped_column(
        String(30), nullable=True, doc="E.164 formatted phone"
    )

    # Compliance & verification
    is_verified: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default=text("false"),
        doc="Email verified?",
    )
    is_kyc_verified: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default=text("false"),
        doc="KYC/identity verified (for paid tiers)",
    )

    # Auth - core credential (password for non-OAuth fallback)
    # OAuth delegated to dedicated OAuthAccount (1:N; see db/models/oauth.py for
    # multi-provider support: GitHub + GitLab + etc. simultaneously)
    hashed_password: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        doc="Bcrypt/Argon2 hash via passlib.CryptContext - None for pure SSO/OAuth",
    )

    # SaaS / domain specific
    role: Mapped[UserRole] = mapped_column(
        SAEnum(UserRole, values_callable=enum_values, name="user_role_enum", create_constraint=True, native_enum=True),
        nullable=False,
        server_default=text("'user'"),
        doc="Global role; extend with org-scoped roles for multi-tenancy",
    )
    # is_self_evaluating REMOVED: replaced by dedicated Assessment model/table
    # (self-eval via Assessment.created_by == user.id; supports org/team evals)

    # TODO: org memberships via secondary table for multi-org support

    # Preferences / audit
    timezone: Mapped[str] = mapped_column(
        String(50), default="UTC", nullable=False, server_default=text("'UTC'"),
        doc="IANA tz for reports/dates",
    )
    locale: Mapped[str] = mapped_column(
        String(10), default="en", nullable=False, server_default=text("'en'"),
        doc="BCP-47 locale",
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, doc="Last successful auth timestamp"
    )

    # Timestamps (DRY candidate for TimestampMixin)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Creation timestamp (UTC)",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="Last update timestamp - auto via trigger/ORM",
    )

    status: Mapped[UserStatus] = mapped_column(
        SAEnum(
            UserStatus,
            values_callable=enum_values,
            name="user_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'ACTIVE'"),
        doc="User lifecycle status",
    )

    # Relationships (string annos for forward refs; lazy='selectin' for perf in API)
    # OAuthAccounts: multi-provider (e.g., GitHub + GitLab) - core for req
    oauth_accounts: Mapped[list[OAuthAccount]] = relationship(
        "OAuthAccount",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",  # Efficient for FastAPI serialization
    )
    # created_assessments: 1:N to Assessment (replaces is_self_evaluating flag)
    # Users can create multiple (self-eval or org)
    created_assessments: Mapped[list[Assessment]] = relationship(
        "Assessment",
        back_populates="creator",
        lazy="selectin",
    )
    # created_scenarios: 1:N (users create org/global scenarios)
    created_scenarios: Mapped[list[Scenario]] = relationship(
        "Scenario",
        back_populates="creator_user",
        lazy="selectin",
    )
    # submissions: 1:N to Submission (assessments taken; self-eval or org role)
    # Joins User to Assessment with extra data (status, timestamps)
    submissions: Mapped[list[Submission]] = relationship(
        "Submission",
        back_populates="user",
        lazy="selectin",
    )
    # org_dept_memberships: 1:N to UserOrgDepartment for multi-org/dept tenancy
    # (fixed backref mismatch surfaced in RBAC mapping; DRY with user_org_department.py)
    org_dept_memberships: Mapped[list["UserOrgDepartment"]] = relationship(
        "UserOrgDepartment",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # RBAC perms mapping: 1:N to UserRoleAssignment (system_roles/app_roles + scopes for
    # org/dept; enables standard users w/ default APP role, multi-roles/tenancy)
    # DRY with role_permissions; prod-grade for JWT claims/services
    role_assignments: Mapped[list["UserRoleAssignment"]] = relationship(
        "UserRoleAssignment",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # Server-side refresh tokens (hashed; cascade-delete on user removal)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="dynamic",  # dynamic: never eager-load all tokens; query on demand
    )
    # Multi-org tenancy via direct FKs in child tables (positions/submissions/org_id etc.) per intent
    # Plus user_org_departments for membership; RBAC assignments layered for perms
    # No UserOrganization assoc (misunderstanding surfaced/removed)
    # Example stubs (uncomment once more models wired)
    # roles: Mapped[list["Role"]] = ...
    # organizations: via FKs...

    def __repr__(self) -> str:
        """DRY repr for debugging/logging - avoids PII leakage."""
        return f"<User(id={self.id}, email={self.email}, status={self.status})>"
