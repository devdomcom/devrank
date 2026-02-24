"""Add core users + oauth_accounts + assessments tables for multi-tenancy SaaS.

Revision ID: ad29913efe04
Revises: 
Create Date: 2026-02-16 16:40:20.200201

All-in-one migration (per task: no real/prod DB yet, no incremental allowed - all changes consolidated here incl. Scenario full table + FKs/additional fields on existing tables). Table rename: users (plural).

Changes:
- OAuth separation: oauth_accounts for multi-provider (GitHub/GitLab/etc.).
- is_self_evaluating REMOVED; assessments table + Scenario (1:N) + FKs on submissions/evaluations + additional Scenario fields (files/system_prompt/personas/version/duration/tool)
  (proposal-aligned: title/slug/role_id/created_by/status etc.; supports self-eval/org).
- RBAC tables added: system_roles (SaaS admin), app_roles (ORG/DEPT scoped), permissions
  (resource:action slugs e.g. org:read), role_permissions (polymorphic junction with
  unique constraint). Per "start from implementing the required tables".

Covers:
- Enums (..., assessment_status + new RBAC enums) - native PG
- users (lean; is_self_evaluating dropped)
- oauth_accounts (1:N)
- assessments (FKs to users/roles, slug unique)
- RBAC: system_roles/app_roles/permissions/role_permissions
- Replaces sample table

DRY: dedicated models (db/models/{user,oauth,assessment,*.rbac}.py); enforces
JWT/route perms later.
"""

from collections.abc import Sequence

import sqlalchemy as sa
# PGEnum from postgresql dialect for enum cols (respects create_type=False in SQLA 2.0.46
# for PG native enums pre-created in op.execute; avoids support error).
# DRY with db/models/ SAEnum (native_enum=True); right def for migration.
from sqlalchemy.dialects.postgresql import ENUM as PGEnum
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ad29913efe04"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema to include User + OAuthAccount models.

    Uses op.create_table for full control (mirrors SQLAlchemy models in
    db/models/{user,oauth}.py).
    Enums created explicitly for PG native_enum=True.
    - users: plural convention, lean (oauth delegated)
    - oauth_accounts: multi-provider support (1:N, uniques)
    """
    # Create enum types first (PostgreSQL)
    # pgcrypto for gen_random_uuid()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        """
        CREATE TYPE gender_enum AS ENUM (
            'm', 'f', 'other', 'prefer_not_to_say'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE user_status_enum AS ENUM (
            'ACTIVE', 'DEACTIVATED', 'BANNED', 'PENDING_VERIFICATION'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE user_role_enum AS ENUM (
            'user', 'engineer', 'admin', 'superuser'
        )
        """
    )
    # OAuth enum (separate for modularity; supports GitHub/GitLab/etc.)
    op.execute(
        """
        CREATE TYPE oauth_provider_enum AS ENUM (
            'github', 'gitlab', 'linkedin', 'google', 'microsoft'
        )
        """
    )
    # Assessment status enum (for dedicated assessments table)
    op.execute(
        """
        CREATE TYPE assessment_status_enum AS ENUM (
            'DRAFT', 'PUBLISHED', 'DELETED'
        )
        """
    )
    # Submission status enum (for submissions join table)
    op.execute(
        """
        CREATE TYPE submission_status_enum AS ENUM (
            'PENDING', 'COMPLETED', 'ABANDONED', 'DELETED'
        )
        """
    )
    # New enums for org/dept/role/position/user_org (multi-tenancy chain)
    # Surfaced missing: user_organizations assoc table/enum for user-org links
    op.execute(
        """
        CREATE TYPE organization_status_enum AS ENUM (
            'ACTIVE', 'DEACTIVATED', 'BANNED', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE department_status_enum AS ENUM (
            'ACTIVE', 'DEACTIVATED', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE role_status_enum AS ENUM (
            'DRAFT', 'PUBLISHED', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE position_status_enum AS ENUM (
            'DRAFT', 'PUBLISHED', 'DELETED'
        )
        """
    )
    # Scenario enums (full lifecycle + tool; after role/position patterns)
    op.execute(
        """
        CREATE TYPE scenario_status_enum AS ENUM (
            'DRAFT', 'PUBLISHED', 'DEACTIVATED', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE scenario_tool_enum AS ENUM (
            'MEET', 'CHAT'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE org_role_enum AS ENUM (
            'owner', 'admin', 'member', 'guest'
        )
        """
    )

    # Create users table (lean core profile)
    # NOTE: id uses server_default for DB-side UUID gen (DRY with model Python fallback)
    # Table: conventional plural "users".
    # is_self_evaluating REMOVED (delegated to assessments table/model).
    # OAuth delegated to oauth_accounts.
    op.create_table(
        "users",
        # PK
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Personal
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("surname", sa.String(length=100), nullable=False),
        sa.Column("nickname", sa.String(length=50), nullable=True),
        sa.Column("avatar", sa.String(length=500), nullable=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column(
            "gender",
            # Right enum def for SQLA 2.0.46 compat (values list + name, create_type=False
            # after pre-create in op.execute; avoids support error on PG native enum.
            # DRY with model Gender enum values)
            PGEnum(
                "m",
                "f",
                "other",
                "prefer_not_to_say",
                name="gender_enum",
                create_type=False,  # already created above
            ),
            nullable=False,
            server_default="prefer_not_to_say",
        ),
        sa.Column("dob", sa.Date(), nullable=True),
        sa.Column("country", sa.String(length=2), nullable=True),
        sa.Column("address", sa.String(length=255), nullable=True),
        sa.Column("zip", sa.String(length=20), nullable=True),
        sa.Column("phone", sa.String(length=30), nullable=True),
        # Compliance
        sa.Column(
            "verified", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "kyc", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        # Auth core (SSO fallback; multi-OAuth in separate table)
        sa.Column("hashed_password", sa.String(length=255), nullable=True),
        # SaaS/domain
        sa.Column(
            "role",
            PGEnum(
                "user_role_enum",
                name="user_role_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="user",
        ),
        # Prefs
        sa.Column(
            "timezone", sa.String(length=50), nullable=False, server_default="UTC"
        ),
        sa.Column(
            "locale", sa.String(length=10), nullable=False, server_default="en"
        ),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "status",
            PGEnum(
                "user_status_enum",
                name="user_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Constraints/indexes
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )

    # Indexes for performance (email already unique=idx; explicit for others)
    # Index name: ix_<table>_<column> convention
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    # Note: gen_random_uuid() requires pgcrypto extension - assumed enabled in PG setup
    # (docker-compose postgres has it or init script)

    # Create oauth_accounts table (multi-provider SSO separation)
    # 1:N with users; uniques enforce one account per (user, provider)
    # Mirrors OAuthAccount model; supports GitHub/GitLab/etc. concurrently
    op.create_table(
        "oauth_accounts",
        # PK
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # FK to users (cascade delete for cleanup)
        sa.Column("user_id", sa.UUID(), nullable=False),
        # Provider details
        sa.Column(
            "provider",
            # Enum ref; create_type=False as created above
            PGEnum(
                "oauth_provider_enum",
                name="oauth_provider_enum",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("provider_user_id", sa.String(length=100), nullable=False),
        # Tokens (minimal; secure handling in auth layer)
        sa.Column("access_token", sa.String(length=500), nullable=True),
        sa.Column("refresh_token", sa.String(length=500), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # Constraints/FKs/indexes
        sa.PrimaryKeyConstraint("id"),
        # FK (explicit; ondelete=CASCADE)
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], ondelete="CASCADE"
        ),
        # Uniques for multi-provider integrity
        sa.UniqueConstraint("user_id", "provider", name="uq_user_provider"),
        sa.UniqueConstraint(
            "provider", "provider_user_id", name="uq_provider_external_id"
        ),
    )

    # Indexes
    op.create_index(
        "ix_oauth_accounts_user_id", "oauth_accounts", ["user_id"]
    )
    # Note: composite uniques auto-index; supports fast lookups e.g., by provider

    # Assessments table creation MOVED below (after roles table) to fix order issue
    # (assessments refs roles.id FK; prevents CREATE hard failure).
    # Role_id type fixed to UUID (consistent with UUID PKs).
    # Indexes moved with table.

    # Create tables for org/dept/role/position/eval/user_org assoc (multi-tenancy chain)
    # Surfaced missing link: user_organizations for users in multiple orgs (with roles/depts/positions)
    # Order: parents first (org > dept > role > position > eval) to satisfy FKs in submissions/user_org
    # All-in-one; placeholders ensure no breakage.

    # Organizations (tenant root)
    op.create_table(
        "organizations",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "status",
            PGEnum(
                "organization_status_enum",
                name="organization_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("banned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_organizations_slug", "organizations", ["slug"], unique=True)
    op.create_index("ix_organizations_name", "organizations", ["name"])

    # Departments (org subunit)
    op.create_table(
        "departments",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "is_default", sa.Boolean(), nullable=False, server_default=sa.text("false"),
            comment="True for the auto-created default department; cannot be soft-deleted or deactivated via API.",
        ),
        sa.Column(
            "status",
            PGEnum(
                "department_status_enum",
                name="department_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("org_id", "slug", name="uq_org_dept_slug"),
    )
    op.create_index("ix_departments_org_id", "departments", ["org_id"])

    # Roles (global/org; config JSON)
    op.create_table(
        "roles",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("creator", sa.UUID(), nullable=True),
        sa.Column("org_id", sa.UUID(), nullable=True),  # NULL=global
        sa.Column("global_role", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "status",
            PGEnum(
                "role_status_enum",
                name="role_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="DRAFT",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
        sa.ForeignKeyConstraint(["creator"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_roles_slug", "roles", ["slug"], unique=True)
    op.create_index("ix_roles_org_id", "roles", ["org_id"])

    # Assessments table (moved here after roles to fix CREATE order/FK dependency)
    # (refs roles.id; role_id type fixed to UUID for consistency with PKs)
    # Replaces User.is_self_evaluating; ties to impact/roles/metrics
    # Proposal-aligned: title/slug/role_id/created_by/status
    op.create_table(
        "assessments",
        # PK
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Core (per proposal)
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=True),
        # FKs (role_id UUID fixed)
        sa.Column("role_id", sa.UUID(), nullable=True),  # to roles.id
        # created_by: to users.id (CASCADE; matches model)
        sa.Column("created_by", sa.UUID(), nullable=False),
        # Status
        sa.Column(
            "status",
            # Enum ref; create_type=False
            PGEnum(
                "assessment_status_enum",
                name="assessment_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="DRAFT",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # Constraints/FKs/indexes
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),  # Per proposal
        # FKs explicit
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        # role_id FK (to roles.id; now after roles table)
        sa.ForeignKeyConstraint(
            ["role_id"], ["roles.id"], ondelete="SET NULL"
        ),
    )

    # Indexes for perf/unique
    op.create_index("ix_assessments_slug", "assessments", ["slug"], unique=True)
    op.create_index("ix_assessments_created_by", "assessments", ["created_by"])  # creator
    op.create_index("ix_assessments_role_id", "assessments", ["role_id"])

    # Positions (role in dept/org)
    op.create_table(
        "positions",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("dept_id", sa.UUID(), nullable=True),
        sa.Column("role_id", sa.UUID(), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "status",
            PGEnum(
                "position_status_enum",
                name="position_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="DRAFT",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dept_id"], ["departments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["role_id"], ["roles.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("slug"),
        sa.UniqueConstraint("org_id", "dept_id", "role_id", name="uq_org_dept_role"),
    )
    op.create_index("ix_positions_org_id", "positions", ["org_id"])
    op.create_index("ix_positions_slug", "positions", ["slug"], unique=True)
    # dept_id index: mirrors Position.dept_id(index=True) in ORM model; supports
    # efficient department-filter queries in the list-positions endpoint.
    op.create_index("ix_positions_dept_id", "positions", ["dept_id"])

    # Evaluations (results for assessment)
    op.create_table(
        "evaluations",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("assessment_id", sa.UUID(), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=False),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["assessments.id"], ondelete="CASCADE"
        ),
    )
    op.create_index("ix_evaluations_assessment_id", "evaluations", ["assessment_id"])

    # UserOrganizations assoc SKIPPED/removed (misunderstanding of intent - no table needed)
    # Direct FKs in positions/submissions suffice for user-org links/multi-tenancy.
    # (org_role_enum also dropped from enums/drop sections)

    # Create submissions table (User-Assessment join; per req)
    # Tracks participation (self-eval via nullable position_id, status, timestamps)
    # FKs to assessments/users + placeholders (evaluation/position); unique per user/assess
    op.create_table(
        "submissions",
        # PK
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # FKs
        sa.Column("assessment_id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        # Placeholders (future tables; UUID fixed for position_id consistency with Position.id PK)
        sa.Column("evaluation_id", sa.UUID(), nullable=True),
        sa.Column("position_id", sa.UUID(), nullable=True),  # NULL=self-assess
        # Status
        sa.Column(
            "status",
            # Enum ref
            PGEnum(
                "submission_status_enum",
                name="submission_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="PENDING",
        ),
        # Timestamps (full lifecycle)
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("abandoned_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints/FKs/indexes
        sa.PrimaryKeyConstraint("id"),
        # FKs explicit
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["assessments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], ondelete="CASCADE"
        ),
        # Placeholders (eval/position; UUID FKs)
        sa.ForeignKeyConstraint(
            ["evaluation_id"], ["evaluations.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["position_id"], ["positions.id"], ondelete="SET NULL"
        ),
        # Unique per user+assessment (one submission/take)
        sa.UniqueConstraint("user_id", "assessment_id", name="uq_user_assessment_submission"),
    )

    # Indexes for perf
    op.create_index("ix_submissions_user_id", "submissions", ["user_id"])
    op.create_index("ix_submissions_assessment_id", "submissions", ["assessment_id"])
    op.create_index("ix_submissions_position_id", "submissions", ["position_id"])

    # Scenarios table (first: required before FKs to it from submissions)
    # assessment_id required; files/personas JSON, system_prompt Text, version/duration/tool
    op.create_table(
        "scenarios",
        sa.Column("id", sa.UUID(), primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("assessment_id", sa.UUID(), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=True),
        sa.Column("org_id", sa.UUID(), nullable=True),
        sa.Column("dept_id", sa.UUID(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=True),
        sa.Column("global_scenario", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("files", sa.JSON(), nullable=True),
        sa.Column("system_prompt", sa.Text(), nullable=True),
        sa.Column("personas", sa.JSON(), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column(
            "tool",
            PGEnum(
                "scenario_tool_enum",
                name="scenario_tool_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="CHAT",
        ),
        sa.Column(
            "status",
            PGEnum(
                "scenario_status_enum",
                name="scenario_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="DRAFT",
        ),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["assessment_id"], ["assessments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dept_id"], ["departments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_scenarios_assessment_id", "scenarios", ["assessment_id"])
    op.create_index("ix_scenarios_org_id", "scenarios", ["org_id"])
    op.create_index("ix_scenarios_dept_id", "scenarios", ["dept_id"])
    op.create_index("ix_scenarios_slug", "scenarios", ["slug"], unique=True)

    # Additional columns for existing tables (scenario_id, submission_id FKs; unique update)
    # submissions: scenario_id (nullable)
    op.add_column("submissions", sa.Column("scenario_id", sa.UUID(), nullable=True))
    op.create_index("ix_submissions_scenario_id", "submissions", ["scenario_id"])
    op.drop_constraint("uq_user_assessment_submission", "submissions", type_="unique")
    op.create_unique_constraint("uq_user_assessment_scenario_submission", "submissions", ["user_id", "assessment_id", "scenario_id"])
    op.create_foreign_key(None, "submissions", "scenarios", ["scenario_id"], ["id"], ondelete="SET NULL")

    # evaluations: submission_id
    op.add_column("evaluations", sa.Column("submission_id", sa.UUID(), nullable=True))
    op.create_index("ix_evaluations_submission_id", "evaluations", ["submission_id"])


    # User-Org-Department membership (multi-tenancy join)
    op.execute(
        """
        CREATE TYPE user_org_dept_status_enum AS ENUM (
            'ACTIVE', 'DEACTIVATED', 'DELETED'
        )
        """
    )
    op.create_table(
        "user_org_departments",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("dept_id", sa.UUID(), nullable=True),
        sa.Column(
            "status",
            PGEnum(
                "user_org_dept_status_enum",
                name="user_org_dept_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dept_id"], ["departments.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("user_id", "org_id", "dept_id", name="uq_user_org_dept"),
    )
    op.create_index("ix_user_org_departments_user_id", "user_org_departments", ["user_id"])
    op.create_index("ix_user_org_departments_org_id", "user_org_departments", ["org_id"])
    op.create_index("ix_user_org_departments_dept_id", "user_org_departments", ["dept_id"])

    # ── RBAC tables for permission system ─────────────────────────────────────
    # Added here (continue on single migration as no prod DB per task).
    # Order: enums -> permissions (referenced) -> system_roles/app_roles -> junction.
    # Polymorphic role_id: no FK constraint (app-enforced + unique; see RolePermission
    # model doc). Matches spec: resource:action slugs, role_type discriminator,
    # unique (role_type, role_id, permission_id). DRY enums/statuses/timestamps.
    # Future: seed defaults (e.g., superuser perms) in data migration.
    #
    # Enums (native PG, distinct names to avoid collision)
    op.execute(
        """
        CREATE TYPE system_role_status_enum AS ENUM (
            'ACTIVE', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE app_role_status_enum AS ENUM (
            'ACTIVE', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE permission_status_enum AS ENUM (
            'ACTIVE', 'DELETED'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE role_type_enum AS ENUM (
            'SYSTEM', 'APP'
        )
        """
    )
    op.execute(
        """
        CREATE TYPE scope_level_enum AS ENUM (
            'ORG', 'DEPT'
        )
        """
    )
    # New for user_role_assignments junction (DRY with user_org_dept_status)
    op.execute(
        """
        CREATE TYPE assignment_status_enum AS ENUM (
            'ACTIVE', 'DEACTIVATED', 'DELETED'
        )
        """
    )

    # Permissions (base for RBAC slugs e.g. org:read)
    op.create_table(
        "permissions",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "status",
            PGEnum(
                "permission_status_enum",
                name="permission_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps DRY
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_permissions_slug", "permissions", ["slug"], unique=True)

    # System roles (SaaS-wide admins)
    op.create_table(
        "system_roles",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "is_system_wide", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column(
            "status",
            PGEnum(
                "system_role_status_enum",
                name="system_role_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_system_roles_slug", "system_roles", ["slug"], unique=True)

    # App roles (ORG/DEPT scoped)
    op.create_table(
        "app_roles",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "scope_level",
            PGEnum(
                "scope_level_enum",
                name="scope_level_enum",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            PGEnum(
                "app_role_status_enum",
                name="app_role_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_app_roles_slug", "app_roles", ["slug"], unique=True)

    # Role permissions junction (polymorphic role_type + role_id)
    op.create_table(
        "role_permissions",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column(
            "role_type",
            PGEnum(
                "role_type_enum",
                name="role_type_enum",
                create_type=False,
            ),
            nullable=False,
        ),
        # role_id polymorphic (SYSTEM=system_roles.id or APP=app_roles.id); no FK
        # constraint (app-enforced per model doc to avoid dual-target issues)
        sa.Column("role_id", sa.UUID(), nullable=False),
        sa.Column(
            "permission_id", sa.UUID(), nullable=False
        ),
        # Constraints/FKs
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["permission_id"], ["permissions.id"], ondelete="CASCADE"
        ),
        # Unique per spec
        sa.UniqueConstraint(
            "role_type", "role_id", "permission_id",
            name="uq_role_type_role_permission"
        ),
    )
    # Indexes for perf/join queries
    op.create_index("ix_role_permissions_role_id", "role_permissions", ["role_id"])
    op.create_index(
        "ix_role_permissions_permission_id", "role_permissions", ["permission_id"]
    )
    # Composite idx for unique queries (auto from UniqueConstraint but explicit)
    op.create_index(
        "ix_role_permissions_role_lookup",
        "role_permissions",
        ["role_type", "role_id"],
    )

    # UserRoleAssignment junction (prod-grade RBAC mapping; after RBAC roles/perms for FKs)
    # Supports: system admins, org/dept scoped perms, standard users (null scopes),
    # multi-roles/tenancy. Polymorphic role_id; DRY with user_org_departments.
    # Unique constraint allows standard/no-org users per req.
    # Default 'standard_user' APP role seedable here (future).
    op.create_table(
        "user_role_assignments",
        sa.Column(
            "id", sa.UUID(), primary_key=True, nullable=False,
            server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column(
            "role_type",
            PGEnum(
                "role_type_enum",  # reused from RBAC
                name="role_type_enum",
                create_type=False,
            ),
            nullable=False,
        ),
        # Polymorphic role_id (no FK; app-enforced)
        sa.Column("role_id", sa.UUID(), nullable=False),
        # Nullable scopes for org/dept APP roles (null=system/standard)
        sa.Column("org_id", sa.UUID(), nullable=True),
        sa.Column("dept_id", sa.UUID(), nullable=True),
        sa.Column(
            "status",
            PGEnum(
                "assignment_status_enum",
                name="assignment_status_enum",
                create_type=False,
            ),
            nullable=False,
            server_default="ACTIVE",
        ),
        # Timestamps (DRY)
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints/FKs
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["dept_id"], ["departments.id"], ondelete="SET NULL"
        ),
        # Constraints/FKs only (no UniqueConstraint: composite NULLs treated as
        # distinct in PG, would allow dupe standard users/null scopes per req cases).
        # Partial unique indexes below enforce uniqueness DRY.
    )
    # Partial unique indexes for NULL handling (PG best practice/industry std for
    # nullable composites; e.g., unique standard user vs. scoped org/dept perms).
    # - Standard/system (null scopes): one per user/role_type/role_id
    # - Scoped: full unique for org/dept cases
    # (replaces uq_user_role_scope_assignment; avoids dupes while supporting req)
    # Common for RBAC lookups/JWT claims; perf + integrity.
    op.create_index(
        "uq_user_role_standard_assignment",
        "user_role_assignments",
        ["user_id", "role_type", "role_id"],
        unique=True,
        postgresql_where=sa.text("org_id IS NULL AND dept_id IS NULL"),
    )
    op.create_index(
        "uq_user_role_scoped_assignment",
        "user_role_assignments",
        ["user_id", "role_type", "role_id", "org_id", "dept_id"],
        unique=True,
        postgresql_where=sa.text("org_id IS NOT NULL OR dept_id IS NOT NULL"),
    )
    # Additional query indexes (perf)
    op.create_index(
        "ix_user_role_assignments_user_id", "user_role_assignments", ["user_id"]
    )
    op.create_index(
        "ix_user_role_assignments_role_id", "user_role_assignments", ["role_id"]
    )
    op.create_index(
        "ix_user_role_assignments_org_id", "user_role_assignments", ["org_id"]
    )
    op.create_index(
        "ix_user_role_assignments_dept_id", "user_role_assignments", ["dept_id"]
    )

    # ── Refresh tokens (server-side hashed store for JWT rotation) ────────
    # Stores SHA-256 hashes of issued refresh tokens (raw token never persisted).
    # revoked_at IS NOT NULL => consumed/rotated/invalidated.
    # Cascade-delete via users.id FK keeps table clean on user removal.
    op.create_table(
        "refresh_tokens",
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("user_id", sa.UUID(), nullable=False),
        # SHA-256 hex digest (64 chars); unique so duplicate issuance is impossible
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_refresh_tokens_token_hash", "refresh_tokens", ["token_hash"], unique=True
    )
    op.create_index("ix_refresh_tokens_user_id", "refresh_tokens", ["user_id"])
    op.create_index(
        "ix_refresh_tokens_user_id_revoked",
        "refresh_tokens",
        ["user_id", "revoked_at"],
    )


def downgrade() -> None:
    """Downgrade: drop dependent tables first (full chain: user_role_assignments +
    RBAC junction/roles/perms + submissions/user_orgs/evals/positions/roles/depts/orgs
    -> assessments/oauth/users), then enums.

    Order critical for FKs (multi-tenancy chain + RBAC/user_assignment FKs; polymorphic
    role_id has no FK so safe). Continue in single migration per task (no prod DB).
    Supports RBAC mapping for system/org/dept/standard users.
    Surfaced missing UserOrganization assoc included.
    pgcrypto left intact.
    """
    # Drop children first (reverse create order; user_assignments refs users/RBAC/org/dept)
    # refresh_tokens first (FK to users; no other deps)
    op.drop_table("refresh_tokens")
    # Drop partial unique indexes explicitly (PG; before table)
    op.execute("DROP INDEX IF EXISTS uq_user_role_standard_assignment")
    op.execute("DROP INDEX IF EXISTS uq_user_role_scoped_assignment")
    op.drop_table("user_role_assignments")
    # RBAC first due to perms FK
    # role_permissions (FK to permissions) before roles/perms
    op.drop_table("role_permissions")
    op.drop_table("app_roles")
    op.drop_table("system_roles")
    op.drop_table("permissions")
    # Existing chain (assessments before roles: assessments.role_id FK)
    op.drop_table("user_org_departments")
    # Reverse scenario changes (drop table + restore columns/constraint)
    op.drop_table("scenarios")
    # submissions/evals restore
    op.drop_column("submissions", "scenario_id")
    op.drop_constraint("uq_user_assessment_scenario_submission", "submissions", type_="unique")
    op.create_unique_constraint("uq_user_assessment_submission", "submissions", ["user_id", "assessment_id"])
    op.drop_column("evaluations", "submission_id")
    op.drop_table("submissions")
    op.drop_table("evaluations")
    op.drop_table("assessments")
    op.drop_table("positions")
    op.drop_table("roles")
    op.drop_table("departments")
    op.drop_table("organizations")
    op.drop_table("oauth_accounts")
    # Core
    op.drop_table("users")

    # Drop enum types (PG-specific; IF EXISTS idempotent)
    # RBAC + assignment enums first (no deps)
    op.execute("DROP TYPE IF EXISTS system_role_status_enum")
    op.execute("DROP TYPE IF EXISTS app_role_status_enum")
    op.execute("DROP TYPE IF EXISTS permission_status_enum")
    op.execute("DROP TYPE IF EXISTS role_type_enum")
    op.execute("DROP TYPE IF EXISTS scope_level_enum")
    op.execute("DROP TYPE IF EXISTS assignment_status_enum")
    # Existing enums
    op.execute("DROP TYPE IF EXISTS gender_enum")
    op.execute("DROP TYPE IF EXISTS user_status_enum")
    op.execute("DROP TYPE IF EXISTS user_role_enum")
    op.execute("DROP TYPE IF EXISTS oauth_provider_enum")
    op.execute("DROP TYPE IF EXISTS assessment_status_enum")
    op.execute("DROP TYPE IF EXISTS submission_status_enum")
    op.execute("DROP TYPE IF EXISTS organization_status_enum")
    op.execute("DROP TYPE IF EXISTS department_status_enum")
    op.execute("DROP TYPE IF EXISTS role_status_enum")
    op.execute("DROP TYPE IF EXISTS position_status_enum")
    op.execute("DROP TYPE IF EXISTS scenario_status_enum")
    op.execute("DROP TYPE IF EXISTS scenario_tool_enum")
    op.execute("DROP TYPE IF EXISTS user_org_dept_status_enum")
    op.execute("DROP TYPE IF EXISTS org_role_enum")
