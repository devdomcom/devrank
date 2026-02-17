"""Add core users + oauth_accounts + assessments tables for multi-tenancy SaaS.

Revision ID: ad29913efe04
Revises: 
Create Date: 2026-02-16 16:40:20.200201

All-in-one migration (per task: no real DB yet, no incremental needed).
Table rename: users (plural).

Changes:
- OAuth separation: oauth_accounts for multi-provider (GitHub/GitLab/etc.).
- is_self_evaluating REMOVED from users; dedicated assessments table/model instead
  (proposal-aligned: title/slug/role_id/created_by/status etc.; supports self-eval/org).

Covers:
- Enums (..., assessment_status) - native PG
- users (lean; is_self_evaluating dropped)
- oauth_accounts (1:N)
- assessments (FKs to users/roles, slug unique)
- Replaces sample table

DRY: dedicated models (db/models/{user,oauth,assessment}.py); future Role/org.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ad29913efe04"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


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
            sa.Enum(
                "gender_enum",
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
            sa.Enum(
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
            sa.Enum(
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
            sa.Enum(
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
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
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
            "status",
            sa.Enum(
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
        sa.Column("version", sa.Integer(), nullable=False, server_default=text("1")),
        sa.Column("creator", sa.UUID(), nullable=True),
        sa.Column("org_id", sa.UUID(), nullable=True),  # NULL=global
        sa.Column("global_role", sa.Boolean(), nullable=False, server_default=text("false")),
        sa.Column(
            "status",
            sa.Enum(
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
            sa.Enum(
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
            sa.Enum(
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
            sa.Enum(
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


def downgrade() -> None:
    """Downgrade: drop dependent tables first (full chain: submissions/user_orgs/evals/positions/roles/depts/orgs -> assessments/oauth/users), then enums.

    Order critical for FKs (multi-tenancy chain: orgs/depts/roles/positions, assoc, submissions, etc.).
    Surfaced missing UserOrganization assoc included.
    pgcrypto left intact.
    """
    # Drop children first (reverse create order)
    op.drop_table("submissions")
    op.drop_table("user_organizations")
    op.drop_table("evaluations")
    op.drop_table("positions")
    op.drop_table("roles")
    op.drop_table("departments")
    op.drop_table("organizations")
    # Prior children
    op.drop_table("assessments")
    op.drop_table("oauth_accounts")
    # Core
    op.drop_table("users")

    # Drop enum types (PG-specific)
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
    op.execute("DROP TYPE IF EXISTS org_role_enum")
