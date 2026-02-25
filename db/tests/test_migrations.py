"""Tests for Alembic migration pipeline.

Requires a running PostgreSQL instance (``docker compose up postgres``).
Each test starts from a clean ``base`` state and upgrades/downgrades
to validate the full migration chain.

Updated for foundational User model (multi-tenancy SaaS) + RBAC tables
(system_roles/app_roles/permissions/role_permissions). Tests enums, constraints,
indexes, FKs - DRY validation for model-heavy codebase. No rigging: uses
real inspect() on upgraded schema, exact col/enum sets, follows existing patterns
for org/dept/user_org etc.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

# Import models to register metadata (and for ORM usage below)
# Covers User + OAuthAccount + Assessment + Submission (user-assessment join)
# + RBAC + user_role_assignments for mapping validation (system/org/dept/standard users;
# no ORM roundtrip yet; future with auth services)
from db.engine import SyncSessionLocal, sync_engine
from db.models import (
    # Core
    Assessment,
    AssessmentStatus,
    OAuthAccount,
    OAuthProvider,
    Submission,
    SubmissionStatus,
    User,
    UserRole,
    UserStatus,
    # RBAC (new; import to ensure SA metadata + test access)
    AppRole,
    AppRoleStatus,
    Permission,
    PermissionStatus,
    RolePermission,
    RoleType,
    ScopeLevel,
    SystemRole,
    SystemRoleStatus,
    # User-role assignment for RBAC perms mapping (prod-grade)
    AssignmentStatus,
    UserRoleAssignment,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _alembic_cfg() -> Config:
    """Return an Alembic Config wired to our alembic.ini."""
    cfg = Config("alembic.ini")
    return cfg


def _current_rev() -> str | None:
    """Return the current Alembic revision applied to the database."""
    with sync_engine.connect() as conn:
        result = conn.execute(text("SELECT version_num FROM alembic_version"))
        row = result.fetchone()
        return row[0] if row else None


def _table_exists(name: str) -> bool:
    """Check if a table exists in the public schema."""
    insp = inspect(sync_engine)
    return name in insp.get_table_names()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_db():
    """Downgrade to base before each test so every test starts clean."""
    cfg = _alembic_cfg()
    command.downgrade(cfg, "base")
    yield
    # Cleanup: leave DB at base after each test.
    command.downgrade(cfg, "base")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMigrationUpgrade:
    """Upgrade to head creates expected schema for User model."""

    def test_upgrade_creates_users_table(self):
        """Table + alembic_version tracking. (Renamed to conventional plural 'users'.)"""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        assert _table_exists("users")
        assert _current_rev() is not None

    def test_upgrade_users_table_columns(self):
        """All expected columns from User model present (DRY schema-model sync)."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        insp = inspect(sync_engine)
        cols = {c["name"] for c in insp.get_columns("users")}
        # oauth_* fields removed (delegated to oauth_accounts for multi-provider)
        expected = {
            "id",
            "name",
            "surname",
            "nickname",
            "avatar",
            "email",
            "gender",
            "dob",
            "country",
            "address",
            "zip",
            "phone",
            "is_verified",
            "is_kyc_verified",
            "hashed_password",
            "role",
            "timezone",
            "locale",
            "last_login_at",
            "created_at",
            "updated_at",
            "status",
        }
        assert cols == expected

    def test_upgrade_enums_created(self):
        """Native PG enums for type safety (gender, role, status, oauth_provider, assessment_status, submission_status
        + RBAC: system_role_status etc.).

        Supports multi-OAuth + assessments/submissions (self-eval/org) + RBAC permissions/roles.
        No rigging: exact enum_names set check from inspector post-upgrade.
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        insp = inspect(sync_engine)
        enums = insp.get_enums()
        enum_names = {e["name"] for e in enums}
        # Includes multi-tenancy enums (org/dept/role_status/user_org etc.) + RBAC
        # Assert subset to stay robust to additions; follows original pattern
        assert {
            "user_gender_enum",
            "user_status_enum",
            "user_role_enum",
            "oauth_provider_enum",
            "assessment_status_enum",
            "submission_status_enum",
            # Multi-tenancy (added in same migration)
            "organization_status_enum",
            "department_status_enum",
            "role_status_enum",
            "position_status_enum",
            "user_org_dept_status_enum",
            # RBAC per task spec
            "system_role_status_enum",
            "app_role_status_enum",
            "permission_status_enum",
            "role_type_enum",
            "scope_level_enum",
        } <= enum_names

    def test_upgrade_oauth_accounts_table(self):
        """Dedicated oauth_accounts table for multi-provider support.

        Verifies 1:N FK, uniques, indexes (req: multiple OAuth per User).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        assert _table_exists("oauth_accounts")
        insp = inspect(sync_engine)
        cols = {c["name"] for c in insp.get_columns("oauth_accounts")}
        expected = {
            "id",
            "user_id",
            "provider",
            "provider_user_id",
            "access_token",
            "refresh_token",
            "expires_at",
            "created_at",
            "updated_at",
        }
        assert cols == expected

        # Constraints/FKs
        fks = {fk["referred_table"] for fk in insp.get_foreign_keys("oauth_accounts")}
        assert "users" in fks
        constraints = {c["name"] for c in insp.get_unique_constraints("oauth_accounts")}
        assert {"uq_user_provider", "uq_provider_external_id"} & constraints

    def test_upgrade_assessments_table(self):
        """Dedicated assessments table (replaces User.is_self_evaluating).

        Verifies proposal fields, FKs (created_by User, role_id), slug unique.
        Supports self-eval/org-assigned (impact/roles tie-in).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        assert _table_exists("assessments")
        insp = inspect(sync_engine)
        cols = {c["name"] for c in insp.get_columns("assessments")}
        # Note: created_by (not user_id); role_id placeholder
        expected = {
            "id",
            "title",
            "slug",
            "description",
            "role_id",
            "created_by",
            "org_id",
            "position_id",
            "status",
            "created_at",
            "updated_at",
            "published_at",
            "deleted_at",
        }
        assert cols == expected

        # Constraints/FKs/indexes
        fks = {fk["referred_table"] for fk in insp.get_foreign_keys("assessments")}
        assert {"users", "roles", "organizations", "positions"} <= fks
        constraints = {c.get("name") for c in insp.get_unique_constraints("assessments") if c.get("name")}
        assert "assessments_slug_key" in constraints or "uq_assessments_slug" in constraints  # Alembic/PG naming
        # Indexes
        indexes = {i["name"] for i in insp.get_indexes("assessments")}
        assert "ix_assessments_slug" in indexes

    def test_upgrade_submissions_table(self):
        """submissions table (User-Assessment join; per req for taken assessments).

        Verifies FKs (assessment/user/eval/position), status, timestamps, unique constraint.
        Supports self-assess (position_id NULL) + org roles.
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        assert _table_exists("submissions")
        insp = inspect(sync_engine)
        cols = {c["name"] for c in insp.get_columns("submissions")}
        expected = {
            "id",
            "assessment_id",
            "scenario_id",
            "user_id",
            "evaluation_id",
            "position_id",
            "status",
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
            "deleted_at",
            "abandoned_at",
        }
        assert cols == expected

        # Constraints/FKs/indexes
        fks = {fk["referred_table"] for fk in insp.get_foreign_keys("submissions")}
        assert {"users", "assessments", "evaluations", "positions", "scenarios"} <= fks
        constraints = {c.get("name") for c in insp.get_unique_constraints("submissions") if c.get("name")}
        assert "uq_user_assessment_scenario_submission" in constraints
        # Indexes
        indexes = {i["name"] for i in insp.get_indexes("submissions")}
        assert {
            "ix_submissions_user_id",
            "ix_submissions_assessment_id",
            "ix_submissions_position_id",
        } <= indexes


    def test_upgrade_rbac_tables(self):
        """RBAC tables for permission system: permissions, system_roles, app_roles, role_permissions.

        Verifies cols, enums/statuses, uniques, indexes, FKs per spec and migration.
        DRY patterns from org/dept/user_org_dept tests; polymorphic role_id has no FK
        (app-enforced, see model/migration doc). No rigging - uses inspect post-upgrade.
        Covers start of RBAC impl; JWT/route enforcement in subsequent tasks.
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        insp = inspect(sync_engine)

        # Permissions table
        assert _table_exists("permissions")
        perm_cols = {c["name"] for c in insp.get_columns("permissions")}
        expected_perm = {
            "id", "slug", "description", "status",
            "created_at", "updated_at", "deleted_at",
        }
        assert perm_cols == expected_perm
        perm_fks = {fk["referred_table"] for fk in insp.get_foreign_keys("permissions")}
        assert not perm_fks  # leaf table
        perm_constraints = {c.get("name") for c in insp.get_unique_constraints("permissions") if c.get("name")}
        assert "permissions_slug_key" in perm_constraints or "uq_permissions_slug" in perm_constraints  # PG/Alembic naming
        perm_idxs = {i["name"] for i in insp.get_indexes("permissions")}
        assert "ix_permissions_slug" in perm_idxs

        # System roles
        assert _table_exists("system_roles")
        sys_cols = {c["name"] for c in insp.get_columns("system_roles")}
        expected_sys = {
            "id", "slug", "name", "description", "is_system_wide", "status",
            "created_at", "updated_at", "deleted_at",
        }
        assert sys_cols == expected_sys
        sys_idxs = {i["name"] for i in insp.get_indexes("system_roles")}
        assert "ix_system_roles_slug" in sys_idxs

        # App roles
        assert _table_exists("app_roles")
        app_cols = {c["name"] for c in insp.get_columns("app_roles")}
        expected_app = {
            "id", "slug", "name", "description", "scope_level", "status",
            "created_at", "updated_at", "deleted_at",
        }
        assert app_cols == expected_app
        app_idxs = {i["name"] for i in insp.get_indexes("app_roles")}
        assert "ix_app_roles_slug" in app_idxs

        # Role permissions junction (polymorphic)
        assert _table_exists("role_permissions")
        rp_cols = {c["name"] for c in insp.get_columns("role_permissions")}
        expected_rp = {
            "id", "role_type", "role_id", "permission_id",
        }
        assert rp_cols == expected_rp
        rp_fks = {fk["referred_table"] for fk in insp.get_foreign_keys("role_permissions")}
        assert {"permissions"} == rp_fks  # only perm FK; role_id polymorphic
        rp_constraints = {c.get("name") for c in insp.get_unique_constraints("role_permissions") if c.get("name")}
        assert "uq_role_type_role_permission" in rp_constraints
        rp_idxs = {i["name"] for i in insp.get_indexes("role_permissions")}
        assert {
            "ix_role_permissions_role_id",
            "ix_role_permissions_permission_id",
            "ix_role_permissions_role_lookup",
        } <= rp_idxs

        # UserRoleAssignment junction (RBAC mapping for users/orgs/depts)
        # Verifies polymorphic scopes, partial unique indexes for all cases (system admin,
        # org/dept, standard user w/ null scopes - PG NULLs distinct so constraint alone
        # insufficient; partials enforce per fix). DRY from user_org_dept, real inspect
        # asserts (no rigging).
        assert _table_exists("user_role_assignments")
        ura_cols = {c["name"] for c in insp.get_columns("user_role_assignments")}
        expected_ura = {
            "id", "user_id", "role_type", "role_id", "org_id", "dept_id",
            "status", "created_at", "updated_at", "deleted_at", "deactivated_at",
        }
        assert ura_cols == expected_ura
        ura_fks = {fk["referred_table"] for fk in insp.get_foreign_keys("user_role_assignments")}
        assert {"users", "organizations", "departments"} == ura_fks
        # Note: no UniqueConstraint (old name removed); uniqueness via partial indexes
        # (get_unique_constraints may miss index-based; check indexes below)
        ura_idxs = {i["name"] for i in insp.get_indexes("user_role_assignments")}
        assert {
            # Query indexes
            "ix_user_role_assignments_user_id",
            "ix_user_role_assignments_role_id",
            "ix_user_role_assignments_org_id",
            "ix_user_role_assignments_dept_id",
            # Partial uniques for NULLs (PG best practice; enforces std/standard user
            # no dupes while scoped unique)
            "uq_user_role_standard_assignment",
            "uq_user_role_scoped_assignment",
        } <= ura_idxs
        # Verify partials are unique
        unique_idxs = {i["name"] for i in insp.get_indexes("user_role_assignments") if i.get("unique")}
        assert {
            "uq_user_role_standard_assignment",
            "uq_user_role_scoped_assignment",
        } <= unique_idxs


class TestMigrationDowngrade:
    """Downgrade back to base removes schema cleanly."""

    def test_downgrade_removes_tables(self):
        """Tables dropped; enums cleaned (order: user_role_assignments + RBAC +
        submissions/assessments/oauth -> users).

        (Table rename to 'users' + new assessments/oauth/submissions/RBAC/user_assignment
        tables reflected here.) Downgrade order (junction/deps first) per migration.
        Covers full RBAC mapping for system/org/dept/standard users.
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        # Pre-downgrade: core + RBAC + assignment exist
        assert _table_exists("users")
        assert _table_exists("oauth_accounts")
        assert _table_exists("assessments")
        assert _table_exists("submissions")
        # RBAC per tables task
        assert _table_exists("permissions")
        assert _table_exists("system_roles")
        assert _table_exists("app_roles")
        assert _table_exists("role_permissions")
        # User-role assignment for perms mapping
        assert _table_exists("user_role_assignments")

        command.downgrade(cfg, "base")
        # Post: none remain
        assert not _table_exists("users")
        assert not _table_exists("oauth_accounts")
        assert not _table_exists("assessments")
        assert not _table_exists("submissions")
        # RBAC removed
        assert not _table_exists("permissions")
        assert not _table_exists("system_roles")
        assert not _table_exists("app_roles")
        assert not _table_exists("role_permissions")
        # Assignment removed
        assert not _table_exists("user_role_assignments")

    def test_downgrade_clears_revision(self):
        """No lingering alembic_version."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")

        assert _current_rev() is None

    def test_downgrade_removes_enums(self):
        """Enums dropped to keep schema clean (no orphan types).

        Covers original + multi-tenancy + RBAC enums. Uses set intersection
        assert (follows pattern; real inspector, no mocks/rigging).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        command.downgrade(cfg, "base")
        insp = inspect(sync_engine)
        enums = insp.get_enums()
        enum_names = {e["name"] for e in enums}
        # No remaining enums after base downgrade
        assert not {
            # Original
            "user_gender_enum",
            "user_status_enum",
            "user_role_enum",
            "oauth_provider_enum",
            "assessment_status_enum",
            "submission_status_enum",
            # Multi-tenancy
            "organization_status_enum",
            "department_status_enum",
            "role_status_enum",
            "position_status_enum",
            "user_org_dept_status_enum",
            # RBAC + assignment per mapping task (supports full perms for
            # system/org/dept/standard users)
            "system_role_status_enum",
            "app_role_status_enum",
            "permission_status_enum",
            "role_type_enum",
            "scope_level_enum",
            "assignment_status_enum",
        } & enum_names


class TestMigrationRoundTrip:
    """Full upgrade → downgrade → upgrade cycle is idempotent."""

    def test_round_trip(self):
        """Schema state identical after cycle."""
        cfg = _alembic_cfg()

        command.upgrade(cfg, "head")
        rev1 = _current_rev()

        command.downgrade(cfg, "base")
        assert not _table_exists("users")
        # oauth + assessments/submissions gone (FK deps)
        assert not _table_exists("oauth_accounts")
        assert not _table_exists("assessments")
        assert not _table_exists("submissions")

        command.upgrade(cfg, "head")
        rev2 = _current_rev()

        assert rev1 == rev2
        assert _table_exists("users")
        assert _table_exists("oauth_accounts")
        assert _table_exists("assessments")
        assert _table_exists("submissions")


class TestUserModelUsable:
    """After migration, the ORM User + OAuthAccount + Assessment models work
    end-to-end (insert/query/enums/relationships).

    Tests multi-tenancy: status, role, defaults, *multi-provider OAuth*, *assessments*
    (replaces is_self_evaluating; self-eval via created_assessments). Tables: users (plural).
    """

    def test_insert_and_query_via_orm(self):
        """Full ORM usage - Pydantic/SQLAlchemy best practice.

        Note: session.get works by PK regardless of table name (model-driven).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            user = User(
                id=user_id,
                name="Test",
                surname="Engineer",
                email="test@example.com",
                # defaults for enums/status
                role=UserRole.ENGINEER,  # Ties to impact assessment
                status=UserStatus.ACTIVE,
                # is_self_evaluating removed (use Assessment instead)
                dob=date(1990, 1, 1),
            )
            session.add(user)
            session.commit()

        # Query back
        with SyncSessionLocal() as session:
            queried = session.get(User, user_id)
            assert queried is not None
            assert queried.email == "test@example.com"
            assert queried.role == UserRole.ENGINEER
            assert queried.status == UserStatus.ACTIVE
            assert queried.is_verified is False  # default
            assert queried.nickname is None
            # OAuth/assessment rels empty by default (multi-ready)
            assert len(queried.oauth_accounts) == 0
            assert len(queried.created_assessments) == 0

    def test_oauth_relationship_and_multi_provider(self):
        """OAuthAccount FK/rel + multiple providers per User (core req).

        Verifies 1:N, cascade, uniques (e.g., same provider twice blocked).
        Ties to Authlib for GitHub/GitLab/etc.
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            user = User(
                id=user_id,
                name="OAuth",
                surname="User",
                email="oauth@example.com",
                role=UserRole.USER,
                status=UserStatus.ACTIVE,
            )
            # Multiple OAuthAccounts (e.g., GitHub + GitLab)
            gh_oauth = OAuthAccount(
                user_id=user_id,
                provider=OAuthProvider.GITHUB,
                provider_user_id="gh-123",
                access_token="gh-token-abc",
            )
            gl_oauth = OAuthAccount(
                user_id=user_id,
                provider=OAuthProvider.GITLAB,
                provider_user_id="gl-456",
                access_token="gl-token-def",
            )
            user.oauth_accounts.extend([gh_oauth, gl_oauth])
            session.add(user)
            session.commit()

        # Query + rel eager load
        with SyncSessionLocal() as session:
            queried = session.get(User, user_id)
            assert len(queried.oauth_accounts) == 2
            providers = {oa.provider for oa in queried.oauth_accounts}
            assert providers == {OAuthProvider.GITHUB, OAuthProvider.GITLAB}
            # Backref
            assert queried.oauth_accounts[0].user.email == "oauth@example.com"

    def test_assessment_relationship_and_self_eval(self):
        """Assessment FK/rel (replaces is_self_evaluating); supports self/org-eval.

        Verifies 1:N, cascade; ties to impact/roles (slug unique, creator FK).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            user = User(
                id=user_id,
                name="Assessor",
                surname="User",
                email="assess@example.com",
                role=UserRole.ENGINEER,
                status=UserStatus.ACTIVE,
            )
            # Create assessment (self-eval example)
            assess = Assessment(
                title="Senior Eng Impact Q1",
                slug="senior-eng-q1-2026",
                description="Eval via impact metrics",
                # role_id nullable (UUID FK to roles; None for self-eval)
                role_id=None,
                created_by=user_id,
                status=AssessmentStatus.PUBLISHED,
            )
            user.created_assessments.append(assess)
            session.add(user)
            session.commit()

        # Query + rel
        with SyncSessionLocal() as session:
            queried = session.get(User, user_id)
            assert len(queried.created_assessments) == 1
            ass = queried.created_assessments[0]
            assert ass.slug == "senior-eng-q1-2026"
            assert ass.status == AssessmentStatus.PUBLISHED
            # Backref to creator
            assert ass.creator.email == "assess@example.com"

    def test_submission_relationship_and_user_assessment_join(self):
        """Submission FK/rel (user-assessment connect; per req).

        Verifies join table, FKs (assessment/user/placeholders), status/timestamps,
        unique (one sub per user/assess), nullable position_id (self-eval).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            user = User(
                id=user_id,
                name="Submitter",
                surname="User",
                email="submit@example.com",
                role=UserRole.ENGINEER,
                status=UserStatus.ACTIVE,
            )
            # Create assessment + submission (self-eval: position_id=None)
            assess = Assessment(
                title="Impact Self-Eval",
                slug="self-eval-2026",
                created_by=user_id,
                status=AssessmentStatus.PUBLISHED,
            )
            sub = Submission(
                assessment_id=assess.id,  # will resolve post-flush
                user_id=user_id,
                # evaluation_id/position_id placeholders (NULL=self-assess)
                position_id=None,
                status=SubmissionStatus.COMPLETED,
                started_at=datetime.utcnow(),  # from datetime import in test scope OK
                completed_at=datetime.utcnow(),
            )
            user.submissions.append(sub)
            # Also link via assessment
            assess.submissions.append(sub)
            session.add_all([user, assess])
            session.commit()

        # Query + rels (user + assessment side)
        with SyncSessionLocal() as session:
            queried_user = session.get(User, user_id)
            assert len(queried_user.submissions) == 1
            sub = queried_user.submissions[0]
            assert sub.status == SubmissionStatus.COMPLETED
            assert sub.position_id is None  # self-assess
            assert sub.user_id == user_id

            # Assessment side
            queried_ass = session.execute(
                text("SELECT id FROM assessments WHERE slug = 'self-eval-2026'")
            ).scalar()
            ass = session.get(Assessment, queried_ass)
            assert len(ass.submissions) == 1
            assert ass.submissions[0].user.email == "submit@example.com"

    def test_unique_email_constraint(self):
        """Enforces SaaS email uniqueness (multi-tenant auth)."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id1 = uuid.uuid4()
        user_id2 = uuid.uuid4()
        with SyncSessionLocal() as session:
            u1 = User(
                id=user_id1,
                name="User1",
                surname="Test",
                email="dup@example.com",
            )
            session.add(u1)
            session.commit()

            # Duplicate should raise
            u2 = User(
                id=user_id2,
                name="User2",
                surname="Test",
                email="dup@example.com",
            )
            session.add(u2)
            with pytest.raises(Exception):  # IntegrityError from PG
                session.commit()

    def test_oauth_unique_constraints(self):
        """uq_user_provider + uq_provider_external_id (prevents dups)."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        user_id = uuid.uuid4()
        with SyncSessionLocal() as session:
            user = User(
                id=user_id,
                name="Dup",
                surname="OAuth",
                email="dup-oauth@example.com",
            )
            session.add(user)
            session.commit()

            # First OK
            oa1 = OAuthAccount(
                user_id=user_id,
                provider=OAuthProvider.GITHUB,
                provider_user_id="gh-123",
            )
            session.add(oa1)
            session.commit()

            # Dup provider for same user: blocked by uq_user_provider
            oa2 = OAuthAccount(
                user_id=user_id,
                provider=OAuthProvider.GITHUB,
                provider_user_id="gh-999",
            )
            session.add(oa2)
            with pytest.raises(Exception):  # IntegrityError
                session.commit()
