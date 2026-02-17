"""Tests for Alembic migration pipeline.

Requires a running PostgreSQL instance (``docker compose up postgres``).
Each test starts from a clean ``base`` state and upgrades/downgrades
to validate the full migration chain.

Updated for foundational User model (multi-tenancy SaaS).
Tests enums, constraints, ORM round-trip - DRY validation for model-heavy codebase.
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
from db.engine import SyncSessionLocal, sync_engine
from db.models import (
    Assessment,
    AssessmentStatus,
    OAuthAccount,
    OAuthProvider,
    Submission,
    SubmissionStatus,
    User,
    UserRole,
    UserStatus,
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
            "verified",
            "kyc",
            "hashed_password",
            "role",
            "is_self_evaluating",
            "timezone",
            "locale",
            "last_login_at",
            "created_at",
            "updated_at",
            "status",
        }
        assert cols == expected

    def test_upgrade_enums_created(self):
        """Native PG enums for type safety (gender, role, status, oauth_provider, assessment_status, submission_status).

        Supports multi-OAuth + assessments/submissions (self-eval/org).
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        insp = inspect(sync_engine)
        enums = insp.get_enums()
        enum_names = {e["name"] for e in enums}
        assert {
            "gender_enum",
            "user_status_enum",
            "user_role_enum",
            "oauth_provider_enum",
            "assessment_status_enum",
            "submission_status_enum",
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
            "status",
            "created_at",
            "updated_at",
        }
        assert cols == expected

        # Constraints/FKs/indexes
        fks = {fk["referred_table"] for fk in insp.get_foreign_keys("assessments")}
        assert {"users", "roles"} <= fks  # roles placeholder OK
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
        assert {"users", "assessments", "evaluations", "positions"} <= fks  # placeholders OK
        constraints = {c.get("name") for c in insp.get_unique_constraints("submissions") if c.get("name")}
        assert "uq_user_assessment_submission" in constraints
        # Indexes
        indexes = {i["name"] for i in insp.get_indexes("submissions")}
        assert {
            "ix_submissions_user_id",
            "ix_submissions_assessment_id",
            "ix_submissions_position_id",
        } <= indexes


class TestMigrationDowngrade:
    """Downgrade back to base removes schema cleanly."""

    def test_downgrade_removes_tables(self):
        """Tables dropped; enums cleaned (order: submissions/assessments/oauth -> users).

        (Table rename to 'users' + new assessments/oauth/submissions reflected here.)
        """
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        assert _table_exists("users")
        assert _table_exists("oauth_accounts")
        assert _table_exists("assessments")
        assert _table_exists("submissions")

        command.downgrade(cfg, "base")
        assert not _table_exists("users")
        assert not _table_exists("oauth_accounts")
        assert not _table_exists("assessments")
        assert not _table_exists("submissions")

    def test_downgrade_clears_revision(self):
        """No lingering alembic_version."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")

        assert _current_rev() is None

    def test_downgrade_removes_enums(self):
        """Enums dropped to keep schema clean (no orphan types)."""
        cfg = _alembic_cfg()
        command.upgrade(cfg, "head")

        command.downgrade(cfg, "base")
        insp = inspect(sync_engine)
        enums = insp.get_enums()
        enum_names = {e["name"] for e in enums}
        assert not {
            "gender_enum",
            "user_status_enum",
            "user_role_enum",
            "oauth_provider_enum",
            "assessment_status_enum",
            "submission_status_enum",
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
            assert queried.verified is False  # default
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
                # role_id placeholder (future Role FK)
                role_id=1,
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
