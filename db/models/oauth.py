"""OAuthAccount model - dedicated table for multi-provider SSO/OAuth support.

Separated from User per requirements: one User can have multiple OAuth providers
(e.g., GitHub, GitLab, LinkedIn) active simultaneously for flexible auth.

FastAPI/SQLAlchemy 2026 best practices (DRY, modular, model-heavy SaaS):
- Dedicated file for auth-related entity (scalability: future providers, tokens).
- 1:N relationship (User.oauth_accounts -> OAuthAccount).
- Enum for providers (extensible, type-safe).
- FK with cascade for tenant integrity; unique constraint per (user, provider).
- Tokens: store minimally (access/refresh); encrypt/hydrate via auth service (Authlib/passlib).
- No PII in tokens; ties to User for multi-tenancy (orgs/assessments).
- Alembic-ready; indexes for lookups.

Future: oauth_providers in api/schemas.py, auth routers in api/routes/.

Links to impact/providers/* (e.g., GitHub adapter).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class OAuthProvider(str, Enum):
    """Supported OAuth providers - extensible for SaaS growth.

    Matches Authlib providers; ties to impact/adapters/providers for GitHub etc.
    """

    GITHUB = "github"
    GITLAB = "gitlab"
    LINKEDIN = "linkedin"
    GOOGLE = "google"  # Common for SSO
    MICROSOFT = "microsoft"  # Future enterprise
    # Add others as needed; DB enum auto-extends on migration


class OAuthAccount(Base):
    """OAuth credentials/account for a User.

    Allows multiple providers per User (e.g., GitHub + GitLab login).
    DRY: centralizes auth tokens; User model stays lean (core profile only).
    """

    __tablename__ = "oauth_accounts"

    # PK
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="UUIDv4 for secure ref to OAuth record",
    )

    # FK to User (1:N; cascade delete for data integrity)
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="Links to owning User - enables multi-provider auth",
    )

    # Provider details
    provider: Mapped[OAuthProvider] = mapped_column(
        # Native PG enum for integrity (extensible)
        # SAEnum alias avoids name clash; matches User model pattern
        SAEnum(
            OAuthProvider,
            name="oauth_provider_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        doc="Provider slug (e.g., 'github')",
    )
    provider_user_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="External ID from provider (e.g., GitHub node_id)",
    )

    # Token storage (minimal; hydrate via Authlib; never plain in logs)
    # TODO: Encrypt at rest (e.g., via SQLAlchemy hooks or Fernet) for prod compliance
    access_token: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, doc="OAuth access token (short-lived)"
    )
    refresh_token: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, doc="Refresh for long-lived sessions"
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Token expiry - for auto-refresh",
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationship back to User (lazy-loaded; DRY with User side)
    user: Mapped["User"] = relationship(
        "User", back_populates="oauth_accounts"
    )

    # Table constraints (DB-enforced uniqueness)
    __table_args__ = (
        UniqueConstraint("user_id", "provider", name="uq_user_provider"),
        # Unique per-provider ID to prevent dup accounts
        UniqueConstraint("provider", "provider_user_id", name="uq_provider_external_id"),
    )

    def __repr__(self) -> str:
        """Safe repr (no token leakage)."""
        return (
            f"<OAuthAccount(id={self.id}, user_id={self.user_id}, "
            f"provider={self.provider})>"
        )
