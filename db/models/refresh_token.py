"""RefreshToken model — server-side refresh token store for JWT rotation.

Design:
- One row per issued refresh token (revoked rows kept for audit; pruned by
  a background job or on next login).
- ``token_hash``: SHA-256 of the raw token (never stored in plain text).
  The raw token is a URL-safe random secret (secrets.token_urlsafe) returned
  once to the client and never persisted.
- ``expires_at``: absolute UTC expiry (configurable via settings).
- ``revoked_at``: set when the token is consumed (rotation) or explicitly
  invalidated (logout / security event). A non-null value means invalid.
- ``user_id``: FK to users; cascade-delete keeps the table clean when a
  user is hard-deleted.

Token rotation (one-time-use):
  On each /auth/refresh call the old row is revoked and a new row + raw
  token are issued atomically.  Replay of a revoked token is detected
  immediately (revoked_at IS NOT NULL) and should trigger a security alert
  (possible token theft).
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class RefreshToken(Base):
    """Hashed refresh token tied to a user session."""

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # SHA-256 hex digest of the raw token (64 hex chars)
    token_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        unique=True,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    # Set on first (and only) use — marks token as consumed/rotated
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    # Relationship back to User (lazy load; not needed on hot paths)
    user: Mapped[User] = relationship("User", back_populates="refresh_tokens")

    __table_args__ = (
        # Fast lookup by hash on every refresh call
        Index("ix_refresh_tokens_token_hash", "token_hash", unique=True),
        # Prune queries: find all active tokens for a user
        Index("ix_refresh_tokens_user_id_revoked", "user_id", "revoked_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<RefreshToken(id={self.id}, user_id={self.user_id}, "
            f"expires_at={self.expires_at}, revoked={self.revoked_at is not None})>"
        )
