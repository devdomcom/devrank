"""JWT security utils for RBAC auth (app-level).

Handles:
- Access token creation/decoding (python-jose; HS256 per config).
- Refresh token generation, hashing, DB persistence, and rotation.

Refresh token design:
  Raw token  — secrets.token_urlsafe(32) — returned to client once, never stored.
  Token hash — SHA-256 hex digest        — stored in DB; used for all lookups.
  Rotation   — on each /auth/refresh the old row is revoked and a new
               raw+hash pair is issued atomically in the same transaction.
"""
from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.exceptions import AuthenticationError
from config import settings


class TokenData(BaseModel):
    """Claims payload for JWT."""

    user_id: str | None = None


# ── Access token ─────────────────────────────────────────────────────────────


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=settings.access_token_expire_minutes
        )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> TokenData:
    """Decode/validate JWT; raise AuthenticationError (401) on failure."""
    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.jwt_algorithm]
        )
        user_id: str | None = payload.get("sub")
        if user_id is None:
            raise AuthenticationError("Invalid token payload")
        return TokenData(user_id=user_id)
    except JWTError as e:
        raise AuthenticationError("Could not validate credentials") from e


# ── Refresh token ─────────────────────────────────────────────────────────────


def _hash_token(raw: str) -> str:
    """Return the SHA-256 hex digest of a raw token string."""
    return hashlib.sha256(raw.encode()).hexdigest()


def create_refresh_token(user_id: uuid.UUID, db: Session) -> str:
    """Generate a new refresh token, persist its hash, and return the raw value.

    The raw token is a 32-byte URL-safe random string (256 bits of entropy).
    Only the SHA-256 hash is written to the DB — the raw value is returned
    once and must be forwarded to the client immediately.

    ``expires_at`` is derived from ``settings.refresh_token_expire_days``.
    """
    # Import here to avoid circular imports at module load time
    from db.models.refresh_token import RefreshToken

    raw = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw)
    expires_at = datetime.now(timezone.utc) + timedelta(
        days=settings.refresh_token_expire_days
    )
    record = RefreshToken(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=expires_at,
    )
    db.add(record)
    # Flush so the row is visible within the transaction but not yet committed;
    # the caller commits after also creating the access token (atomic pair).
    db.flush()
    return raw


def rotate_refresh_token(raw_token: str, db: Session) -> tuple[str, uuid.UUID]:
    """Validate a refresh token, revoke it, and issue a replacement.

    Returns ``(new_raw_token, user_id)`` on success.

    Raises ``AuthenticationError`` (401) for:
    - Token not found (never issued or already pruned)
    - Token expired
    - Token already revoked (replay attack — possible token theft)

    Rotation is atomic within the caller's transaction: the old row is marked
    revoked and the new row is flushed before the caller commits.
    """
    from sqlalchemy import select
    from db.models.refresh_token import RefreshToken

    token_hash = _hash_token(raw_token)
    record: RefreshToken | None = db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    ).scalar_one_or_none()

    if record is None:
        raise AuthenticationError("Invalid refresh token")

    now = datetime.now(timezone.utc)

    if record.expires_at.replace(tzinfo=timezone.utc) < now:
        raise AuthenticationError("Refresh token has expired")

    if record.revoked_at is not None:
        # Token was already used — possible replay/theft.  Revoke all tokens
        # for this user as a precaution (security best practice).
        _revoke_all_for_user(record.user_id, db)
        raise AuthenticationError(
            "Refresh token has already been used; all sessions invalidated"
        )

    # Revoke the consumed token
    record.revoked_at = now
    db.flush()

    # Issue replacement
    new_raw = create_refresh_token(record.user_id, db)
    return new_raw, record.user_id


def _revoke_all_for_user(user_id: uuid.UUID, db: Session) -> None:
    """Revoke all active refresh tokens for a user (security event response)."""
    from sqlalchemy import select, update
    from db.models.refresh_token import RefreshToken

    now = datetime.now(timezone.utc)
    db.execute(
        update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.revoked_at.is_(None),
        )
        .values(revoked_at=now)
    )
