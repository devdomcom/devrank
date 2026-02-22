"""Auth routes for JWT login, token refresh, and /me profile (app-level).

Email/password login (passlib bcrypt verify). /me returns AuthContext with
RBAC roles/perms. Mounted at /api/v1/auth; protected routes use Depends.

Token lifecycle:
  POST /auth/login   → issues access_token (JWT, short-lived) +
                        refresh_token (opaque, long-lived, stored hashed in DB)
  POST /auth/refresh → validates refresh_token, rotates it (old revoked, new
                        issued), returns a fresh token pair
  GET  /auth/me      → returns current user context from access_token
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_current_user, get_db
from api.auth.schemas import AuthContext, LoginRequest, RefreshRequest, TokenPair
from api.auth.security import (
    create_access_token,
    create_refresh_token,
    rotate_refresh_token,
)
from api.exceptions import AuthenticationError
from db.models import User

router = APIRouter(prefix="/auth", tags=["auth"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def authenticate_user(db: Session, email: str, password: str) -> User | None:
    """Verify creds against User.hashed_password."""
    user = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if not user or not user.hashed_password:
        return None
    if not pwd_context.verify(password, user.hashed_password):
        return None
    return user


@router.post(
    "/login",
    response_model=TokenPair,
    summary="Login with email and password",
)
def login(
    body: LoginRequest,
    db: Annotated[Session, Depends(get_db)],
) -> TokenPair:
    """Authenticate with email/password; returns a short-lived access token and
    a long-lived refresh token.

    The access token is a signed JWT — include it as
    ``Authorization: Bearer <access_token>`` on every subsequent API call.

    The refresh token is an opaque secret stored server-side (as a SHA-256
    hash).  Use it with ``POST /auth/refresh`` to obtain a new token pair
    before the access token expires.  Each refresh token is single-use:
    it is revoked on first use and a replacement is issued atomically.
    """
    user = authenticate_user(db, body.email, body.password)
    if not user:
        raise AuthenticationError("Incorrect email or password")

    access_token = create_access_token(data={"sub": str(user.id)})
    # create_refresh_token flushes the new row; commit makes both tokens durable.
    raw_refresh = create_refresh_token(user.id, db)
    db.commit()

    return TokenPair(access_token=access_token, refresh_token=raw_refresh)


@router.post(
    "/refresh",
    response_model=TokenPair,
    summary="Exchange a refresh token for a new token pair",
)
def refresh(
    body: RefreshRequest,
    db: Annotated[Session, Depends(get_db)],
) -> TokenPair:
    """Rotate a refresh token and issue a fresh access + refresh token pair.

    The supplied ``refresh_token`` is validated against the DB (must exist,
    not expired, not previously used).  On success:
    - The old refresh token row is marked ``revoked_at = now()``.
    - A new refresh token is generated and persisted (hash only).
    - A new access token (JWT) is issued for the same user.

    Both DB mutations and the response are committed atomically.

    **Replay detection**: if a revoked token is presented again (possible
    token theft), *all* active refresh tokens for that user are immediately
    invalidated and a 401 is returned.
    """
    new_raw_refresh, user_id = rotate_refresh_token(body.refresh_token, db)
    access_token = create_access_token(data={"sub": str(user_id)})
    db.commit()

    return TokenPair(access_token=access_token, refresh_token=new_raw_refresh)


@router.get("/me", response_model=AuthContext, summary="Get current user context")
def read_users_me(
    current: AuthContext = Depends(get_current_user),
) -> AuthContext:
    """Returns authenticated user's info, roles, and permissions."""
    return current
