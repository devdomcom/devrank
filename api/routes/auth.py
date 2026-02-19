"""Auth routes for JWT login and /me profile (app-level).

Email/password login (passlib bcrypt verify). /me returns AuthContext with
RBAC roles/perms. Mounted at /api/v1/auth; protected routes use Depends.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_current_user, get_db
from api.auth.schemas import AuthContext, LoginRequest, Token
from api.auth.security import create_access_token
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


@router.post("/login", response_model=Token, summary="Login with email and password")
def login(
    body: LoginRequest,
    db: Annotated[Session, Depends(get_db)],
) -> Token:
    """Authenticate with email/password, returns JWT bearer token."""
    user = authenticate_user(db, body.email, body.password)
    if not user:
        raise AuthenticationError("Incorrect email or password")
    access_token = create_access_token(data={"sub": str(user.id)})
    return Token(access_token=access_token)


@router.get("/me", response_model=AuthContext, summary="Get current user context")
def read_users_me(
    current: AuthContext = Depends(get_current_user),
) -> AuthContext:
    """Returns authenticated user's info, roles, and permissions."""
    return current
