"""Auth routes for JWT login and /me profile.

Simple email/password login (passlib verify; hashed in User model).
/me returns AuthContext with user + RBAC roles/perms (from user_role_assignments).

DRY: reuses security utils, deps (get_current_user), RBAC models, schemas.
FastAPI: OAuth2 form for Swagger (token endpoint), Pydantic responses, docs.

Mounted at /api/v1/auth ; protected routes use Depends.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.orm import Session

# DRY imports from API package
from impact.api.schemas import AuthContext, Token
from impact.api.security import create_access_token
# DRY deps (get_current_user for protected, handles decode/DB/RBAC)
from impact.api.dependencies import get_current_user, get_db
from db.models import User, UserRoleAssignment
from impact.exceptions import ImpactError  # Sanitized

router = APIRouter(prefix="/auth", tags=["auth"])


# Password verify (DRY; passlib[bcrypt] from User model)
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def authenticate_user(db: Session, email: str, password: str) -> User | None:
    """Verify creds against User.hashed_password (DRY with oauth fallback)."""
    user = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if not user or not user.hashed_password:
        return None
    if not pwd_context.verify(password, user.hashed_password):
        return None
    return user


@router.post("/login", response_model=Token, summary="Login with email/password")
def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: Annotated[Session, Depends(get_db)],
) -> Token:
    """OAuth2-compatible login (form for Swagger auto-token UI).

    Returns JWT; use in Authorization: Bearer <token> for protected /me etc.
    Uses settings.access_token_expire_minutes (DRY config).
    """
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # DRY token create; expiry from settings
    access_token = create_access_token(
        data={"sub": str(user.id)},
        # expires_delta defaults to settings.access_token_expire_minutes
    )
    return Token(access_token=access_token)


@router.get("/me", response_model=AuthContext, summary="Get current user + RBAC context")
def read_users_me(
    # DRY protected dep: decode token, DB lookup, RBAC roles/perms query
    current: AuthContext = Depends(get_current_user),
) -> AuthContext:
    """Protected /me : user info + roles/perms from DB (system/app via user_role_assignments).

    Ready for Swagger testing (Authorize button with token).
    """
    # Full impl via dep (extend AuthContext for roles/perms list)
    return current
