"""Auth dependencies for FastAPI (app-level).

Provides get_db, get_current_user, and require_permission.
"""
from __future__ import annotations

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.schemas import AuthContext
from api.auth.security import TokenData, decode_access_token
from api.exceptions import AuthenticationError, AuthorizationError
from db.engine import SyncSessionLocal
from db.models import (
    AppRole,
    Permission,
    RolePermission,
    SystemRole,
    User,
    UserRoleAssignment,
)
from db.models.role_permission import RoleType


def get_db() -> Session:
    """Yield sync DB session (close after request)."""
    db = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()


bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> AuthContext:
    """Resolve JWT → User → roles/permissions → AuthContext."""
    token_data: TokenData = decode_access_token(credentials.credentials)
    if token_data.user_id is None:
        raise AuthenticationError("Invalid credentials")
    user = db.get(User, token_data.user_id)
    if not user:
        raise AuthenticationError("User not found")

    # Resolve role assignments → human-readable slugs
    assignments = db.scalars(
        select(UserRoleAssignment).where(UserRoleAssignment.user_id == user.id)
    ).all()

    role_slugs: list[str] = []
    role_ids_by_type: dict[str, list] = {"SYSTEM": [], "APP": []}
    for a in assignments:
        role_ids_by_type[a.role_type.value].append(a.role_id)

    if role_ids_by_type["SYSTEM"]:
        sys_roles = db.scalars(
            select(SystemRole).where(SystemRole.id.in_(role_ids_by_type["SYSTEM"]))
        ).all()
        role_slugs.extend(r.slug for r in sys_roles)

    if role_ids_by_type["APP"]:
        app_roles = db.scalars(
            select(AppRole).where(AppRole.id.in_(role_ids_by_type["APP"]))
        ).all()
        role_slugs.extend(r.slug for r in app_roles)

    # Resolve permissions via role_permissions → permissions
    all_role_ids = [a.role_id for a in assignments]
    perm_slugs: list[str] = []
    if all_role_ids:
        perms = db.execute(
            select(Permission.slug)
            .join(RolePermission, RolePermission.permission_id == Permission.id)
            .where(RolePermission.role_id.in_(all_role_ids))
        ).scalars().all()
        perm_slugs = list(set(perms))

    return AuthContext(
        user_id=user.id,
        email=user.email,
        name=f"{user.name} {user.surname}".strip(),
        roles=role_slugs,
        permissions=perm_slugs,
    )


def require_permission(*permission_slugs: str):
    """Dependency factory: enforces authentication AND one or more permissions.

    Multiple slugs are checked as AND (user must have ALL of them).
    Superuser role bypasses all permission checks.

    Usage (single permission):
        @router.get("/", dependencies=[Depends(require_permission("metrics:list"))])

    Usage (multiple permissions — AND):
        @router.post("/", dependencies=[Depends(require_permission("dumps:upload", "metrics:compute"))])

    Usage (handler needs the authenticated user):
        def endpoint(auth: AuthContext = Depends(require_permission("metrics:read"))):
    """

    def _check_permission(
        current_user: AuthContext = Depends(get_current_user),
    ) -> AuthContext:
        if "superuser" in current_user.roles:
            return current_user
        missing = [p for p in permission_slugs if p not in current_user.permissions]
        if missing:
            raise AuthorizationError(
                f"Missing required permission(s): {', '.join(missing)}"
            )
        return current_user

    return _check_permission
