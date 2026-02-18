"""Auth dependencies for FastAPI (app-level).

Provides get_db, get_current_user, and require_permission.
"""
from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.schemas import AuthContext
from api.auth.security import TokenData, decode_access_token
from api.exceptions import AuthenticationError, AuthorizationError
from db.engine import SyncSessionLocal
from db.models import (
    AppRole,
    AssignmentStatus,
    Organization,
    Permission,
    RolePermission,
    SystemRole,
    User,
    UserRoleAssignment,
)
from db.models.role_permission import RoleType
from sqlalchemy import select
from uuid import UUID


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


def get_organization_with_access(
    org_id_or_slug: str,
    current_user: AuthContext = Depends(require_permission("organizations:read")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce access (system role or org admin scope).

    Used by GET /organizations/{org_id_or_slug} for tenancy RBAC.

    - System roles (e.g., superuser) bypass scope.
    - App roles (e.g., org_admin) must have UserRoleAssignment matching org_id.
    - Raises 404 if not found/active; 403 if no access (via AuthorizationError).
    DRY extension of require_permission; ties into UserRoleAssignment.org_id
    and models (soft-delete, UUID/slug lookup).
    """
    # Resolve org (flexible ID or slug; indexed; soft-delete filter)
    org = None
    try:
        org_id = UUID(org_id_or_slug)
        org = db.get(Organization, org_id)
    except ValueError:
        # Slug fallback
        org = db.execute(
            select(Organization).where(Organization.slug == org_id_or_slug)
        ).scalar_one_or_none()

    if not org or org.deleted_at:
        raise HTTPException(  # Safe 404 (pattern from routes; no leak)
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organization '{org_id_or_slug}' not found",
        )

    # System role bypass (DRY with require_permission superuser check)
    if "superuser" in current_user.roles:
        return org

    # Org admin scope: full RBAC chain for specific org
    # Combine user_id + org_id (tenancy) + role -> perm (organizations:read)
    # Joins: UserRoleAssignment (user/org scope) -> RolePermission (polymorphic) -> Permission.slug
    # (DRY with get_current_user perm resolution; explicit for org_admin app role)
    # System roles bypassed earlier; active/APP filter for scope.
    # Ensures perm + org combo as requested.
    has_access = db.execute(
        select(UserRoleAssignment)
        .join(
            RolePermission,
            # Polymorphic join on role_type/role_id (DRY with role_permission model)
            (RolePermission.role_id == UserRoleAssignment.role_id)
            & (RolePermission.role_type == UserRoleAssignment.role_type),
        )
        .join(Permission, Permission.id == RolePermission.permission_id)
        .where(
            UserRoleAssignment.user_id == current_user.user_id,
            UserRoleAssignment.org_id == org.id,  # Org-specific combo
            UserRoleAssignment.role_type == RoleType.APP,  # Org-scoped app role
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,  # Active assignment
            Permission.slug == "organizations:read",  # Specific perm from RBAC yaml
        )
    ).first()
    if not has_access:
        raise AuthorizationError(f"Not authorized for organization '{org.slug}'")

    return org
