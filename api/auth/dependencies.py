"""Auth dependencies for FastAPI (app-level).

Provides get_db, get_current_user, and require_permission.
"""
from __future__ import annotations

from fastapi import Depends, HTTPException, Path, status
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
    Department,
    Organization,
    Permission,
    Position,
    RolePermission,
    SystemRole,
    User,
    UserRoleAssignment,
)
from db.models.role_permission import RoleType
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


def _resolve_organization(
    org_id_or_slug: str, db: Session, *, allow_deleted: bool = False
) -> Organization:
    """Resolve org by UUID or slug.

    DRY helper shared by read/update/delete access dependencies.
    When ``allow_deleted=False`` (default), soft-deleted orgs raise 404.
    When ``allow_deleted=True`` (for system admins or delete ops), soft-deleted
    orgs are returned so callers can inspect/act on them.
    Raises 404 if not found (or soft-deleted when not allowed).
    """
    org = None
    try:
        org_id = UUID(org_id_or_slug)
        org = db.get(Organization, org_id)
    except ValueError:
        # Slug fallback
        org = db.execute(
            select(Organization).where(Organization.slug == org_id_or_slug)
        ).scalar_one_or_none()

    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organization '{org_id_or_slug}' not found",
        )
    if org.deleted_at and not allow_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organization '{org_id_or_slug}' not found",
        )
    return org


def _check_org_scoped_permission(
    current_user: AuthContext,
    org: Organization,
    permission_slug: str,
    db: Session,
) -> None:
    """Verify the user has a specific org-scoped permission via RBAC chain.

    DRY helper: superuser bypasses; otherwise checks UserRoleAssignment →
    RolePermission → Permission for the specific org + permission combo.
    Raises AuthorizationError (403) on failure.
    """
    if "superuser" in current_user.roles:
        return

    has_access = db.execute(
        select(UserRoleAssignment)
        .join(
            RolePermission,
            (RolePermission.role_id == UserRoleAssignment.role_id)
            & (RolePermission.role_type == UserRoleAssignment.role_type),
        )
        .join(Permission, Permission.id == RolePermission.permission_id)
        .where(
            UserRoleAssignment.user_id == current_user.user_id,
            UserRoleAssignment.org_id == org.id,
            UserRoleAssignment.role_type == RoleType.APP,
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,
            Permission.slug == permission_slug,
        )
    ).first()
    if not has_access:
        raise AuthorizationError(f"Not authorized for organization '{org.slug}'")


def _check_dept_scoped_permission(
    current_user: AuthContext,
    dept: Department,
    permission_slug: str,
    db: Session,
) -> None:
    """Verify the user has a specific department permission via org OR dept RBAC.

    Two-level check for department operations:
    1. Org-level admin (org_admin with org_id = dept.org_id)
    2. Dept-level admin (dept_admin with dept_id = dept.id)

    DRY helper: superuser bypasses; otherwise checks UserRoleAssignment →
    RolePermission → Permission for either org-scoped or dept-scoped access.
    Raises AuthorizationError (403) on failure.
    """
    if "superuser" in current_user.roles:
        return

    # Check 1: Org-level admin access (org_admin role with matching org_id)
    org_access = db.execute(
        select(UserRoleAssignment)
        .join(
            RolePermission,
            (RolePermission.role_id == UserRoleAssignment.role_id)
            & (RolePermission.role_type == UserRoleAssignment.role_type),
        )
        .join(Permission, Permission.id == RolePermission.permission_id)
        .where(
            UserRoleAssignment.user_id == current_user.user_id,
            UserRoleAssignment.org_id == dept.org_id,
            UserRoleAssignment.dept_id.is_(None),  # ORG-scoped (not dept-specific)
            UserRoleAssignment.role_type == RoleType.APP,
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,
            Permission.slug == permission_slug,
        )
    ).first()

    # Check 2: Dept-level admin access (dept_admin role with matching dept_id)
    dept_access = db.execute(
        select(UserRoleAssignment)
        .join(
            RolePermission,
            (RolePermission.role_id == UserRoleAssignment.role_id)
            & (RolePermission.role_type == UserRoleAssignment.role_type),
        )
        .join(Permission, Permission.id == RolePermission.permission_id)
        .where(
            UserRoleAssignment.user_id == current_user.user_id,
            UserRoleAssignment.dept_id == dept.id,
            UserRoleAssignment.role_type == RoleType.APP,
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,
            Permission.slug == permission_slug,
        )
    ).first()

    if not (org_access or dept_access):
        raise AuthorizationError(
            f"Not authorized for department '{dept.slug}' in organization '{dept.organization.slug}'"
        )


def get_organization_with_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("organizations:read")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce access (system role or org admin scope).

    Used by GET /organizations/{org_id_or_slug} for tenancy RBAC.

    - System roles (e.g., superuser) bypass scope AND can see soft-deleted orgs.
    - App roles (e.g., org_admin) must have UserRoleAssignment matching org_id.
    - Raises 404 if not found/active; 403 if no access (via AuthorizationError).
    DRY extension of require_permission; ties into UserRoleAssignment.org_id
    and models (soft-delete, UUID/slug lookup).
    """
    org = _resolve_organization(
        org_id_or_slug, db, allow_deleted=current_user.is_system_admin
    )
    _check_org_scoped_permission(current_user, org, "organizations:read", db)
    return org


def get_organization_with_update_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("organizations:update")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce update access (system role or org admin scope).

    Used by PATCH /organizations/{org_id_or_slug} for tenancy RBAC.

    - Soft-deleted orgs are **never** editable — even system admins must first
      re-enable (un-delete) an org before patching it. ``allow_deleted=False``
      ensures a 404 for any deleted org, regardless of caller role.
    - System roles (e.g., superuser) bypass scope check (but not the soft-delete guard).
    - App roles (e.g., org_admin) must have UserRoleAssignment with organizations:update perm.
    - Raises 404 if not found or soft-deleted; 403 if no update access.
    DRY: reuses _resolve_organization + _check_org_scoped_permission with update perm.
    """
    org = _resolve_organization(org_id_or_slug, db, allow_deleted=False)
    _check_org_scoped_permission(current_user, org, "organizations:update", db)
    return org


def get_organization_with_delete_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("organizations:delete")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce delete access (system role or org admin scope).

    Used by DELETE /organizations/{org_id_or_slug} for tenancy RBAC.

    - ``allow_deleted=True`` so the endpoint can return 404 for already-deleted
      orgs via route logic, rather than silently hiding them.
    - System roles (e.g., superuser) bypass scope.
    - App roles (e.g., org_admin) must have organizations:delete perm scoped to org.
    DRY: reuses _resolve_organization + _check_org_scoped_permission with delete perm.
    """
    org = _resolve_organization(org_id_or_slug, db, allow_deleted=True)
    _check_org_scoped_permission(current_user, org, "organizations:delete", db)
    return org


def check_set_default_permission(
    current_user: AuthContext,
    dept: Department,
    db: Session,
) -> None:
    """Verify the caller has ``departments:set-default`` at org scope.

    Used inline by ``update_department`` when ``is_default=True`` is sent.
    This permission is intentionally restricted to org-level roles (org_admin /
    superuser) and is NOT granted to dept_admin, ensuring only org admins can
    transfer the default flag across departments.

    DRY: delegates to ``_check_org_scoped_permission`` with the set-default slug.
    Raises AuthorizationError (403) on failure.
    """
    _check_org_scoped_permission(current_user, dept.organization, "departments:set-default", db)


def get_organization_with_create_dept_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("departments:create")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce department-create access (org admin scope).

    Used by POST /organizations/{org}/departments/ for tenancy RBAC.

    - Soft-deleted orgs cannot have departments created — ``allow_deleted=False``
      ensures a 404 for any deleted org.
    - System roles (e.g., superuser) bypass scope check.
    - App roles (e.g., org_admin) must have ``departments:create`` perm scoped to org.
    - Raises 404 if org not found or soft-deleted; 403 if no create access.
    DRY: reuses _resolve_organization + _check_org_scoped_permission with create perm.
    """
    org = _resolve_organization(org_id_or_slug, db, allow_deleted=False)
    _check_org_scoped_permission(current_user, org, "departments:create", db)
    return org


def get_organization_with_create_position_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("positions:create")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce position-create access.

    Used by POST /organizations/{org}/positions/ for tenancy RBAC.

    Access is granted to:
    - Superusers (system-wide bypass).
    - Org admins (``positions:create`` scoped to the org).
    - Dept admins (``positions:create`` scoped to a dept within the org).

    The org-level RBAC check covers both org_admin (org_id match) and dept_admin
    (dept_id match within the org). The route handler performs an additional
    dept-ownership guard when ``dept_id`` is supplied: dept_admin callers may
    only create positions for their own department.

    - Soft-deleted orgs cannot have positions created — ``allow_deleted=False``
      ensures a 404 for any deleted org.
    - Raises 404 if org not found or soft-deleted; 403 if no create access.
    """
    org = _resolve_organization(org_id_or_slug, db, allow_deleted=False)
    _check_org_scoped_permission(current_user, org, "positions:create", db)
    return org


def get_organization_with_positions_access(
    org_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-corp"}}),
    current_user: AuthContext = Depends(require_permission("positions:list")),
    db: Session = Depends(get_db),
) -> Organization:
    """Resolve org by ID/slug AND enforce positions-list access (org-scoped RBAC).

    Used by GET /organizations/{org}/positions/ for tenancy RBAC.

    - System roles (e.g., superuser) bypass scope AND can see soft-deleted orgs.
    - App roles (e.g., org_admin, analyst) must have ``positions:list`` perm
      scoped to the org via UserRoleAssignment.
    - Raises 404 if org not found or soft-deleted (for non-system-admins);
      403 if no positions:list access.
    DRY: reuses _resolve_organization + _check_org_scoped_permission with list perm.
    """
    org = _resolve_organization(
        org_id_or_slug, db, allow_deleted=current_user.is_system_admin
    )
    _check_org_scoped_permission(current_user, org, "positions:list", db)
    return org


def check_dept_admin_scope(
    auth: AuthContext,
    org_id: UUID,
    dept_id: UUID | None,
    db: Session,
) -> None:
    """Verify that dept-admin callers may only act within their own department.

    Org admins and superusers are unrestricted. Dept admins must have a
    UserRoleAssignment matching the target ``dept_id``; they cannot act on
    org-wide resources (``dept_id=None``) or on departments outside their
    scope.

    Raises HTTPException(403) on scope violation.
    """
    if auth.is_system_admin or "org_admin" in auth.roles:
        return

    org_level_access = db.execute(
        select(UserRoleAssignment).where(
            UserRoleAssignment.user_id == auth.user_id,
            UserRoleAssignment.org_id == org_id,
            UserRoleAssignment.dept_id.is_(None),  # org-scoped (not dept-specific)
        )
    ).first()

    if org_level_access:
        return

    # Caller has only dept-scoped access
    dept_assignments = db.scalars(
        select(UserRoleAssignment).where(
            UserRoleAssignment.user_id == auth.user_id,
            UserRoleAssignment.org_id == org_id,
            UserRoleAssignment.dept_id.isnot(None),
        )
    ).all()
    allowed_dept_ids = {a.dept_id for a in dept_assignments}

    if dept_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Department admins cannot manage org-wide resources; "
            "specify a department matching your scope",
        )
    if dept_id not in allowed_dept_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not authorized to manage resources in department '{dept_id}'",
        )


# ── Department-scoped dependencies ─────────────────────────────────────────


def _resolve_department(
    dept_id_or_slug: str,
    org: Organization,
    db: Session,
    *,
    allow_deleted: bool = False,
) -> Department:
    """Resolve department by UUID or slug within a specific organization.

    DRY helper for department access dependencies.
    Departments are scoped to an org — slug uniqueness is per-org (not global),
    so we always filter by ``org.id``.
    When ``allow_deleted=False`` (default), soft-deleted departments raise 404.
    Raises 404 if not found (or soft-deleted when not allowed).
    """
    dept = None
    try:
        dept_id = UUID(dept_id_or_slug)
        dept = db.execute(
            select(Department).where(
                Department.id == dept_id,
                Department.org_id == org.id,
            )
        ).scalar_one_or_none()
    except ValueError:
        # Slug fallback (scoped to org)
        dept = db.execute(
            select(Department).where(
                Department.slug == dept_id_or_slug,
                Department.org_id == org.id,
            )
        ).scalar_one_or_none()

    if not dept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Department '{dept_id_or_slug}' not found in organization '{org.slug}'",
        )
    if dept.deleted_at and not allow_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Department '{dept_id_or_slug}' not found in organization '{org.slug}'",
        )
    return dept


def _resolve_position(
    position_id_or_slug: str,
    org: Organization,
    db: Session,
    *,
    allow_deleted: bool = False,
) -> Position:
    """Resolve position by UUID or slug within a specific organization.

    Positions are scoped to an org by ``org_id`` but have globally unique slugs,
    so we still scope lookups to the org for safety.
    """
    position = None
    try:
        position_id = UUID(position_id_or_slug)
        position = db.execute(
            select(Position).where(
                Position.id == position_id,
                Position.org_id == org.id,
            )
        ).scalar_one_or_none()
    except ValueError:
        position = db.execute(
            select(Position).where(
                Position.slug == position_id_or_slug,
                Position.org_id == org.id,
            )
        ).scalar_one_or_none()

    if not position:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Position '{position_id_or_slug}' not found in organization '{org.slug}'",
        )
    if position.deleted_at and not allow_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Position '{position_id_or_slug}' not found in organization '{org.slug}'",
        )
    return position


def _check_position_access(
    current_user: AuthContext,
    position: Position,
    db: Session,
    permission_slug: str,
) -> None:
    """Verify position access for a permission via org OR dept scope.

    Mirrors department access rules: org admins can manage any position in the org;
    dept admins can manage positions in their own department.
    """
    if "superuser" in current_user.roles:
        return

    org_access = db.execute(
        select(UserRoleAssignment)
        .join(
            RolePermission,
            (RolePermission.role_id == UserRoleAssignment.role_id)
            & (RolePermission.role_type == UserRoleAssignment.role_type),
        )
        .join(Permission, Permission.id == RolePermission.permission_id)
        .where(
            UserRoleAssignment.user_id == current_user.user_id,
            UserRoleAssignment.org_id == position.org_id,
            UserRoleAssignment.dept_id.is_(None),
            UserRoleAssignment.role_type == RoleType.APP,
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,
            Permission.slug == permission_slug,
        )
    ).first()

    dept_access = None
    if position.dept_id is not None:
        dept_access = db.execute(
            select(UserRoleAssignment)
            .join(
                RolePermission,
                (RolePermission.role_id == UserRoleAssignment.role_id)
                & (RolePermission.role_type == UserRoleAssignment.role_type),
            )
            .join(Permission, Permission.id == RolePermission.permission_id)
            .where(
                UserRoleAssignment.user_id == current_user.user_id,
                UserRoleAssignment.dept_id == position.dept_id,
                UserRoleAssignment.role_type == RoleType.APP,
                UserRoleAssignment.status == AssignmentStatus.ACTIVE,
                Permission.slug == permission_slug,
            )
        ).first()

    if not (org_access or dept_access):
        raise AuthorizationError(
            f"Not authorized to manage position '{position.slug}' in organization '{position.org_id}'"
        )


def get_department_with_access(
    dept_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-engineering"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("departments:read")),
    db: Session = Depends(get_db),
) -> Department:
    """Resolve department by ID/slug AND enforce access (org-scoped RBAC).

    Used by GET /organizations/{org}/departments/{dept} for tenancy RBAC.

    Two-level access check:
    1. Organization access — via ``get_organization_with_access`` (resolves org,
       checks ``organizations:read`` + org-scoped RBAC).
    2. Department permission — via ``require_permission("departments:read")``.

    - System roles (e.g., superuser) bypass scope AND can see soft-deleted depts.
    - App roles (e.g., org_admin, analyst) must have org-scoped assignment.
    - Raises 404 if org or dept not found/active; 403 if no access.
    """
    dept = _resolve_department(
        dept_id_or_slug, org, db, allow_deleted=current_user.is_system_admin
    )
    return dept


def get_department_with_update_access(
    dept_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-engineering"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("departments:update")),
    db: Session = Depends(get_db),
) -> Department:
    """Resolve department by ID/slug AND enforce update access (org + dept RBAC).

    Used by PATCH /organizations/{org}/departments/{dept} for tenancy RBAC.

    Three-level access check:
    1. Organization access — via ``get_organization_with_access`` (resolves org,
       checks ``organizations:read`` + org-scoped RBAC).
    2. Department permission — via ``require_permission("departments:update")``.
    3. Dept-specific RBAC — via ``_check_dept_scoped_permission`` (org_admin OR dept_admin).

    - Soft-deleted departments are **never** editable — ``allow_deleted=False``
      ensures a 404 for any deleted dept, regardless of caller role.
    - System roles (e.g., superuser) bypass all scope checks.
    - App roles: org_admin (org-scoped) OR dept_admin (dept-scoped) required.
    - Raises 404 if org or dept not found/deleted; 403 if no update access.
    """
    dept = _resolve_department(dept_id_or_slug, org, db, allow_deleted=False)
    _check_dept_scoped_permission(current_user, dept, "departments:update", db)
    return dept


def get_department_with_delete_access(
    dept_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-engineering"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("departments:delete")),
    db: Session = Depends(get_db),
) -> Department:
    """Resolve department by ID/slug AND enforce delete access (org + dept RBAC).

    Used by DELETE /organizations/{org}/departments/{dept} for tenancy RBAC.

    Three-level access check:
    1. Organization access — via ``get_organization_with_access`` (resolves org,
       checks ``organizations:read`` + org-scoped RBAC).
    2. Department permission — via ``require_permission("departments:delete")``.
    3. Dept-specific RBAC — via ``_check_dept_scoped_permission`` (org_admin OR dept_admin).

    - ``allow_deleted=True`` so the endpoint can return 409 for already-deleted
      departments via route logic, rather than silently hiding them.
    - System roles (e.g., superuser) bypass all scope checks.
    - App roles: org_admin (org-scoped) OR dept_admin (dept-scoped) required.
    - Raises 404 if org or dept not found; 403 if no delete access.
    """
    dept = _resolve_department(dept_id_or_slug, org, db, allow_deleted=True)
    _check_dept_scoped_permission(current_user, dept, "departments:delete", db)
    return dept


def get_position_with_access(
    position_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-eng-senior-engineer"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("positions:read")),
    db: Session = Depends(get_db),
) -> Position:
    """Resolve position by ID/slug within org AND enforce read access.

    Used by GET /organizations/{org}/positions/{position} for tenancy RBAC.

    Access rules:
    - Requires org access (organizations:read) plus positions:read permission
      (which analysts, org_admins, dept_admins have).
    - Org admins (org-scoped) or dept admins (dept-scoped) are allowed for read.
    - Superusers bypass scope checks.
    - System admins can see soft-deleted positions; others get 404.

    Mirrors get_department_with_access pattern (DRY).
    """
    position = _resolve_position(
        position_id_or_slug, org, db, allow_deleted=current_user.is_system_admin
    )
    # For read, no extra _check_position_access needed beyond the list/read perm
    # (the org-scoped check in get_organization_with_access + permission covers it)
    return position


def get_position_with_delete_access(
    position_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-eng-senior-engineer"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("positions:delete")),
    db: Session = Depends(get_db),
) -> Position:
    """Resolve position by ID/slug within org AND enforce delete access.

    Used by DELETE /organizations/{org}/positions/{position} for tenancy RBAC.

    Access rules:
    - Requires org access (organizations:read) plus positions:delete permission.
    - Org admins (org-scoped) or dept admins (dept-scoped) are allowed.
    - Superusers bypass scope checks.

    ``allow_deleted=True`` so the route can return 409 for already-deleted positions.
    """
    position = _resolve_position(position_id_or_slug, org, db, allow_deleted=True)
    _check_position_access(current_user, position, db, "positions:delete")
    return position


def get_position_with_update_access(
    position_id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-acme-eng-senior-engineer"}}),
    org: Organization = Depends(get_organization_with_access),
    current_user: AuthContext = Depends(require_permission("positions:update")),
    db: Session = Depends(get_db),
) -> Position:
    """Resolve position by ID/slug within org AND enforce update access.

    Used by PATCH /organizations/{org}/positions/{position} for tenancy RBAC.

    Access rules:
    - Requires org access (organizations:read) plus positions:update permission.
    - Org admins (org-scoped) or dept admins (dept-scoped) are allowed.
    - Superusers bypass scope checks.

    Soft-deleted positions are not editable.
    """
    position = _resolve_position(position_id_or_slug, org, db, allow_deleted=False)
    _check_position_access(current_user, position, db, "positions:update")
    return position
