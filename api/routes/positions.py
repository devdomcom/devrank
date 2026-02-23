"""Positions routes (org-scoped; nested under organizations).

Endpoints:
- GET  /organizations/{org_id_or_slug}/positions/ — cursor-paginated list of
  open (PUBLISHED) positions, filterable by department slugs or IDs.
- POST /organizations/{org_id_or_slug}/positions/ — create a new position
  (org admins, dept admins, or users with positions:create permission).

Positions represent open roles within an organization (and optionally a
specific department). They follow a DRAFT → PUBLISHED → DELETED lifecycle.

Access model (create):
- ``positions:create`` permission required.
- Org admins: can create positions for any department in their org, or org-wide.
- Dept admins: can only create positions scoped to their own department.
- Superusers: full bypass.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import (
    get_db,
    get_organization_with_create_position_access,
    get_organization_with_positions_access,
    require_permission,
)
from api.auth.schemas import AuthContext
from api.exceptions import AuthorizationError
from api.pagination import get_cursor_params, paginate_positions
from api.schemas import (
    CreatePositionRequest,
    PositionFilterParams,
    PositionResponse,
    PositionsCursorPage,
    get_position_filters,
)
from db.models import Department, Organization
from db.models.position import Position, PositionStatus
from db.models.role import Role
from db.models.user_role_assignment import UserRoleAssignment

router = APIRouter(
    prefix="/organizations/{org_id_or_slug}/positions",
    tags=["positions"],
)


@router.get(
    "/",
    summary="List open positions for an organization (cursor paginated, filterable)",
    response_model=PositionsCursorPage,
)
def list_positions(
    org: Organization = Depends(get_organization_with_positions_access),
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)] = ...,
    filters: Annotated[PositionFilterParams, Depends(get_position_filters)] = ...,
    auth: AuthContext = Depends(require_permission("positions:list")),
    db: Session = Depends(get_db),
) -> PositionsCursorPage:
    """List open (PUBLISHED) positions for an organization.

    Returns a cursor-paginated list of positions scoped to the requested
    organization. By default only ``PUBLISHED`` positions are returned — these
    are the "open" positions visible to hiring managers and candidates.

    **Filtering by department**

    Pass ``dept_ids`` (UUIDs) and/or ``dept_slugs`` (human-readable slugs)
    to narrow results to one or more departments. Both parameters are
    repeatable and OR-combined:

        GET /organizations/acme/positions/?dept_slugs=engineering&dept_slugs=platform

    Dept slugs are resolved to UUIDs scoped to this organization at query time,
    so cross-org slug collisions are not possible.

    **Status filtering**

    Omitting ``status`` returns only PUBLISHED positions (the default "open"
    view). Pass ``?status=DRAFT`` or ``?status=DELETED`` (system admins only)
    to override:

        GET /organizations/acme/positions/?status=PUBLISHED&status=DRAFT

    **Search**

    ``?search=senior`` does a case-insensitive contains-match on ``slug`` and
    ``description``.

    **Cursor pagination**

    Use ``next_cursor`` from the previous response as the ``cursor`` query
    parameter to fetch the next page. ``next_cursor=null`` signals the last page.

    Organization access is verified via ``get_organization_with_positions_access``
    (system roles bypass; org-scoped roles checked via RBAC chain).
    ``auth.is_system_admin`` drives the ``include_deleted`` flag so system admins
    see PUBLISHED + DELETED positions by default without an explicit status filter.
    """
    cursor, limit = params
    return paginate_positions(
        db,
        org.id,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
    )


@router.post(
    "/",
    summary="Create a new position within an organization",
    response_model=PositionResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_position(
    body: CreatePositionRequest,
    org: Organization = Depends(get_organization_with_create_position_access),
    auth: AuthContext = Depends(require_permission("positions:create")),
    db: Session = Depends(get_db),
) -> PositionResponse:
    """Create a new position within an organization.

    **Access rules**

    - **Superusers**: full bypass — can create any position in any org/dept.
    - **Org admins** (``positions:create`` scoped to org): can create positions
      for any department in their org, or org-wide (``dept_id=null``).
    - **Dept admins** (``positions:create`` scoped to a dept): can only create
      positions scoped to their own department. Attempting to create an org-wide
      position or a position in a different department returns 403.

    **Validation steps**

    1. Reject ``status=DELETED`` on creation (use DELETE endpoint instead).
    2. Resolve ``dept_id_or_slug`` (if provided) to a department within this org.
    3. Dept-admin scope guard: if the caller is a dept_admin (not org_admin /
       superuser), the resolved dept must match one of their scoped departments.
    4. Resolve ``role_id_or_slug`` to an existing role (404 otherwise).
    5. Reject duplicate slug (globally unique — 409 Conflict).
    6. Reject duplicate ``(org_id, dept_id, role_id)`` triplet (409 Conflict).
    7. Set ``published_at`` automatically when ``status=PUBLISHED``.
    8. Commit and return the created position.
    """
    now = datetime.now(timezone.utc)

    # ── 1. Reject DELETED on creation ─────────────────────────────────────
    if body.status == PositionStatus.DELETED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot create a position with status DELETED; use the DELETE endpoint",
        )

    # ── 2. Resolve dept_id_or_slug (optional) ──────────────────────────────
    dept: Department | None = None
    dept_id: uuid.UUID | None = None
    if body.dept_id_or_slug is not None:
        try:
            dept_uuid = uuid.UUID(body.dept_id_or_slug)
            dept = db.execute(
                select(Department).where(
                    Department.id == dept_uuid,
                    Department.org_id == org.id,
                )
            ).scalar_one_or_none()
        except ValueError:
            dept = db.execute(
                select(Department).where(
                    Department.slug == body.dept_id_or_slug,
                    Department.org_id == org.id,
                )
            ).scalar_one_or_none()

        if not dept:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Department '{body.dept_id_or_slug}' not found in organization '{org.slug}'",
            )
        if dept.deleted_at is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Department '{dept.slug}' is deleted; cannot create a position in it",
            )
        dept_id = dept.id

    # ── 3. Dept-admin scope guard ──────────────────────────────────────────
    # Dept admins may only create positions scoped to their own department.
    # Org admins and superusers are unrestricted.
    if not auth.is_system_admin and "org_admin" not in auth.roles:
        org_level_access = db.execute(
            select(UserRoleAssignment).where(
                UserRoleAssignment.user_id == auth.user_id,
                UserRoleAssignment.org_id == org.id,
                UserRoleAssignment.dept_id.is_(None),  # org-scoped (not dept-specific)
            )
        ).first()

        if not org_level_access:
            # Caller has only dept-scoped access: find their dept assignments for this org
            dept_assignments = db.scalars(
                select(UserRoleAssignment).where(
                    UserRoleAssignment.user_id == auth.user_id,
                    UserRoleAssignment.org_id == org.id,
                    UserRoleAssignment.dept_id.isnot(None),
                )
            ).all()
            allowed_dept_ids = {a.dept_id for a in dept_assignments}

            if dept_id is None:
                raise AuthorizationError(
                    "Department admins cannot create org-wide positions; "
                    "specify a dept_id_or_slug matching your department"
                )
            if dept_id not in allowed_dept_ids:
                raise AuthorizationError(
                    f"Not authorized to create positions in department '{dept_id}'"
                )

    # ── 4. Resolve role_id_or_slug ─────────────────────────────────────────
    role: Role | None = None
    try:
        role_uuid = uuid.UUID(body.role_id_or_slug)
        role = db.get(Role, role_uuid)
    except ValueError:
        role = db.execute(
            select(Role).where(Role.slug == body.role_id_or_slug)
        ).scalar_one_or_none()

    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Role '{body.role_id_or_slug}' not found",
        )

    # ── 5. Reject duplicate slug (globally unique) ─────────────────────────
    existing_slug = db.execute(
        select(Position).where(Position.slug == body.slug)
    ).scalar_one_or_none()
    if existing_slug:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Position with slug '{body.slug}' already exists",
        )

    # ── 6. Reject duplicate (org, dept, role) triplet ─────────────────────
    dept_clause = (
        Position.dept_id.is_(None)
        if dept_id is None
        else Position.dept_id == dept_id
    )
    existing_triplet = db.execute(
        select(Position).where(
            Position.org_id == org.id,
            dept_clause,
            Position.role_id == role.id,
        )
    ).scalar_one_or_none()
    if existing_triplet:
        dept_label = str(dept_id) if dept_id else "org-wide"
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"A position for role '{role.slug}' already exists in "
                f"org '{org.slug}' / dept '{dept_label}'"
            ),
        )

    # ── 7. Set published_at if publishing immediately ──────────────────────
    published_at: datetime | None = now if body.status == PositionStatus.PUBLISHED else None

    # ── 8. Create and persist ──────────────────────────────────────────────
    position = Position(
        org_id=org.id,
        dept_id=dept_id,
        role_id=role.id,
        slug=body.slug,
        description=body.description,
        status=body.status,
        created_at=now,
        updated_at=now,
        published_at=published_at,
    )
    db.add(position)
    db.commit()
    db.refresh(position)

    return PositionResponse.model_validate(position, from_attributes=True)
