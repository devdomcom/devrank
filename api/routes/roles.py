"""Global roles routes.

Endpoints:
- GET /roles/          — cursor-paginated list of global roles (is_global=True,
                         org_id=NULL). Platform-wide defaults for positions/assessments.
- GET /roles/{id_or_slug} — single role detail including JSON config (metric
                            thresholds synced from YAML via ``devrank roles sync``).

Access model:
- ``roles:list``  → list endpoint.
- ``roles:read``  → detail endpoint.
- System admins (superuser) see all statuses including DELETED.
- Regular callers see DRAFT and PUBLISHED roles only (DELETED hidden).
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_db, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_roles
from api.schemas import RoleFilterParams, RoleResponse, RolesCursorPage, get_role_filters
from db.models.role import Role, RoleStatus

router = APIRouter(
    prefix="/roles",
    tags=["roles"],
)


@router.get(
    "/",
    summary="List global roles available across all organizations (cursor paginated)",
    response_model=RolesCursorPage,
)
def list_global_roles(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[RoleFilterParams, Depends(get_role_filters)],
    auth: AuthContext = Depends(require_permission("roles:list")),
    db: Session = Depends(get_db),
) -> RolesCursorPage:
    """List all platform-wide global roles.

    Returns a cursor-paginated list of roles where ``is_global=True`` and
    ``org_id=null``. These are the default roles defined for the entire
    platform that any organization can reference when creating positions or
    running assessments. Org-specific roles (created by individual organizations
    with a non-null ``org_id``) are always excluded from this endpoint.

    **Status filtering**

    By default, only ``DRAFT`` and ``PUBLISHED`` roles are returned. System
    admins can pass ``?status=DELETED`` to include soft-deleted roles for audit
    purposes:

        GET /roles/?status=PUBLISHED
        GET /roles/?status=DRAFT&status=PUBLISHED

    **Search**

    ``?search=senior`` does a case-insensitive contains-match on ``slug`` and
    ``description``:

        GET /roles/?search=senior

    **Cursor pagination**

    Use ``next_cursor`` from the previous response as the ``cursor`` query
    parameter to fetch the next page. ``next_cursor=null`` signals the last page.

        GET /roles/?cursor=<opaque>&limit=20

    System admins (``is_system_admin=True``) automatically see DELETED roles
    alongside non-deleted ones when no explicit ``status`` filter is applied.
    """
    cursor, limit = params
    return paginate_roles(
        db,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
    )


@router.get(
    "/{id_or_slug}",
    summary="Get a global role by ID or slug (includes config)",
    response_model=RoleResponse,
)
def get_global_role(
    id_or_slug: str = Path(openapi_examples={"default": {"value": "sample-role-senior-engineer"}}),
    auth: AuthContext = Depends(require_permission("roles:read")),
    db: Session = Depends(get_db),
) -> RoleResponse:
    """Fetch a single global role by UUID or slug.

    Returns the full role including the JSON ``config`` column containing
    metric thresholds and configuration synced from YAML via
    ``devrank roles sync``.

    Only global roles (``is_global=True``, ``org_id=None``) are returned.
    Soft-deleted roles are visible only to system admins.
    """
    try:
        role_id = uuid.UUID(id_or_slug)
        role = db.execute(
            select(Role).where(
                Role.id == role_id,
                Role.is_global.is_(True),
                Role.org_id.is_(None),
            )
        ).scalar_one_or_none()
    except ValueError:
        role = db.execute(
            select(Role).where(
                Role.slug == id_or_slug,
                Role.is_global.is_(True),
                Role.org_id.is_(None),
            )
        ).scalar_one_or_none()

    if role is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Global role '{id_or_slug}' not found",
        )

    if role.status == RoleStatus.DELETED and not auth.is_system_admin:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Global role '{id_or_slug}' not found",
        )

    return RoleResponse.model_validate(role, from_attributes=True)
