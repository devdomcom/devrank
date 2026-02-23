"""Global roles routes.

Endpoints:
- GET /roles/ — cursor-paginated list of global roles (global_role=True,
  org_id=NULL). These are the platform-wide default roles available to every
  organization for use with positions and assessments. Org-specific roles
  (created by individual organizations) are always excluded.

Access model:
- Authenticated users with ``roles:list`` permission can list global roles.
- System admins (superuser) see all statuses including DELETED.
- Regular callers see DRAFT and PUBLISHED roles only (DELETED hidden).
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.auth.dependencies import get_db, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_roles
from api.schemas import RoleFilterParams, RolesCursorPage, get_role_filters

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

    Returns a cursor-paginated list of roles where ``global_role=True`` and
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
