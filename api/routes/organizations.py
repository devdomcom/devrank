"""Organizations routes (platform tenancy; app-level like auth/health).

GET /organizations/ — cursor-paginated list, system-role restricted via RBAC perm.

Follows existing patterns:
- APIRouter with prefix/tags (see auth.py, health.py)
- require_permission dep from api/auth/dependencies.py for enforcement
  at API level (DRY; ties into permissions.yaml/system_roles)
- get_db dep, sync endpoint (blocking DB = threadpooled by FastAPI)
- Reuses pagination utils + schemas (DRY)
- Pydantic response_model (AGENTS.md best practice; schema validation)
- No direct body consumer + Depends mix (avoids parsing error)
- Soft-delete aware, indexed query for perf

RBAC note: perm "organizations:list" only in superuser (system role) via "*";
app roles excluded. Service-layer guard in paginate_ func.

2026 FastAPI: dep factories, typed Annotated, cursor over offset.
"""
from __future__ import annotations

from typing import Annotated

# FastAPI (APIRouter, Depends, HTTPException for 404 pattern from roles.py/metrics)
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

# Auth/DB deps (app-level; shared with /auth, /health/infra)
# Includes get_organization_with_access for RBAC scope (system/org-admin)
from api.auth.dependencies import get_db, get_organization_with_access, require_permission
# Pagination utils (DRY cursor logic)
from api.pagination import get_cursor_params, paginate_organizations
# Response schema (platform-level in api/schemas.py)
from api.schemas import OrganizationsCursorPage, OrganizationResponse

# Router mounted at /api/v1/organizations (v1 prefix in app.py)
router = APIRouter(prefix="/organizations", tags=["organizations"])


@router.get(
    "/",
    summary="List all organizations (cursor paginated)",
    description=(
        "Returns active organizations with cursor pagination. "
        "Restricted to users with system roles (e.g., superuser) via "
        "'organizations:list' permission (see api/auth/rbac/permissions.yaml)."
    ),
    response_model=OrganizationsCursorPage,
    # Dependencies list for perm check (pattern from metrics/roles routes;
    # handler receives no auth if not needed, but could inject)
    dependencies=[Depends(require_permission("organizations:list"))],
)
def list_organizations(
    # Unpack cursor/limit from shared dep (DRY; validated Query)
    params: Annotated[
        tuple[str | None, int], Depends(get_cursor_params)
    ],
    # DB session per-request (from api/auth/dependencies.py; matches auth.py)
    db: Annotated[Session, Depends(get_db)],
) -> OrganizationsCursorPage:
    """List organizations endpoint.

    Uses pagination.paginate_organizations for query/encoding/mapping.
    Aligns with DB models (soft-delete, UUID PK) and FastAPI best practices.
    """
    cursor, limit = params
    return paginate_organizations(db, cursor, limit)


@router.get(
    "/{org_id_or_slug}",
    summary="Get organization by ID or slug",
    description=(
        "Returns full details for an org (active only). Accessible to system roles "
        "(e.g., superuser) or org admins (app role with org_id scope match via "
        "RBAC 'organizations:read' perm and UserRoleAssignment check)."
    ),
    response_model=OrganizationResponse,
    # Perm check at API level (DRY with list; org-scope enforced in dep)
    dependencies=[Depends(require_permission("organizations:read"))],
)
def get_organization(
    # Uses dep for resolve + RBAC scope (system/org-admin; handles ID/slug, 404, access)
    # DRY with single-item pattern from roles.py/metrics/{slug}
    org: Organization = Depends(get_organization_with_access),
) -> OrganizationResponse:
    """Get org detail.

    Dep handles query/scope; map ORM to schema (Pydantic from_attributes).
    """
    return OrganizationResponse.model_validate(org, from_attributes=True)
