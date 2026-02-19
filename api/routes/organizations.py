"""Organizations routes (platform tenancy; app-level like auth/health).

Endpoints:
- GET  /organizations/               — cursor-paginated list (system roles only)
- GET  /organizations/{id_or_slug}   — single org detail (system roles + org admins)
- POST /organizations/               — create org (any user with organizations:create)
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_db, get_organization_with_access, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_organizations
from api.schemas import (
    CreateOrganizationRequest,
    OrganizationResponse,
    OrganizationsCursorPage,
)
from db.models import (
    AppRole,
    Organization,
    RoleType,
    UserOrgDepartment,
    UserRoleAssignment,
)

router = APIRouter(prefix="/organizations", tags=["organizations"])


@router.get(
    "/",
    summary="List all organizations (cursor paginated)",
    response_model=OrganizationsCursorPage,
    dependencies=[Depends(require_permission("organizations:list"))],
)
def list_organizations(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    db: Annotated[Session, Depends(get_db)],
) -> OrganizationsCursorPage:
    cursor, limit = params
    return paginate_organizations(db, cursor, limit)


@router.get(
    "/{org_id_or_slug}",
    summary="Get organization by ID or slug",
    response_model=OrganizationResponse,
    dependencies=[Depends(require_permission("organizations:read"))],
)
def get_organization(
    org: Organization = Depends(get_organization_with_access),
) -> OrganizationResponse:
    return OrganizationResponse.model_validate(org, from_attributes=True)


@router.post(
    "/",
    summary="Create a new organization",
    response_model=OrganizationResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_organization(
    body: CreateOrganizationRequest,
    auth: AuthContext = Depends(require_permission("organizations:create")),
    db: Session = Depends(get_db),
) -> OrganizationResponse:
    """Create an organization and assign the creator as org_admin.

    Steps (single transaction):
    1. Validate slug uniqueness
    2. Create Organization record
    3. Create UserOrgDepartment membership (creator → org)
    4. Assign org_admin role scoped to the new org
    """
    # 1. Check slug uniqueness
    existing = db.execute(
        select(Organization).where(Organization.slug == body.slug)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Organization with slug '{body.slug}' already exists",
        )

    # 2. Create organization
    org = Organization(slug=body.slug, description=body.description)
    db.add(org)
    db.flush()  # Get org.id for FK refs

    # 3. Create membership (creator → org, no dept)
    membership = UserOrgDepartment(user_id=auth.user_id, org_id=org.id)
    db.add(membership)

    # 4. Assign org_admin role scoped to this org
    org_admin_role = db.execute(
        select(AppRole).where(AppRole.slug == "org_admin")
    ).scalar_one_or_none()
    if org_admin_role:
        db.add(
            UserRoleAssignment(
                user_id=auth.user_id,
                role_type=RoleType.APP,
                role_id=org_admin_role.id,
                org_id=org.id,
            )
        )

    db.commit()
    db.refresh(org)

    return OrganizationResponse.model_validate(org, from_attributes=True)
