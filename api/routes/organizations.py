"""Organizations routes (platform tenancy; app-level like auth/health).

Endpoints:
- GET    /organizations/               — cursor-paginated list with filters (system roles only)
- GET    /organizations/{id_or_slug}   — single org detail (system roles + org admins)
- POST   /organizations/               — create org (any user with organizations:create)
- PATCH  /organizations/{id_or_slug}   — partial update (org admins + superusers)
- DELETE /organizations/{id_or_slug}   — soft-delete (org admins + superusers)
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from datetime import datetime, timezone

from api.auth.dependencies import (
    get_db,
    get_organization_with_access,
    get_organization_with_delete_access,
    get_organization_with_update_access,
    require_permission,
)
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_organizations
from api.schemas import (
    CreateOrganizationRequest,
    DepartmentResponse,
    OrganizationCreateResponse,
    OrganizationDeletedResponse,
    OrganizationFilterParams,
    OrganizationResponse,
    OrganizationsCursorPage,
    UpdateOrganizationRequest,
    get_organization_filters,
)
from db.models import (
    AppRole,
    Department,
    Organization,
    OrganizationStatus,
    RoleType,
    UserOrgDepartment,
    UserRoleAssignment,
)

router = APIRouter(prefix="/organizations", tags=["organizations"])


@router.get(
    "/",
    summary="List all organizations (cursor paginated, filterable)",
    response_model=OrganizationsCursorPage,
)
def list_organizations(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[OrganizationFilterParams, Depends(get_organization_filters)],
    auth: AuthContext = Depends(require_permission("organizations:list")),
    db: Session = Depends(get_db),
) -> OrganizationsCursorPage:
    """List organizations with optional status filters.

    Non-system-admin users see only non-deleted organizations by default.
    System admins see all organizations including soft-deleted ones.
    Explicit status filters (e.g. ``?status=DELETED``) narrow results
    to the requested statuses regardless of role.
    """
    cursor, limit = params
    return paginate_organizations(
        db, cursor, limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
    )


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
    response_model=OrganizationCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_organization(
    body: CreateOrganizationRequest,
    auth: AuthContext = Depends(require_permission("organizations:create")),
    db: Session = Depends(get_db),
) -> OrganizationCreateResponse:
    """Create an organization with a default department and assign the creator as org_admin.

    Steps (single transaction):
    1. Validate slug uniqueness
    2. Create Organization record
    3. Create default Department (slug ``general``) for the new org
    4. Create UserOrgDepartment membership (creator → org + default dept)
    5. Assign org_admin role scoped to the new org
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
    org = Organization(name=body.name, slug=body.slug, description=body.description)
    db.add(org)
    db.flush()  # Get org.id for FK refs

    # 3. Create default department (every org starts with a "general" department)
    #    is_default=True marks it as the immutable org default (cannot be
    #    soft-deleted or deactivated via API; cascade-deleted with the org).
    #    activated_at set to creation time since it is always active.
    now = datetime.now(timezone.utc)
    default_dept = Department(
        org_id=org.id,
        name="General",
        slug="general",
        description="Default department",
        is_default=True,
        activated_at=now,
    )
    db.add(default_dept)
    db.flush()  # Get default_dept.id for FK refs

    # 4. Create membership (creator → org + default dept)
    membership = UserOrgDepartment(
        user_id=auth.user_id,
        org_id=org.id,
        dept_id=default_dept.id,
    )
    db.add(membership)

    # 5. Assign org_admin role scoped to this org
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
    db.refresh(default_dept)

    org_response = OrganizationResponse.model_validate(org, from_attributes=True)
    dept_response = DepartmentResponse.model_validate(default_dept, from_attributes=True)

    return OrganizationCreateResponse(
        **org_response.model_dump(),
        default_department=dept_response,
    )


# ── Status → timestamp mapping for lifecycle transitions ──────────────
_STATUS_TIMESTAMP_FIELD: dict[OrganizationStatus, str] = {
    OrganizationStatus.ACTIVE: "activated_at",
    OrganizationStatus.DEACTIVATED: "deactivated_at",
    OrganizationStatus.BANNED: "banned_at",
    OrganizationStatus.DELETED: "deleted_at",
}


@router.patch(
    "/{org_id_or_slug}",
    summary="Partially update an organization",
    response_model=OrganizationResponse,
)
def update_organization(
    body: UpdateOrganizationRequest,
    org: Organization = Depends(get_organization_with_update_access),
    db: Session = Depends(get_db),
) -> OrganizationResponse:
    """Partially update an organization's properties.

    Only fields present in the request body are applied (true PATCH semantics).
    Uses Pydantic v2 ``model_fields_set`` to distinguish "not sent" from
    "explicitly sent as null" — e.g., sending ``{"description": null}`` clears
    the description, while omitting it leaves it unchanged.

    Steps:
    1. Reject empty body (no fields to update)
    2. Validate slug uniqueness if changing
    3. Apply only sent fields to the ORM instance
    4. Set lifecycle timestamp on status transitions
    5. Commit + refresh and return updated org
    """
    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    # Slug uniqueness check (only if slug is being changed)
    if "slug" in sent_fields and body.slug is not None and body.slug != org.slug:
        existing = db.execute(
            select(Organization).where(Organization.slug == body.slug)
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Organization with slug '{body.slug}' already exists",
            )

    # Apply each sent field granularly
    if "name" in sent_fields:
        if body.name is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="name cannot be null",
            )
        org.name = body.name

    if "slug" in sent_fields:
        if body.slug is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="slug cannot be null",
            )
        org.slug = body.slug

    if "description" in sent_fields:
        # description is nullable — setting to None clears it
        org.description = body.description

    if "status" in sent_fields:
        if body.status is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="status cannot be null",
            )
        if body.status != org.status:
            org.status = body.status
            # Record lifecycle timestamp for the new status
            ts_field = _STATUS_TIMESTAMP_FIELD.get(body.status)
            if ts_field:
                setattr(org, ts_field, datetime.now(timezone.utc))

    db.commit()
    db.refresh(org)

    return OrganizationResponse.model_validate(org, from_attributes=True)


@router.delete(
    "/{org_id_or_slug}",
    summary="Soft-delete an organization",
    response_model=OrganizationDeletedResponse,
)
def delete_organization(
    org: Organization = Depends(get_organization_with_delete_access),
    db: Session = Depends(get_db),
) -> OrganizationDeletedResponse:
    """Soft-delete an organization (sets status=DELETED and deleted_at timestamp).

    Idempotency: returns 409 if the organization is already soft-deleted,
    so callers can distinguish "just deleted" from "was already deleted".
    The organization record is preserved for auditing; hard-delete is not
    supported via API.
    """
    if org.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Organization '{org.slug}' is already deleted",
        )

    now = datetime.now(timezone.utc)
    org.status = OrganizationStatus.DELETED
    org.deleted_at = now

    db.commit()
    db.refresh(org)

    return OrganizationDeletedResponse(
        id=org.id,
        slug=org.slug,
        status=org.status,
        deleted_at=org.deleted_at,
    )
