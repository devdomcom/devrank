"""Departments routes (org-scoped; nested under organizations).

Endpoints:
- GET    /organizations/{org_id_or_slug}/departments/                  — cursor-paginated list
- GET    /organizations/{org_id_or_slug}/departments/{dept_id_or_slug} — single department detail
- POST   /organizations/{org_id_or_slug}/departments/                  — create department
- PATCH  /organizations/{org_id_or_slug}/departments/{dept_id_or_slug} — partial update
- DELETE /organizations/{org_id_or_slug}/departments/{dept_id_or_slug} — soft-delete

Departments are always scoped to an organization. Access requires either
system-level permissions (superuser), org-scoped RBAC (org_admin), or
dept-scoped RBAC (dept_admin). Analysts have read-only access.

A default department (slug ``general``) is auto-created when an organization
is created (see ``api/routes/organizations.py``). Additional departments can
be created via the POST endpoint by org_admins and superusers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import (
    check_set_default_permission,
    get_db,
    get_department_with_access,
    get_department_with_delete_access,
    get_department_with_update_access,
    get_organization_with_create_dept_access,
    get_organization_with_access,
    require_permission,
)
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_departments
from api.schemas import (
    CreateDepartmentRequest,
    DepartmentDeletedResponse,
    DepartmentFilterParams,
    DepartmentResponse,
    DepartmentsCursorPage,
    UpdateDepartmentRequest,
    get_department_filters,
)
from db.models import Department, DepartmentStatus, Organization

router = APIRouter(
    prefix="/organizations/{org_id_or_slug}/departments",
    tags=["departments"],
)


@router.get(
    "/",
    summary="List departments of an organization (cursor paginated, filterable)",
    response_model=DepartmentsCursorPage,
)
def list_departments(
    org: Organization = Depends(get_organization_with_access),
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)] = ...,
    filters: Annotated[DepartmentFilterParams, Depends(get_department_filters)] = ...,
    auth: AuthContext = Depends(require_permission("departments:list")),
    db: Session = Depends(get_db),
) -> DepartmentsCursorPage:
    """List departments for an organization with optional status/search filters.

    Non-system-admin users see only non-deleted departments by default.
    System admins see all departments including soft-deleted ones.
    Explicit status filters (e.g. ``?status=DELETED``) narrow results
    to the requested statuses regardless of role.

    Organization access is verified via ``get_organization_with_access``
    (system roles bypass; org-scoped roles checked via RBAC chain).
    """
    cursor, limit = params
    return paginate_departments(
        db,
        org.id,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
    )


@router.get(
    "/{dept_id_or_slug}",
    summary="Get department by ID or slug within an organization",
    response_model=DepartmentResponse,
)
def get_department(
    dept: Department = Depends(get_department_with_access),
) -> DepartmentResponse:
    """Return full detail for a single department.

    Organization access is verified via ``get_organization_with_access``
    (embedded in ``get_department_with_access``). Department permission
    checked via ``departments:read``. System admins can see soft-deleted
    departments; regular users get 404 for deleted ones.
    """
    return DepartmentResponse.model_validate(dept, from_attributes=True)


@router.post(
    "/",
    summary="Create a new department within an organization",
    response_model=DepartmentResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_department(
    body: CreateDepartmentRequest,
    org: Organization = Depends(get_organization_with_create_dept_access),
    db: Session = Depends(get_db),
) -> DepartmentResponse:
    """Create a department within an organization.

    Steps (single transaction):
    1. Validate slug uniqueness within the organization
    2. Create Department record linked to the org
    3. Commit + refresh and return created department

    Access requires org_admin (org-level) or superuser (system-level).
    Organization access + ``departments:create`` permission verified via
    ``get_organization_with_create_dept_access`` (org-scoped RBAC check).
    """
    # 1. Check slug uniqueness within org
    existing = db.execute(
        select(Department).where(
            Department.org_id == org.id,
            Department.slug == body.slug,
        )
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Department with slug '{body.slug}' already exists in this organization",
        )

    # 2. Create department
    dept = Department(
        org_id=org.id,
        name=body.name,
        slug=body.slug,
        description=body.description,
    )
    db.add(dept)
    db.commit()
    db.refresh(dept)

    return DepartmentResponse.model_validate(dept, from_attributes=True)


# ── Status → timestamp mapping for lifecycle transitions ──────────────
_DEPT_STATUS_TIMESTAMP_FIELD: dict[DepartmentStatus, str] = {
    DepartmentStatus.ACTIVE: "activated_at",
    DepartmentStatus.DEACTIVATED: "deactivated_at",
    DepartmentStatus.DELETED: "deleted_at",
}


@router.patch(
    "/{dept_id_or_slug}",
    summary="Partially update a department",
    response_model=DepartmentResponse,
)
def update_department(
    body: UpdateDepartmentRequest,
    dept: Department = Depends(get_department_with_update_access),
    auth: AuthContext = Depends(require_permission("departments:update")),
    db: Session = Depends(get_db),
) -> DepartmentResponse:
    """Partially update a department's properties.

    Only fields present in the request body are applied (true PATCH semantics).
    Uses Pydantic v2 ``model_fields_set`` to distinguish "not sent" from
    "explicitly sent as null" — e.g., sending ``{"description": null}`` clears
    the description, while omitting it leaves it unchanged.

    Default departments (``is_default=True``) cannot be deactivated or have
    their status or slug changed — they are always ACTIVE for the lifetime of
    the org. Only ``description`` and ``is_default`` (transfer) may be sent.

    Setting ``is_default=true`` transfers the default flag from the current
    default department to this one atomically. The target department must be
    ACTIVE and must not already be the default. Only org admins and superusers
    may perform this operation (``departments:set-default`` permission).

    Steps:
    1. Reject empty body (no fields to update)
    2. Guard default department immutability (status + slug)
    3. Handle is_default transfer (org-admin permission + atomic swap)
    4. Validate slug uniqueness within the org if changing
    5. Apply remaining sent fields to the ORM instance
    6. Reject DELETED status (use DELETE endpoint instead)
    7. Set lifecycle timestamp on status transitions
    8. Commit + refresh and return updated department
    """
    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    # Guard: default department cannot have status or slug changed
    if dept.is_default:
        if "status" in sent_fields:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cannot change the status of the default department; "
                "it is always active for the lifetime of the organization",
            )
        if "slug" in sent_fields:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cannot change the slug of the default department",
            )

    # Handle is_default transfer (must come before other field mutations)
    if "is_default" in sent_fields:
        if body.is_default is False:
            # Unsetting is_default directly is not allowed — designate another
            # department as default instead (which will clear this flag atomically).
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot set is_default to false directly; "
                "designate another department as default instead",
            )

        if body.is_default is True:
            if dept.is_default:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Department '{dept.slug}' is already the default department",
                )

            # Org-admin-only permission check (dept_admin cannot set default)
            check_set_default_permission(auth, dept, db)

            # Target must be ACTIVE — cannot make a deactivated/deleted dept the default
            if dept.status != DepartmentStatus.ACTIVE:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Only an ACTIVE department can be made the default",
                )

            # Atomically clear the current default and set the new one.
            # Fetch the existing default within this org (guaranteed to exist).
            current_default = db.execute(
                select(Department).where(
                    Department.org_id == dept.org_id,
                    Department.is_default.is_(True),
                )
            ).scalar_one_or_none()
            if current_default:
                current_default.is_default = False

            now = datetime.now(timezone.utc)
            dept.is_default = True
            dept.activated_at = now  # Record when it became the default

    # Slug uniqueness check (scoped to org; only if slug is being changed)
    if "slug" in sent_fields and body.slug is not None and body.slug != dept.slug:
        existing = db.execute(
            select(Department).where(
                Department.org_id == dept.org_id,
                Department.slug == body.slug,
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Department with slug '{body.slug}' already exists in this organization",
            )

    # Apply each sent field granularly
    if "name" in sent_fields:
        if body.name is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="name cannot be null",
            )
        dept.name = body.name

    if "slug" in sent_fields:
        if body.slug is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="slug cannot be null",
            )
        dept.slug = body.slug

    if "description" in sent_fields:
        # description is nullable — setting to None clears it
        dept.description = body.description

    if "status" in sent_fields:
        if body.status is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="status cannot be null",
            )
        if body.status == DepartmentStatus.DELETED:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot set status to DELETED via PATCH; use the DELETE endpoint",
            )
        if body.status != dept.status:
            dept.status = body.status
            # Record lifecycle timestamp for the new status
            ts_field = _DEPT_STATUS_TIMESTAMP_FIELD.get(body.status)
            if ts_field:
                setattr(dept, ts_field, datetime.now(timezone.utc))

    db.commit()
    db.refresh(dept)

    return DepartmentResponse.model_validate(dept, from_attributes=True)


@router.delete(
    "/{dept_id_or_slug}",
    summary="Soft-delete a department",
    response_model=DepartmentDeletedResponse,
)
def delete_department(
    dept: Department = Depends(get_department_with_delete_access),
    db: Session = Depends(get_db),
) -> DepartmentDeletedResponse:
    """Soft-delete a department (sets status=DELETED and deleted_at timestamp).

    Default departments (``is_default=True``) cannot be soft-deleted — they
    are only removed when the parent organization is deleted (cascade).

    Idempotency: returns 409 if the department is already soft-deleted,
    so callers can distinguish "just deleted" from "was already deleted".
    The department record is preserved for auditing; hard-delete is not
    supported via API.

    Access requires org_admin (org-level) OR dept_admin (dept-level) permissions.
    """
    if dept.is_default:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot delete the default department; "
            "it is removed only when the organization is deleted",
        )
    if dept.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Department '{dept.slug}' is already deleted",
        )

    now = datetime.now(timezone.utc)
    dept.status = DepartmentStatus.DELETED
    dept.deleted_at = now

    db.commit()
    db.refresh(dept)

    return DepartmentDeletedResponse(
        id=dept.id,
        slug=dept.slug,
        org_id=dept.org_id,
        status=dept.status,
        deleted_at=dept.deleted_at,
    )
