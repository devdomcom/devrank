"""Assessments routes (platform-wide; org-scoped visibility).

Endpoints:
- GET    /assessments/        — cursor-paginated list
- GET    /assessments/{id_or_slug}  — single assessment detail
- POST   /assessments/              — create assessment
- PATCH  /assessments/{id_or_slug}  — partial update
- DELETE /assessments/{id_or_slug}  — soft-delete

Assessments are either org-scoped (org_id set) or unscoped self-evals
(org_id NULL). Access rules:
- System admins (superuser) can access all assessments.
- Org/dept roles can access assessments scoped to their org.
- For unscoped assessments, only the creator may access them.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import (
    _check_org_scoped_permission,
    get_db,
    require_permission,
)
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_assessments
from api.schemas import (
    AssessmentDeletedResponse,
    AssessmentFilterParams,
    AssessmentResponse,
    AssessmentsCursorPage,
    CreateAssessmentRequest,
    UpdateAssessmentRequest,
    get_assessment_filters,
)
from db.models import AssignmentStatus, Organization, Position, Role, UserRoleAssignment
from db.models.assessment import Assessment, AssessmentStatus

router = APIRouter(prefix="/assessments", tags=["assessments"])


def _get_user_org_ids(db: Session, user_id: uuid.UUID) -> list[uuid.UUID]:
    """Resolve org IDs from active role assignments for a user."""
    rows = db.scalars(
        select(UserRoleAssignment.org_id).where(
            UserRoleAssignment.user_id == user_id,
            UserRoleAssignment.status == AssignmentStatus.ACTIVE,
            UserRoleAssignment.org_id.isnot(None),
        )
    ).all()
    return list({org_id for org_id in rows if org_id is not None})


@router.get(
    "/",
    summary="List assessments (cursor paginated, filterable)",
    response_model=AssessmentsCursorPage,
)
def list_assessments(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[AssessmentFilterParams, Depends(get_assessment_filters)],
    auth: AuthContext = Depends(require_permission("assessments:list")),
    db: Session = Depends(get_db),
) -> AssessmentsCursorPage:
    """List assessments visible to the caller.

    - System admins see all assessments (including deleted by default).
    - Org/dept users see assessments scoped to their orgs plus their own
      unscoped assessments (created_by).
    """
    cursor, limit = params
    if auth.is_system_admin:
        return paginate_assessments(
            db,
            cursor,
            limit,
            filters=filters,
            include_deleted=True,
        )

    org_ids = _get_user_org_ids(db, auth.user_id)
    return paginate_assessments(
        db,
        cursor,
        limit,
        filters=filters,
        include_deleted=False,
        org_ids=org_ids,
        creator_id=auth.user_id,
    )


def _resolve_assessment(
    id_or_slug: str, db: Session, *, allow_deleted: bool = False
) -> Assessment:
    """Resolve assessment by UUID or slug. Raises 404 if not found."""
    assessment = None
    try:
        aid = UUID(id_or_slug)
        assessment = db.get(Assessment, aid)
    except ValueError:
        assessment = db.execute(
            select(Assessment).where(Assessment.slug == id_or_slug)
        ).scalar_one_or_none()
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment '{id_or_slug}' not found",
        )
    if assessment.deleted_at and not allow_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment '{id_or_slug}' not found",
        )
    return assessment


@router.get(
    "/{assessment_id_or_slug}",
    summary="Get assessment by ID or slug",
    response_model=AssessmentResponse,
)
def get_assessment(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> AssessmentResponse:
    """Get a single assessment by ID or slug with RBAC enforcement."""
    assessment = _resolve_assessment(
        assessment_id_or_slug, db,
        allow_deleted=auth.is_system_admin,
    )

    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    return AssessmentResponse.model_validate(assessment, from_attributes=True)


@router.post(
    "/",
    summary="Create a new assessment",
    response_model=AssessmentResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_assessment(
    body: CreateAssessmentRequest,
    auth: AuthContext = Depends(require_permission("assessments:create")),
    db: Session = Depends(get_db),
) -> AssessmentResponse:
    """Create a new assessment.

    When ``org_id`` is provided, the caller must have org-scoped access.
    ``status=DELETED`` is rejected; use DELETE endpoint instead.
    """
    if body.status == AssessmentStatus.DELETED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot create an assessment with status DELETED; use the DELETE endpoint",
        )

    if body.org_id is not None:
        org = db.get(Organization, body.org_id)
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Organization '{body.org_id}' not found",
            )
        _check_org_scoped_permission(auth, org, "assessments:create", db)

    if body.role_id is not None:
        role = db.get(Role, body.role_id)
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role '{body.role_id}' not found",
            )

    if body.position_id is not None:
        position = db.get(Position, body.position_id)
        if not position:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Position '{body.position_id}' not found",
            )
        if body.org_id is not None and position.org_id != body.org_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Position does not belong to the specified organization",
            )

    existing = db.execute(
        select(Assessment).where(Assessment.slug == body.slug)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Assessment with slug '{body.slug}' already exists",
        )

    published_at = (
        datetime.now(timezone.utc)
        if body.status == AssessmentStatus.PUBLISHED
        else None
    )

    assessment = Assessment(
        title=body.title,
        slug=body.slug,
        description=body.description,
        role_id=body.role_id,
        org_id=body.org_id,
        position_id=body.position_id,
        status=body.status,
        created_by=auth.user_id,
        published_at=published_at,
    )
    db.add(assessment)
    db.commit()
    db.refresh(assessment)

    return AssessmentResponse.model_validate(assessment, from_attributes=True)


@router.patch(
    "/{assessment_id_or_slug}",
    summary="Partially update an assessment",
    response_model=AssessmentResponse,
)
def update_assessment(
    body: UpdateAssessmentRequest,
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:update")),
    db: Session = Depends(get_db),
) -> AssessmentResponse:
    """Partially update an assessment's properties."""
    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    assessment = _resolve_assessment(assessment_id_or_slug, db)

    _enforce_assessment_access(assessment, auth, db, "assessments:update")

    if "slug" in sent_fields and body.slug is not None and body.slug != assessment.slug:
        existing = db.execute(
            select(Assessment).where(Assessment.slug == body.slug)
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Assessment with slug '{body.slug}' already exists",
            )
        assessment.slug = body.slug

    if "title" in sent_fields:
        if body.title is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="title cannot be null",
            )
        assessment.title = body.title

    if "description" in sent_fields:
        assessment.description = body.description

    if "org_id" in sent_fields:
        if body.org_id is not None:
            org = db.get(Organization, body.org_id)
            if not org:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Organization '{body.org_id}' not found",
                )
            _check_org_scoped_permission(auth, org, "assessments:update", db)
        assessment.org_id = body.org_id

    if "role_id" in sent_fields:
        if body.role_id is not None:
            role = db.get(Role, body.role_id)
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Role '{body.role_id}' not found",
                )
        assessment.role_id = body.role_id

    if "position_id" in sent_fields:
        if body.position_id is not None:
            position = db.get(Position, body.position_id)
            if not position:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Position '{body.position_id}' not found",
                )
            if assessment.org_id and position.org_id != assessment.org_id:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Position does not belong to the assessment's organization",
                )
        assessment.position_id = body.position_id

    if "status" in sent_fields:
        if body.status is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="status cannot be null",
            )
        if body.status == AssessmentStatus.DELETED:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot set status to DELETED via PATCH; use the DELETE endpoint",
            )
        if body.status != assessment.status:
            assessment.status = body.status
            if body.status == AssessmentStatus.PUBLISHED and assessment.published_at is None:
                assessment.published_at = datetime.now(timezone.utc)
            if body.status == AssessmentStatus.DRAFT:
                assessment.published_at = None

    db.commit()
    db.refresh(assessment)

    return AssessmentResponse.model_validate(assessment, from_attributes=True)


@router.delete(
    "/{assessment_id_or_slug}",
    summary="Soft-delete an assessment",
    response_model=AssessmentDeletedResponse,
)
def delete_assessment(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:delete")),
    db: Session = Depends(get_db),
) -> AssessmentDeletedResponse:
    """Soft-delete an assessment (sets status=DELETED and deleted_at timestamp)."""
    assessment = _resolve_assessment(
        assessment_id_or_slug, db,
        allow_deleted=auth.is_system_admin,
    )

    _enforce_assessment_access(assessment, auth, db, "assessments:delete")

    if assessment.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Assessment '{assessment.slug}' is already deleted",
        )

    assessment.status = AssessmentStatus.DELETED
    assessment.deleted_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(assessment)

    return AssessmentDeletedResponse(
        id=assessment.id,
        slug=assessment.slug,
        status=assessment.status,
        deleted_at=assessment.deleted_at,
    )


def _enforce_assessment_access(
    assessment: Assessment,
    auth: AuthContext,
    db: Session,
    permission_slug: str,
) -> None:
    """Enforce org/self access rules for assessments."""
    if auth.is_system_admin:
        return

    if assessment.org_id is None:
        if assessment.created_by != auth.user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access this assessment",
            )
        return

    org = db.get(Organization, assessment.org_id)
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organization '{assessment.org_id}' not found",
        )
    _check_org_scoped_permission(auth, org, permission_slug, db)
