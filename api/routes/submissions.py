"""Submissions routes (assessment-scoped).

Endpoints:
- GET /assessments/{id_or_slug}/submissions/            — list submissions for an assessment
- GET /assessments/{id_or_slug}/submissions/{id}        — submission detail

Submissions inherit access rules from their parent assessment:
- System admins can access all submissions.
- Org/dept roles can access submissions within their org-scoped assessments.
- For unscoped assessments, only the creator may access submissions.
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_db, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_submissions
from api.routes.assessments import _enforce_assessment_access, _resolve_assessment
from api.schemas import (
    SubmissionFilterParams,
    SubmissionResponse,
    SubmissionsCursorPage,
    get_submission_filters,
)
from db.models.submission import Submission, SubmissionStatus

router = APIRouter(tags=["submissions"])


@router.get(
    "/{assessment_id_or_slug}/submissions/",
    summary="List submissions for an assessment",
    response_model=SubmissionsCursorPage,
)
def list_assessment_submissions(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[SubmissionFilterParams, Depends(get_submission_filters)],
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> SubmissionsCursorPage:
    """List submissions belonging to a specific assessment.

    Access is inherited from the parent assessment's org/creator scope.
    """
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    cursor, limit = params
    return paginate_submissions(
        db,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
        assessment_id=assessment.id,
    )


@router.get(
    "/{assessment_id_or_slug}/submissions/{submission_id}",
    summary="Get a submission by ID within an assessment",
    response_model=SubmissionResponse,
)
def get_assessment_submission(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    submission_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "38f68b4a-3d49-4af4-a1c1-ec2a97a3c90d"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> SubmissionResponse:
    """Get a single submission by ID within an assessment."""
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    submission = db.execute(
        select(Submission).where(
            Submission.id == submission_id,
            Submission.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not submission or (
        submission.status == SubmissionStatus.DELETED and not auth.is_system_admin
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Submission '{submission_id}' not found in assessment '{assessment.slug}'",
        )

    return SubmissionResponse.model_validate(submission, from_attributes=True)
