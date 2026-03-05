"""Evaluations routes (assessment- and submission-scoped).

Endpoints:
- GET    /assessments/{id_or_slug}/evaluations/                       — list evaluations for an assessment
- GET    /assessments/{id_or_slug}/evaluations/{evaluation_id}        — evaluation detail
- POST   /assessments/{id_or_slug}/evaluations/                       — create evaluation (system admins only)
- PATCH  /assessments/{id_or_slug}/evaluations/{evaluation_id}        — update evaluation (system admins only)
- DELETE /assessments/{id_or_slug}/evaluations/{evaluation_id}        — delete evaluation (system admins only)
- GET    /assessments/{id_or_slug}/submissions/{submission_id}/evaluations/            — list evaluations for a submission
- GET    /assessments/{id_or_slug}/submissions/{submission_id}/evaluations/{evaluation_id} — evaluation detail within submission

Evaluations inherit access rules from their parent assessment:
- System admins can access all evaluations.
- Org/dept roles can access evaluations within their org-scoped assessments.
- For unscoped assessments, only the creator may access evaluations.
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_db, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_evaluations
from api.routes.assessments import _enforce_assessment_access, _resolve_assessment
from api.schemas import (
    CreateEvaluationRequest,
    EvaluationFilterParams,
    EvaluationResponse,
    EvaluationsCursorPage,
    UpdateEvaluationRequest,
    get_evaluation_filters,
)
from db.models.evaluation import Evaluation
from db.models.submission import Submission

router = APIRouter(tags=["evaluations"])


@router.get(
    "/{assessment_id_or_slug}/evaluations/",
    summary="List evaluations for an assessment",
    response_model=EvaluationsCursorPage,
)
def list_assessment_evaluations(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[EvaluationFilterParams, Depends(get_evaluation_filters)],
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> EvaluationsCursorPage:
    """List evaluations belonging to a specific assessment.

    Access is inherited from the parent assessment's org/creator scope.
    """
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    cursor, limit = params
    return paginate_evaluations(
        db,
        cursor,
        limit,
        filters=filters,
        assessment_id=assessment.id,
    )


@router.get(
    "/{assessment_id_or_slug}/evaluations/{evaluation_id}",
    summary="Get an evaluation by ID within an assessment",
    response_model=EvaluationResponse,
)
def get_assessment_evaluation(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    evaluation_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "4e78a25a-12f8-4f92-8f7e-2a6d9152d1c3"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> EvaluationResponse:
    """Get a single evaluation by ID within an assessment."""
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    evaluation = db.execute(
        select(Evaluation).where(
            Evaluation.id == evaluation_id,
            Evaluation.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not evaluation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Evaluation '{evaluation_id}' not found in assessment '{assessment.slug}'",
        )

    return EvaluationResponse.model_validate(evaluation, from_attributes=True)


@router.post(
    "/{assessment_id_or_slug}/evaluations/",
    summary="Create a new evaluation for an assessment",
    response_model=EvaluationResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_assessment_evaluation(
    body: CreateEvaluationRequest,
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:update")),
    db: Session = Depends(get_db),
) -> EvaluationResponse:
    """Create a new evaluation within an assessment (system admins only)."""
    if not auth.is_system_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to create evaluations",
        )

    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=True)

    submission_id = body.submission_id
    if submission_id is not None:
        submission = db.execute(
            select(Submission).where(
                Submission.id == submission_id,
                Submission.assessment_id == assessment.id,
            )
        ).scalar_one_or_none()
        if not submission:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Submission '{submission_id}' not found in assessment '{assessment.slug}'",
            )

    evaluation = Evaluation(
        assessment_id=assessment.id,
        submission_id=submission_id,
        summary=body.summary,
    )
    db.add(evaluation)
    db.commit()
    db.refresh(evaluation)

    if submission_id is not None:
        submission = db.get(Submission, submission_id)
        if submission:
            submission.evaluation_id = evaluation.id
            db.commit()

    return EvaluationResponse.model_validate(evaluation, from_attributes=True)


@router.patch(
    "/{assessment_id_or_slug}/evaluations/{evaluation_id}",
    summary="Partially update an evaluation",
    response_model=EvaluationResponse,
)
def update_assessment_evaluation(
    body: UpdateEvaluationRequest,
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    evaluation_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "4e78a25a-12f8-4f92-8f7e-2a6d9152d1c3"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:update")),
    db: Session = Depends(get_db),
) -> EvaluationResponse:
    """Partially update an evaluation within an assessment (system admins only)."""
    if not auth.is_system_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update evaluations",
        )

    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=True)

    evaluation = db.execute(
        select(Evaluation).where(
            Evaluation.id == evaluation_id,
            Evaluation.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not evaluation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Evaluation '{evaluation_id}' not found in assessment '{assessment.slug}'",
        )

    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    if "submission_id" in sent_fields:
        submission_id = body.submission_id
        if submission_id is not None:
            submission = db.execute(
                select(Submission).where(
                    Submission.id == submission_id,
                    Submission.assessment_id == assessment.id,
                )
            ).scalar_one_or_none()
            if not submission:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Submission '{submission_id}' not found in assessment '{assessment.slug}'",
                )
        evaluation.submission_id = submission_id

    if "summary" in sent_fields:
        evaluation.summary = body.summary

    db.commit()
    db.refresh(evaluation)

    if "submission_id" in sent_fields:
        if evaluation.submission_id is not None:
            submission = db.get(Submission, evaluation.submission_id)
            if submission:
                submission.evaluation_id = evaluation.id
        db.commit()

    return EvaluationResponse.model_validate(evaluation, from_attributes=True)


@router.delete(
    "/{assessment_id_or_slug}/evaluations/{evaluation_id}",
    summary="Delete an evaluation",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_assessment_evaluation(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    evaluation_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "4e78a25a-12f8-4f92-8f7e-2a6d9152d1c3"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:delete")),
    db: Session = Depends(get_db),
) -> None:
    """Delete an evaluation within an assessment (system admins only)."""
    if not auth.is_system_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete evaluations",
        )

    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=True)

    evaluation = db.execute(
        select(Evaluation).where(
            Evaluation.id == evaluation_id,
            Evaluation.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not evaluation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Evaluation '{evaluation_id}' not found in assessment '{assessment.slug}'",
        )

    if evaluation.submission_id is not None:
        submission = db.get(Submission, evaluation.submission_id)
        if submission and submission.evaluation_id == evaluation.id:
            submission.evaluation_id = None

    db.delete(evaluation)
    db.commit()


@router.get(
    "/{assessment_id_or_slug}/submissions/{submission_id}/evaluations/",
    summary="List evaluations for a submission",
    response_model=EvaluationsCursorPage,
)
def list_submission_evaluations(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    submission_id: uuid.UUID,
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> EvaluationsCursorPage:
    """List evaluations for a submission within an assessment."""
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    submission = db.execute(
        select(Submission).where(
            Submission.id == submission_id,
            Submission.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not submission:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Submission '{submission_id}' not found in assessment '{assessment.slug}'",
        )

    cursor, limit = params
    return paginate_evaluations(
        db,
        cursor,
        limit,
        filters=EvaluationFilterParams(submission_ids=[submission.id]),
        assessment_id=assessment.id,
    )


@router.get(
    "/{assessment_id_or_slug}/submissions/{submission_id}/evaluations/{evaluation_id}",
    summary="Get an evaluation by ID within a submission",
    response_model=EvaluationResponse,
)
def get_submission_evaluation(
    assessment_id_or_slug: str = Path(
        openapi_examples={"default": {"value": "senior-eng-q1-2026"}},
    ),
    submission_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "38f68b4a-3d49-4af4-a1c1-ec2a97a3c90d"}},
    ),
    evaluation_id: uuid.UUID = Path(
        openapi_examples={"default": {"value": "4e78a25a-12f8-4f92-8f7e-2a6d9152d1c3"}},
    ),
    auth: AuthContext = Depends(require_permission("assessments:read")),
    db: Session = Depends(get_db),
) -> EvaluationResponse:
    """Get a single evaluation by ID within a submission."""
    assessment = _resolve_assessment(assessment_id_or_slug, db, allow_deleted=auth.is_system_admin)
    _enforce_assessment_access(assessment, auth, db, "assessments:read")

    submission = db.execute(
        select(Submission).where(
            Submission.id == submission_id,
            Submission.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()
    if not submission:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Submission '{submission_id}' not found in assessment '{assessment.slug}'",
        )

    evaluation = db.execute(
        select(Evaluation).where(
            Evaluation.id == evaluation_id,
            Evaluation.assessment_id == assessment.id,
            Evaluation.submission_id == submission.id,
        )
    ).scalar_one_or_none()
    if not evaluation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Evaluation '{evaluation_id}' not found in submission '{submission_id}' "
                f"for assessment '{assessment.slug}'"
            ),
        )

    return EvaluationResponse.model_validate(evaluation, from_attributes=True)
