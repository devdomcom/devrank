"""Scenarios routes (global platform listing + assessment-scoped CRUD).

Endpoints:
- GET    /scenarios/                                  — global scenarios (is_global=True, searchable)
- GET    /assessments/{assessment_id}/scenarios/      — list scenarios for an assessment
- GET    /assessments/{assessment_id}/scenarios/{sid} — single scenario detail
- POST   /assessments/{assessment_id}/scenarios/      — create scenario in assessment
- PATCH  /assessments/{assessment_id}/scenarios/{sid} — partial update
- DELETE /assessments/{assessment_id}/scenarios/{sid} — soft-delete

Global scenarios (is_global=True) are visible platform-wide.
Assessment-scoped scenarios inherit access rules from their parent assessment:
- System admins (superuser) can access all scenarios.
- Org/dept roles can access scenarios in assessments scoped to their org.
- For unscoped assessments, only the creator may access their scenarios.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import (
    _check_org_scoped_permission,
    get_db,
    require_permission,
)
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_scenarios
from api.schemas import (
    CreateScenarioRequest,
    ScenarioDeletedResponse,
    ScenarioFilterParams,
    ScenarioResponse,
    ScenariosCursorPage,
    UpdateScenarioRequest,
    get_scenario_filters,
)
from db.models.assessment import Assessment
from db.models.organization import Organization
from db.models.scenario import Scenario, ScenarioStatus

# ── Routers ──────────────────────────────────────────────────────────────

router = APIRouter(prefix="/scenarios", tags=["scenarios"])

assessment_router = APIRouter(tags=["scenarios"])

# ── Helpers ──────────────────────────────────────────────────────────────


def _resolve_assessment(
    assessment_id: uuid.UUID, db: Session
) -> Assessment:
    """Fetch assessment by UUID or raise 404."""
    assessment = db.get(Assessment, assessment_id)
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment '{assessment_id}' not found",
        )
    return assessment


def _enforce_assessment_access(
    assessment: Assessment,
    auth: AuthContext,
    db: Session,
    permission_slug: str,
) -> None:
    """Enforce org/self access rules inherited from the parent assessment.

    Mirrors assessments._enforce_assessment_access (DRY logic, local copy to
    avoid cross-router import coupling).
    """
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


def _resolve_scenario(
    scenario_id: uuid.UUID,
    assessment: Assessment,
    db: Session,
    *,
    allow_deleted: bool = False,
) -> Scenario:
    """Fetch scenario by UUID scoped to an assessment, or raise 404."""
    scenario = db.execute(
        select(Scenario).where(
            Scenario.id == scenario_id,
            Scenario.assessment_id == assessment.id,
        )
    ).scalar_one_or_none()

    if not scenario:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found in assessment '{assessment.slug}'",
        )
    if scenario.deleted_at and not allow_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found in assessment '{assessment.slug}'",
        )
    return scenario


# ── GET /scenarios/ (global, platform-wide) ──────────────────────────────


@router.get(
    "/",
    summary="List global scenarios (platform-wide, searchable)",
    response_model=ScenariosCursorPage,
)
def list_global_scenarios(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[ScenarioFilterParams, Depends(get_scenario_filters)],
    auth: AuthContext = Depends(require_permission("scenarios:list")),
    db: Session = Depends(get_db),
) -> ScenariosCursorPage:
    """List global scenarios (is_global=True) visible to all authenticated users.

    System admins see all global scenarios including deleted.
    Other users see only non-deleted global scenarios.
    Filterable by status, tool, and free-text search (title/slug/description).
    """
    cursor, limit = params
    return paginate_scenarios(
        db,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
        is_global=True,
    )


# ── Assessment-scoped CRUD ───────────────────────────────────────────────


@assessment_router.get(
    "/{assessment_id}/scenarios/",
    summary="List scenarios for an assessment",
    response_model=ScenariosCursorPage,
)
def list_assessment_scenarios(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[ScenarioFilterParams, Depends(get_scenario_filters)],
    assessment_id: uuid.UUID = Path(..., description="Assessment UUID"),
    auth: AuthContext = Depends(require_permission("scenarios:list")),
    db: Session = Depends(get_db),
) -> ScenariosCursorPage:
    """List scenarios belonging to a specific assessment.

    Access is inherited from the parent assessment's org/creator scope.
    """
    assessment = _resolve_assessment(assessment_id, db)
    _enforce_assessment_access(assessment, auth, db, "scenarios:list")

    cursor, limit = params
    return paginate_scenarios(
        db,
        cursor,
        limit,
        filters=filters,
        include_deleted=auth.is_system_admin,
        assessment_id=assessment.id,
    )


@assessment_router.get(
    "/{assessment_id}/scenarios/{scenario_id}",
    summary="Get scenario by ID",
    response_model=ScenarioResponse,
)
def get_scenario(
    assessment_id: uuid.UUID = Path(..., description="Assessment UUID"),
    scenario_id: uuid.UUID = Path(..., description="Scenario UUID"),
    auth: AuthContext = Depends(require_permission("scenarios:read")),
    db: Session = Depends(get_db),
) -> ScenarioResponse:
    """Get a single scenario by ID within an assessment."""
    assessment = _resolve_assessment(assessment_id, db)
    _enforce_assessment_access(assessment, auth, db, "scenarios:read")

    scenario = _resolve_scenario(
        scenario_id, assessment, db,
        allow_deleted=auth.is_system_admin,
    )
    return ScenarioResponse.model_validate(scenario, from_attributes=True)


@assessment_router.post(
    "/{assessment_id}/scenarios/",
    summary="Create a new scenario in an assessment",
    response_model=ScenarioResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_scenario(
    body: CreateScenarioRequest,
    assessment_id: uuid.UUID = Path(..., description="Assessment UUID"),
    auth: AuthContext = Depends(require_permission("scenarios:create")),
    db: Session = Depends(get_db),
) -> ScenarioResponse:
    """Create a new scenario within an assessment.

    Access is inherited from the parent assessment's org/creator scope.
    ``status=DELETED`` is rejected; use the DELETE endpoint instead.
    Slug must be globally unique.
    """
    if body.status == ScenarioStatus.DELETED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot create a scenario with status DELETED; use the DELETE endpoint",
        )

    assessment = _resolve_assessment(assessment_id, db)
    _enforce_assessment_access(assessment, auth, db, "scenarios:create")

    # Slug uniqueness (global)
    existing = db.execute(
        select(Scenario).where(Scenario.slug == body.slug)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Scenario with slug '{body.slug}' already exists",
        )

    published_at = (
        datetime.now(timezone.utc)
        if body.status == ScenarioStatus.PUBLISHED
        else None
    )

    scenario = Scenario(
        assessment_id=assessment.id,
        title=body.title,
        slug=body.slug,
        description=body.description,
        is_global=body.is_global,
        tool=body.tool,
        version=body.version,
        duration=body.duration,
        files=body.files,
        system_prompt=body.system_prompt,
        personas=body.personas,
        status=body.status,
        org_id=assessment.org_id,
        dept_id=None,
        created_by=auth.user_id,
        published_at=published_at,
    )
    db.add(scenario)
    db.commit()
    db.refresh(scenario)

    return ScenarioResponse.model_validate(scenario, from_attributes=True)


@assessment_router.patch(
    "/{assessment_id}/scenarios/{scenario_id}",
    summary="Partially update a scenario",
    response_model=ScenarioResponse,
)
def update_scenario(
    body: UpdateScenarioRequest,
    assessment_id: uuid.UUID = Path(..., description="Assessment UUID"),
    scenario_id: uuid.UUID = Path(..., description="Scenario UUID"),
    auth: AuthContext = Depends(require_permission("scenarios:update")),
    db: Session = Depends(get_db),
) -> ScenarioResponse:
    """Partially update a scenario's properties.

    Only fields present in the request body are applied (true PATCH semantics).
    Uses Pydantic v2 ``model_fields_set`` to distinguish "not sent" from
    "explicitly sent as null".
    """
    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    assessment = _resolve_assessment(assessment_id, db)
    _enforce_assessment_access(assessment, auth, db, "scenarios:update")
    scenario = _resolve_scenario(scenario_id, assessment, db)

    # Slug uniqueness (if changing)
    if "slug" in sent_fields and body.slug != scenario.slug:
        existing = db.execute(
            select(Scenario).where(Scenario.slug == body.slug)
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Scenario with slug '{body.slug}' already exists",
            )
        scenario.slug = body.slug

    # Simple fields
    if "title" in sent_fields:
        scenario.title = body.title
    if "description" in sent_fields:
        scenario.description = body.description
    if "is_global" in sent_fields:
        scenario.is_global = body.is_global
    if "tool" in sent_fields:
        scenario.tool = body.tool
    if "version" in sent_fields:
        scenario.version = body.version
    if "duration" in sent_fields:
        scenario.duration = body.duration
    if "files" in sent_fields:
        scenario.files = body.files
    if "system_prompt" in sent_fields:
        scenario.system_prompt = body.system_prompt
    if "personas" in sent_fields:
        scenario.personas = body.personas

    # Status transitions with lifecycle timestamps
    if "status" in sent_fields:
        if body.status == ScenarioStatus.DELETED:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot set status to DELETED via PATCH; use the DELETE endpoint",
            )
        if body.status != scenario.status:
            scenario.status = body.status
            if body.status == ScenarioStatus.PUBLISHED and scenario.published_at is None:
                scenario.published_at = datetime.now(timezone.utc)
            if body.status == ScenarioStatus.DEACTIVATED:
                scenario.deactivated_at = datetime.now(timezone.utc)
            if body.status == ScenarioStatus.DRAFT:
                scenario.published_at = None
                scenario.deactivated_at = None

    db.commit()
    db.refresh(scenario)

    return ScenarioResponse.model_validate(scenario, from_attributes=True)


@assessment_router.delete(
    "/{assessment_id}/scenarios/{scenario_id}",
    summary="Soft-delete a scenario",
    response_model=ScenarioDeletedResponse,
)
def delete_scenario(
    assessment_id: uuid.UUID = Path(..., description="Assessment UUID"),
    scenario_id: uuid.UUID = Path(..., description="Scenario UUID"),
    auth: AuthContext = Depends(require_permission("scenarios:delete")),
    db: Session = Depends(get_db),
) -> ScenarioDeletedResponse:
    """Soft-delete a scenario (sets status=DELETED and deleted_at timestamp)."""
    assessment = _resolve_assessment(assessment_id, db)
    _enforce_assessment_access(assessment, auth, db, "scenarios:delete")
    scenario = _resolve_scenario(scenario_id, assessment, db, allow_deleted=True)

    if scenario.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Scenario '{scenario.slug}' is already deleted",
        )

    scenario.status = ScenarioStatus.DELETED
    scenario.deleted_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(scenario)

    return ScenarioDeletedResponse(
        id=scenario.id,
        slug=scenario.slug,
        assessment_id=scenario.assessment_id,
        status=scenario.status,
        deleted_at=scenario.deleted_at,
    )
