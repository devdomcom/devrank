"""App-wide API schemas (infrastructure, platform-level responses).

Includes platform entities like organizations and departments for tenancy management.
Schemas follow FastAPI/Pydantic best practices (2026): typed responses,
Field validation, reuse of DB enums where possible for DRY.
"""
from __future__ import annotations

from datetime import date, datetime
import uuid
from enum import Enum  # for potential local enums; OrganizationStatus reused from model
from typing import Any

from fastapi import Query
from pydantic import BaseModel, Field, model_validator

# Reuses status enums from db/models for DRY (immutable str Enum;
# compatible with Pydantic v2 model_validate/from_attributes).
# Placed after stdlib/pydantic to follow import order in other api/ files
# (e.g., impact/api/schemas.py, api/routes/auth.py).
from db.models.assessment import AssessmentStatus
from db.models.department import DepartmentStatus
from db.models.organization import OrganizationStatus
from db.models.position import PositionStatus
from db.models.role import RoleStatus
from db.models.user import UserStatus


class HealthResponse(BaseModel):
    """Liveness probe — no external calls, instant response."""
    status: str
    service: str


class ServiceHealth(BaseModel):
    """Health status for a single backing service."""
    status: str
    latency_ms: float | None = None
    version: str | None = None
    error: str | None = None


class InfraHealthResponse(BaseModel):
    """Detailed health check for all backing infrastructure."""
    status: str
    postgres: ServiceHealth
    redis: ServiceHealth


class OrganizationListItem(BaseModel):
    """Summary view of an organization (for list endpoint; excludes heavy rels).

    Mirrors core fields from db/models/organization.py (status, timestamps)
    for API safety. Used in cursor pagination response.

    ``deleted_at`` is nullable — present only for soft-deleted orgs (visible
    to system admins). Non-deleted orgs return ``null``.
    """

    id: uuid.UUID
    name: str
    slug: str
    description: str | None = None
    status: OrganizationStatus
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class OrganizationsCursorPage(BaseModel):
    """Cursor-paginated response for GET /organizations/.

    Follows DRY pagination pattern (reusable for future list endpoints like
    users/depts). Implements opaque cursor best practices (2026 FastAPI):
    - Base64-encoded ID for security/opacity (prevents enumeration)
    - ORDER BY id (indexed PK), WHERE id > decoded_cursor
    - items + next_cursor (None at end); limit in response for client echo
    - No total_count (avoids expensive COUNT(*) on large tenants)
    - limit=20 default, <=100 to prevent abuse
    Aligns with AGENTS.md: Pydantic response_model, dep injection.
    """

    items: list[OrganizationListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class OrganizationResponse(OrganizationListItem):
    """Full detail for single organization (by ID or slug).

    Extends list item (DRY); full timestamps/status from model.
    """
    pass


class CreateOrganizationRequest(BaseModel):
    """Request body for POST /organizations/."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Human-readable organization name (e.g. 'Acme Corporation')",
        examples=["Acme Corporation"],
    )
    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (e.g. 'sample-acme-corp')",
        examples=["sample-acme-corp"],
    )
    description: str | None = Field(None, max_length=500)


class UpdateOrganizationRequest(BaseModel):
    """Request body for PATCH /organizations/{id_or_slug} (partial update).

    All fields optional — only fields present in the request body are applied.
    Callers use ``model_fields_set`` to distinguish "not sent" from "sent as null".
    Follows Pydantic v2 partial-update best practice (2026): explicit None default
    on every field; route checks ``body.model_fields_set`` for granular mutation.

    ``slug`` validated with same constraints as create (unique, URL-safe).
    ``status`` accepts any valid OrganizationStatus for lifecycle transitions.
    ``description`` can be set to null (cleared) or a new value.
    """

    name: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description="Human-readable organization name",
        examples=["Acme Corporation"],
    )
    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (e.g. 'sample-acme-corp')",
        examples=["sample-acme-corp"],
    )
    description: str | None = Field(None, max_length=500)
    status: OrganizationStatus | None = Field(
        None,
        description="Organization lifecycle status (ACTIVE, DEACTIVATED, BANNED, DELETED)",
    )

    @model_validator(mode="after")
    def _reject_null_required_fields(self) -> UpdateOrganizationRequest:
        for field in ("name", "slug", "status"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class OrganizationDeletedResponse(BaseModel):
    """Response for DELETE /organizations/{id_or_slug} (soft-delete).

    Confirms soft-deletion with minimal detail (no full org dump for safety).
    """

    id: uuid.UUID
    slug: str
    status: OrganizationStatus
    deleted_at: datetime


class OrganizationFilterParams(BaseModel):
    """Query-parameter filter bag for GET /organizations/ (extensible).

    Designed for future expansion (date ranges, tags, etc.) without
    breaking the endpoint signature. Currently supports status filtering
    and free-text search across slug/description.
    All fields optional — omitting them returns all (within visibility rules).

    Extracted as a Pydantic model (vs. bare Query params) so the growing
    filter surface stays testable and documented in OpenAPI.
    """

    status: list[OrganizationStatus] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against name, slug, and description "
            "(contains-match on all three)."
        ),
    )


def get_organization_filters(
    status: list[OrganizationStatus] | None = Query(
        None,
        description="Filter by status(es). Repeatable: ?status=ACTIVE&status=BANNED. "
        "Omit to return all statuses visible to the caller.",
        examples=["ACTIVE"],
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description="Search organizations by name, slug, or description (contains-match, "
        "case-insensitive). Example: ?search=sample-acme",
        examples=["sample-acme"],
    ),
) -> OrganizationFilterParams:
    """FastAPI dependency for organization list filters.

    DRY extraction + validation; extensible for future params.
    Strips whitespace from search to normalize user input.
    Returns a typed filter bag for the pagination layer.
    """
    return OrganizationFilterParams(
        status=status or [],
        search=search.strip() if search else None,
    )


# ── Department schemas ─────────────────────────────────────────────────────


class DepartmentListItem(BaseModel):
    """Summary view of a department (for list endpoint; excludes heavy rels).

    Mirrors core fields from db/models/department.py (status, timestamps)
    for API safety. Used in cursor pagination response.

    ``is_default`` indicates the auto-created default department for an org.
    Default departments cannot be soft-deleted or deactivated via API.

    ``deleted_at`` is nullable — present only for soft-deleted departments
    (visible to system admins). Non-deleted departments return ``null``.
    """

    id: uuid.UUID
    org_id: uuid.UUID
    name: str
    slug: str
    description: str | None = None
    is_default: bool = False
    status: DepartmentStatus
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class DepartmentResponse(DepartmentListItem):
    """Full detail for a single department (by ID or slug within an org).

    Extends list item (DRY); adds lifecycle timestamps not shown in list views.
    """

    activated_at: datetime | None = None
    deactivated_at: datetime | None = None


class UpdateDepartmentRequest(BaseModel):
    """Request body for PATCH /organizations/{org}/departments/{dept} (partial update).

    All fields optional — only fields present in the request body are applied.
    Callers use ``model_fields_set`` to distinguish "not sent" from "sent as null".
    Follows Pydantic v2 partial-update best practice (2026): explicit None default
    on every field; route checks ``body.model_fields_set`` for granular mutation.

    ``slug`` validated with same constraints as org slugs (unique per org, URL-safe).
    ``status`` accepts ACTIVE or DEACTIVATED only — DELETED is handled by a
    dedicated DELETE endpoint for audit/lifecycle separation.
    ``description`` can be set to null (cleared) or a new value.
    ``is_default`` — when ``True``, transfers the default flag from the current
    default department to this one (org admins / superusers only). Setting
    ``False`` explicitly is rejected; omitting the field leaves it unchanged.
    Only one department per organization can be the default at any time.
    """

    name: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description="Human-readable department name",
        examples=["Engineering"],
    )
    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (unique per org, e.g. 'sample-engineering')",
        examples=["sample-engineering"],
    )
    description: str | None = Field(None, max_length=500)
    status: DepartmentStatus | None = Field(
        None,
        description="Department lifecycle status (ACTIVE or DEACTIVATED only; "
        "use DELETE endpoint for soft-deletion)",
    )
    is_default: bool | None = Field(
        None,
        description=(
            "Set to true to make this department the org default, transferring "
            "the flag from the current default. Setting to false is not allowed — "
            "designate another department as default instead. "
            "Requires departments:set-default permission (org admins / superusers only)."
        ),
    )

    @model_validator(mode="after")
    def _reject_null_required_fields(self) -> UpdateDepartmentRequest:
        for field in ("name", "slug", "status"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class DepartmentDeletedResponse(BaseModel):
    """Response for DELETE /organizations/{org}/departments/{dept} (soft-delete).

    Confirms soft-deletion with minimal detail (no full dept dump for safety).
    """

    id: uuid.UUID
    slug: str
    org_id: uuid.UUID
    status: DepartmentStatus
    deleted_at: datetime


class DepartmentsCursorPage(BaseModel):
    """Cursor-paginated response for GET /organizations/{org}/departments/.

    Follows DRY pagination pattern (reusable cursor approach from organizations).
    Implements opaque cursor best practices (2026 FastAPI):
    - Base64-encoded ID for security/opacity (prevents enumeration)
    - ORDER BY id (indexed PK), WHERE id > decoded_cursor
    - items + next_cursor (None at end); limit in response for client echo
    - No total_count (avoids expensive COUNT(*) on large tenants)
    - limit=20 default, <=100 to prevent abuse
    """

    items: list[DepartmentListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class DepartmentFilterParams(BaseModel):
    """Query-parameter filter bag for GET /organizations/{org}/departments/ (extensible).

    Designed for future expansion (date ranges, tags, etc.) without
    breaking the endpoint signature. Currently supports status filtering
    and free-text search across slug/description.
    All fields optional — omitting them returns all (within visibility rules).
    """

    status: list[DepartmentStatus] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against name, slug, and "
            "description (contains-match)."
        ),
    )


def get_department_filters(
    status: list[DepartmentStatus] | None = Query(
        None,
        description="Filter by status(es). Repeatable: ?status=ACTIVE&status=DEACTIVATED. "
        "Omit to return all statuses visible to the caller.",
        examples=["ACTIVE"],
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description="Search departments by name, slug, or description (contains-match, "
        "case-insensitive). Example: ?search=sample-engineering",
        examples=["sample-engineering"],
    ),
) -> DepartmentFilterParams:
    """FastAPI dependency for department list filters.

    DRY extraction + validation; extensible for future params.
    Strips whitespace from search to normalize user input.
    Returns a typed filter bag for the pagination layer.
    """
    return DepartmentFilterParams(
        status=status or [],
        search=search.strip() if search else None,
    )


# ── Department creation ───────────────────────────────────────────────────


class CreateDepartmentRequest(BaseModel):
    """Request body for POST /organizations/{org}/departments/.

    Creates a new department within an organization. The slug must be unique
    within the organization (enforced by DB constraint ``uq_org_dept_slug``).
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Human-readable department name (e.g. 'Engineering')",
        examples=["Engineering"],
    )
    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (unique per org, e.g. 'sample-engineering')",
        examples=["sample-engineering"],
    )
    description: str | None = Field(None, max_length=500)


# ── Organization creation response (includes default department) ──────────


class OrganizationCreateResponse(OrganizationResponse):
    """Response for POST /organizations/ (includes auto-created default department).

    Extends OrganizationResponse with the default department that is
    automatically created alongside the organization. This gives the caller
    immediate visibility into the department without a follow-up GET.
    """

    default_department: DepartmentResponse


# ── Position schemas ───────────────────────────────────────────────────────


class PositionResponse(BaseModel):
    """Full detail for a single position (create response / future GET by ID).

    Mirrors all Position model fields for API safety.
    Returned by POST /organizations/{org}/positions/ on successful creation.
    """

    id: uuid.UUID
    org_id: uuid.UUID
    dept_id: uuid.UUID | None = None
    role_id: uuid.UUID
    slug: str
    description: str | None = None
    status: PositionStatus
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None
    deleted_at: datetime | None = None


class PositionDeletedResponse(BaseModel):
    """Response for DELETE /organizations/{org}/positions/{position} (soft-delete).

    Confirms soft-deletion with minimal detail (no full position dump for safety).
    """

    id: uuid.UUID
    slug: str
    org_id: uuid.UUID
    status: PositionStatus
    deleted_at: datetime


class UpdatePositionRequest(BaseModel):
    """Request body for PATCH /organizations/{org}/positions/{position}.

    All fields optional — only fields present are applied. Null values are
    meaningful for nullable fields (e.g. description, dept_id_or_slug).

    ``status`` accepts DRAFT or PUBLISHED; DELETED is rejected (use DELETE endpoint).
    """

    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (globally unique).",
        examples=["sample-acme-eng-senior-engineer"],
    )
    role_id_or_slug: str | None = Field(
        None,
        description="Role identifier or slug for the domain role.",
        examples=["sample-role-senior-engineer"],
    )
    dept_id_or_slug: str | None = Field(
        None,
        description=(
            "Department identifier or slug within the organization. "
            "Set to null to make the position org-wide."
        ),
        examples=["sample-engineering"],
    )
    description: str | None = Field(None, max_length=500)
    status: PositionStatus | None = Field(
        None,
        description="Lifecycle status (DRAFT or PUBLISHED). Use DELETE endpoint for DELETED.",
    )

    @model_validator(mode="after")
    def _reject_null_required_fields(self) -> UpdatePositionRequest:
        for field in ("slug", "status", "role_id_or_slug"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class CreatePositionRequest(BaseModel):
    """Request body for POST /organizations/{org}/positions/.

    Creates a new position within an organization. The slug must be globally
    unique (positions.slug has a UNIQUE constraint). The (org_id, dept_id,
    role_id) triplet must also be unique (enforced by uq_org_dept_role).

    ``dept_id_or_slug`` is optional — omit or set to null for an org-wide
    position not tied to a specific department. When provided it must resolve
    to a department in the same organization.

    ``role_id_or_slug`` must resolve to an existing role in the ``roles`` table.

    ``status`` defaults to DRAFT. Pass PUBLISHED to immediately open the
    position (sets published_at to the current timestamp server-side).
    DELETED is not accepted on creation — use a future DELETE endpoint instead.
    """

    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (globally unique, e.g. 'sample-acme-eng-senior-engineer')",
        examples=["sample-acme-eng-senior-engineer"],
    )
    role_id_or_slug: str = Field(
        ...,
        description=(
            "Role identifier or slug for the domain role this position is for. "
            "Examples: 'sample-role-senior-engineer'."
        ),
        examples=["sample-role-senior-engineer"],
    )
    dept_id_or_slug: str | None = Field(
        None,
        description=(
            "Department identifier or slug within the organization. "
            "Omit or null for an org-wide position."
        ),
        examples=["sample-engineering"],
    )
    description: str | None = Field(None, max_length=500)
    status: PositionStatus = Field(
        PositionStatus.DRAFT,
        description=(
            "Initial lifecycle status. DRAFT (default) or PUBLISHED (opens immediately). "
            "DELETED is not accepted on creation."
        ),
    )


class PositionListItem(BaseModel):
    """Summary view of a position (for list endpoint; excludes heavy rels).

    Mirrors core fields from db/models/position.py (status, timestamps)
    for API safety. Used in cursor pagination response.

    ``dept_id`` is nullable — positions may be org-wide (no department).
    ``deleted_at`` is nullable — present only for soft-deleted positions
    (visible to system admins). Non-deleted positions return ``null``.
    ``published_at`` is set when the position transitions to PUBLISHED.
    """

    id: uuid.UUID
    org_id: uuid.UUID
    dept_id: uuid.UUID | None = None
    role_id: uuid.UUID
    slug: str
    description: str | None = None
    status: PositionStatus
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None
    deleted_at: datetime | None = None


class PositionsCursorPage(BaseModel):
    """Cursor-paginated response for GET /organizations/{org}/positions/.

    Follows DRY pagination pattern (reusable cursor approach from organizations
    and departments). Implements opaque cursor best practices (2026 FastAPI):
    - Base64-encoded ID for security/opacity (prevents enumeration)
    - ORDER BY id (indexed PK), WHERE id > decoded_cursor
    - items + next_cursor (None at end); limit in response for client echo
    - No total_count (avoids expensive COUNT(*) on large tenants)
    - limit=20 default, <=100 to prevent abuse
    """

    items: list[PositionListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class PositionFilterParams(BaseModel):
    """Query-parameter filter bag for GET /organizations/{org}/positions/ (extensible).

    Designed for future expansion without breaking the endpoint signature.
    Supports:
    - ``status`` — filter by one or more PositionStatus values (repeatable).
    - ``depts`` — filter by one or more department identifiers or slugs.
      Each entry can be a UUID or a slug (resolved to UUIDs at query time).
    - ``search`` — case-insensitive contains-match on slug and description.

    ``depts`` is OR-combined: a position matches if its ``dept_id`` is in the
    resolved set. All fields optional — omitting them returns all (within
    visibility rules).
    """

    status: list[PositionStatus] = Field(default_factory=list)
    depts: list[str] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against slug and description "
            "(contains-match on both)."
        ),
    )


def get_position_filters(
    status: list[PositionStatus] | None = Query(
        None,
        description=(
            "Filter by position status(es). Repeatable: ?status=PUBLISHED&status=DRAFT. "
            "Omit to return all statuses visible to the caller."
        ),
        examples=["PUBLISHED"],
    ),
    depts: list[str] | None = Query(
        None,
        description=(
            "Filter by department identifiers or slugs. Repeatable: "
            "?depts=<uuid>&depts=engineering. Each entry can be a UUID or a slug."
        ),
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description=(
            "Search positions by slug or description (contains-match, case-insensitive). "
            "Example: ?search=sample-acme"
        ),
        examples=["sample-acme"],
    ),
) -> PositionFilterParams:
    """FastAPI dependency for position list filters.

    DRY extraction + validation; extensible for future params.
    Strips whitespace from search to normalize user input.
    Returns a typed filter bag for the pagination layer.
    """
    return PositionFilterParams(
        status=status or [],
        depts=depts or [],
        search=search.strip() if search else None,
    )


# ── Assessment schemas ───────────────────────────────────────────────────


class AssessmentListItem(BaseModel):
    """Summary view of an assessment (for list endpoint; excludes heavy rels)."""

    id: uuid.UUID
    org_id: uuid.UUID | None = None
    position_id: uuid.UUID | None = None
    role_id: uuid.UUID | None = None
    title: str
    slug: str
    description: str | None = None
    status: AssessmentStatus
    created_by: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None
    deleted_at: datetime | None = None


class AssessmentsCursorPage(BaseModel):
    """Cursor-paginated response for GET /assessments/."""

    items: list[AssessmentListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class AssessmentResponse(AssessmentListItem):
    """Full detail for a single assessment."""

    pass


class CreateAssessmentRequest(BaseModel):
    """Request body for POST /assessments/."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Human-readable assessment title",
        examples=["Senior Engineer Q1 2026"],
    )
    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe unique slug for the assessment",
        examples=["senior-eng-q1-2026"],
    )
    description: str | None = Field(None, max_length=1000)
    role_id: uuid.UUID | None = Field(
        None, description="Optional role identifier for the assessment"
    )
    org_id: uuid.UUID | None = Field(
        None, description="Optional organization scope for the assessment"
    )
    position_id: uuid.UUID | None = Field(
        None, description="Optional position associated with the assessment"
    )
    status: AssessmentStatus = Field(
        AssessmentStatus.DRAFT,
        description="Initial lifecycle status (DRAFT or PUBLISHED)",
    )


class UpdateAssessmentRequest(BaseModel):
    """Request body for PATCH /assessments/{id} (partial update)."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description="Human-readable assessment title",
    )
    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe unique slug for the assessment",
    )
    description: str | None = Field(None, max_length=1000)
    role_id: uuid.UUID | None = Field(
        None, description="Optional role identifier for the assessment"
    )
    org_id: uuid.UUID | None = Field(
        None, description="Optional organization scope for the assessment"
    )
    position_id: uuid.UUID | None = Field(
        None, description="Optional position associated with the assessment"
    )
    status: AssessmentStatus | None = Field(
        None, description="Lifecycle status (DRAFT or PUBLISHED)"
    )

    @model_validator(mode="after")
    def _reject_null_required_fields(self) -> UpdateAssessmentRequest:
        for field in ("title", "slug", "status"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class AssessmentDeletedResponse(BaseModel):
    """Response for DELETE /assessments/{id} (soft-delete)."""

    id: uuid.UUID
    slug: str
    status: AssessmentStatus
    deleted_at: datetime


class AssessmentFilterParams(BaseModel):
    """Query-parameter filter bag for GET /assessments/ (extensible)."""

    status: list[AssessmentStatus] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against title, slug, and description "
            "(contains-match on all)."
        ),
    )


def get_assessment_filters(
    status: list[AssessmentStatus] | None = Query(
        None,
        description=(
            "Filter by assessment status(es). Repeatable: ?status=PUBLISHED&status=DRAFT. "
            "Omit to return all visible assessments."
        ),
        examples=["PUBLISHED"],
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description=(
            "Search assessments by title, slug, or description (contains-match, "
            "case-insensitive). Example: ?search=senior"
        ),
        examples=["senior"],
    ),
) -> AssessmentFilterParams:
    """FastAPI dependency for assessment list filters."""
    return AssessmentFilterParams(
        status=status or [],
        search=search.strip() if search else None,
    )


# ── Scenario schemas ───────────────────────────────────────────────────────


from db.models.scenario import ScenarioStatus, ScenarioTool


class ScenarioListItem(BaseModel):
    """Summary view of a scenario (for list endpoint; excludes heavy rels)."""

    id: uuid.UUID
    assessment_id: uuid.UUID
    title: str
    slug: str
    description: str | None = None
    is_global: bool = False
    status: ScenarioStatus
    tool: ScenarioTool
    version: int
    duration: int | None = None
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None
    deactivated_at: datetime | None = None
    deleted_at: datetime | None = None


class ScenariosCursorPage(BaseModel):
    """Cursor-paginated response for GET /scenarios/ and GET /assessments/{id}/scenarios/."""

    items: list[ScenarioListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class ScenarioResponse(ScenarioListItem):
    """Full detail for a single scenario."""

    org_id: uuid.UUID | None = None
    dept_id: uuid.UUID | None = None
    files: list[str] | None = None
    system_prompt: str | None = None
    personas: dict[str, Any] | None = None


class CreateScenarioRequest(BaseModel):
    """Request body for POST /assessments/{id}/scenarios/."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Human-readable scenario title",
        examples=["Live Coding Challenge"],
    )
    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe unique slug for the scenario",
        examples=["live-coding-challenge"],
    )
    description: str | None = Field(None, max_length=1000)
    is_global: bool = Field(
        False,
        description="If true, scenario is reusable across all orgs",
    )
    tool: ScenarioTool = Field(
        ScenarioTool.CHAT,
        description="Tool type (CHAT or MEET)",
    )
    version: int = Field(
        1,
        ge=1,
        description="Scenario version for backward compatibility",
    )
    duration: int | None = Field(
        None,
        ge=60,
        description="Max duration in seconds",
    )
    files: list[str] | None = Field(
        None,
        description="Array of S3 file IDs for scenario assets",
    )
    system_prompt: str | None = Field(
        None,
        description="System prompt/instructions (long text)",
    )
    personas: dict[str, Any] | None = Field(
        None,
        description="JSON config for agent personas",
    )
    status: ScenarioStatus = Field(
        ScenarioStatus.DRAFT,
        description="Initial lifecycle status",
    )


class UpdateScenarioRequest(BaseModel):
    """Request body for PATCH /assessments/{aid}/scenarios/{sid}."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description="Human-readable scenario title",
    )
    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe unique slug for the scenario",
    )
    description: str | None = Field(None, max_length=1000)
    is_global: bool | None = Field(
        None,
        description="If true, scenario is reusable across all orgs",
    )
    tool: ScenarioTool | None = Field(
        None,
        description="Tool type (CHAT or MEET)",
    )
    version: int | None = Field(
        None,
        ge=1,
        description="Scenario version for backward compatibility",
    )
    duration: int | None = Field(
        None,
        ge=60,
        description="Max duration in seconds",
    )
    files: list[str] | None = Field(
        None,
        description="Array of S3 file IDs for scenario assets",
    )
    system_prompt: str | None = Field(
        None,
        description="System prompt/instructions (long text)",
    )
    personas: dict[str, Any] | None = Field(
        None,
        description="JSON config for agent personas",
    )
    status: ScenarioStatus | None = Field(
        None,
        description="Lifecycle status",
    )

    @model_validator(mode="after")
    def _reject_null_required_fields(self) -> UpdateScenarioRequest:
        for field in ("title", "slug", "status", "tool"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class ScenarioDeletedResponse(BaseModel):
    """Response for DELETE /assessments/{aid}/scenarios/{sid} (soft-delete)."""

    id: uuid.UUID
    slug: str
    assessment_id: uuid.UUID
    status: ScenarioStatus
    deleted_at: datetime


class ScenarioFilterParams(BaseModel):
    """Query-parameter filter bag for scenario list endpoints."""

    status: list[ScenarioStatus] = Field(default_factory=list)
    tool: list[ScenarioTool] = Field(default_factory=list)
    is_global: bool | None = None
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against title, slug, and description "
            "(contains-match on all)."
        ),
    )


def get_scenario_filters(
    status: list[ScenarioStatus] | None = Query(
        None,
        description=(
            "Filter by scenario status(es). Repeatable: ?status=PUBLISHED&status=DRAFT."
        ),
        examples=["PUBLISHED"],
    ),
    tool: list[ScenarioTool] | None = Query(
        None,
        description="Filter by tool type(s). Repeatable: ?tool=CHAT&tool=MEET.",
        examples=["CHAT"],
    ),
    is_global: bool | None = Query(
        None,
        description="Filter by global flag (true = global scenarios only).",
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description=(
            "Search scenarios by title, slug, or description (contains-match, "
            "case-insensitive). Example: ?search=coding"
        ),
        examples=["coding"],
    ),
) -> ScenarioFilterParams:
    """FastAPI dependency for scenario list filters."""
    return ScenarioFilterParams(
        status=status or [],
        tool=tool or [],
        is_global=is_global,
        search=search.strip() if search else None,
    )


# ── Role schemas ───────────────────────────────────────────────────────────


class RoleListItem(BaseModel):
    """Summary view of a global role (for list endpoint; excludes heavy rels).

    Only global roles (``is_global=True``, ``org_id=None``) are returned by
    the list endpoint — these are the platform-wide defaults available to every
    organization for use with positions and assessments.

    ``published_at`` is set when the role transitions to PUBLISHED.
    ``deleted_at`` is nullable — present only for soft-deleted roles (visible
    to system admins). Non-deleted roles return ``null``.
    ``config`` holds the JSON metric/threshold configuration for the role.
    """

    id: uuid.UUID
    slug: str
    description: str | None = None
    is_global: bool
    status: RoleStatus
    version: int
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None
    deleted_at: datetime | None = None


class RoleResponse(RoleListItem):
    """Full detail for a single role including JSON config.

    Extends ``RoleListItem`` with the ``config`` column containing
    metric/threshold configuration synced from YAML via ``devrank roles sync``.
    """

    config: dict[str, Any]


class RolesCursorPage(BaseModel):
    """Cursor-paginated response for GET /roles/.

    Follows DRY pagination pattern (reusable cursor approach from organizations,
    departments, and positions). Implements opaque cursor best practices (2026):
    - Base64-encoded ID for security/opacity (prevents enumeration)
    - ORDER BY id (indexed PK), WHERE id > decoded_cursor
    - items + next_cursor (None at end); limit in response for client echo
    - No total_count (avoids expensive COUNT(*) on large tables)
    - limit=20 default, <=100 to prevent abuse
    """

    items: list[RoleListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class RoleFilterParams(BaseModel):
    """Query-parameter filter bag for GET /roles/ (extensible).

    Designed for future expansion without breaking the endpoint signature.
    Currently supports status filtering and free-text search on slug/description.
    All fields optional — omitting them returns all non-deleted global roles.
    """

    status: list[RoleStatus] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against slug and description "
            "(contains-match on both)."
        ),
    )


def get_role_filters(
    status: list[RoleStatus] | None = Query(
        None,
        description=(
            "Filter by role status(es). Repeatable: ?status=PUBLISHED&status=DRAFT. "
            "Omit to return all non-deleted global roles."
        ),
        examples=["PUBLISHED"],
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description=(
            "Search roles by slug or description (contains-match, case-insensitive). "
            "Example: ?search=sample-role-senior"
        ),
        examples=["sample-role-senior"],
    ),
) -> RoleFilterParams:
    """FastAPI dependency for global role list filters.

    DRY extraction + validation; extensible for future params.
    Strips whitespace from search to normalize user input.
    Returns a typed filter bag for the pagination layer.
    """
    return RoleFilterParams(
        status=status or [],
        search=search.strip() if search else None,
    )


# ── User schemas ───────────────────────────────────────────────────────────


class UserListItem(BaseModel):
    """Summary view of a user (for list endpoint; excludes sensitive fields).

    Mirrors core fields from db/models/user.py (status, timestamps)
    for API safety. Used in cursor pagination response.
    """

    id: uuid.UUID
    email: str
    name: str
    surname: str
    nickname: str | None = None
    status: UserStatus
    is_verified: bool
    is_kyc_verified: bool
    created_at: datetime
    updated_at: datetime


class UsersCursorPage(BaseModel):
    """Cursor-paginated response for GET /users/.

    Follows DRY pagination pattern (reusable cursor approach from organizations,
    departments, positions, and roles). Implements opaque cursor best practices (2026):
    - Base64-encoded ID for security/opacity (prevents enumeration)
    - ORDER BY id (indexed PK), WHERE id > decoded_cursor
    - items + next_cursor (None at end); limit in response for client echo
    - No total_count (avoids expensive COUNT(*) on large tables)
    - limit=20 default, <=100 to prevent abuse
    """

    items: list[UserListItem]
    next_cursor: str | None = None
    limit: int = Field(default=20, le=100, description="Page size")


class UserFilterParams(BaseModel):
    """Query-parameter filter bag for GET /users/ (extensible).

    Designed for future expansion without breaking the endpoint signature.
    Currently supports status filtering and free-text search on name/email.
    All fields optional — omitting them returns all users.
    """

    status: list[UserStatus] = Field(default_factory=list)
    search: str | None = Field(
        None,
        description=(
            "Case-insensitive search term matched against name, surname, email, "
            "and nickname (contains-match on all)."
        ),
    )


def get_user_filters(
    status: list[UserStatus] | None = Query(
        None,
        description=(
            "Filter by user status(es). Repeatable: ?status=ACTIVE&status=DEACTIVATED. "
            "Omit to return all users."
        ),
        examples=["ACTIVE"],
    ),
    search: str | None = Query(
        None,
        min_length=1,
        max_length=100,
        description=(
            "Search users by name, surname, email, or nickname (contains-match, "
            "case-insensitive). Example: ?search=john"
        ),
        examples=["john"],
    ),
) -> UserFilterParams:
    """FastAPI dependency for user list filters.

    DRY extraction + validation; extensible for future params.
    Strips whitespace from search to normalize user input.
    Returns a typed filter bag for the pagination layer.
    """
    return UserFilterParams(
        status=status or [],
        search=search.strip() if search else None,
    )


class UserOrgMembership(BaseModel):
    """Organization and department membership for a user.

    Returned as part of UserResponse when viewed by system admins.
    """

    org_id: uuid.UUID
    org_name: str
    org_slug: str
    dept_id: uuid.UUID | None = None
    dept_name: str | None = None
    dept_slug: str | None = None
    status: str


class UserResponse(BaseModel):
    """Full user detail response.

    Standard users see only basic info when viewing their own profile.
    System admins see additional org/department membership information.

    The ``memberships`` field is only populated for system admins; regular
    users will see this as null.
    """

    id: uuid.UUID
    email: str
    name: str
    surname: str
    nickname: str | None = None
    avatar: str | None = None
    dob: date | None = None
    address: str | None = None
    zip: str | None = None
    phone: str | None = None
    gender: str | None = None
    country: str | None = None
    timezone: str
    locale: str
    status: UserStatus
    is_verified: bool
    is_kyc_verified: bool
    created_at: datetime
    updated_at: datetime
    last_login_at: datetime | None = None
    # Admin-only field: org/department memberships
    memberships: list[UserOrgMembership] | None = None


class OAuthAccountResponse(BaseModel):
    """OAuth provider linkage summary for a user."""

    id: uuid.UUID
    provider: str
    provider_user_id: str
    created_at: datetime
    updated_at: datetime


class OAuthAccountsResponse(BaseModel):
    """List response for a user's connected OAuth providers."""

    items: list[OAuthAccountResponse]


class OAuthAccountDisconnectedResponse(BaseModel):
    """Response payload when an OAuth account is disconnected."""

    id: uuid.UUID
    user_id: uuid.UUID
    provider: str


class CreateUserRequest(BaseModel):
    """Request body for POST /users/ (system admins only).

    Creates a new user account with basic information.
    Email must be unique across the platform.
    """

    email: str = Field(
        ...,
        min_length=3,
        max_length=255,
        description="User's email address (unique)",
        examples=["user@example.com"],
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="User's first/given name",
        examples=["John"],
    )
    surname: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="User's last/family name",
        examples=["Doe"],
    )
    nickname: str | None = Field(
        None,
        max_length=50,
        description="User's preferred display name",
        examples=["johndoe"],
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=100,
        description="Initial password (min 8 characters)",
        examples=["SecurePass123!"],
    )
    gender: str | None = Field(
        None,
        description="User's gender (m, f, other, prefer_not_to_say)",
        examples=["prefer_not_to_say"],
    )
    country: str | None = Field(
        None,
        min_length=2,
        max_length=2,
        description="ISO-3166-1 alpha-2 country code",
        examples=["US"],
    )
    timezone: str = Field(
        "UTC",
        max_length=50,
        description="IANA timezone",
        examples=["America/New_York"],
    )
    locale: str = Field(
        "en",
        max_length=10,
        description="BCP-47 locale",
        examples=["en"],
    )
    is_verified: bool = Field(
        False,
        description="Whether the user's email is verified",
    )
    is_kyc_verified: bool = Field(
        False,
        description="Whether the user has passed KYC verification",
    )

class UpdateUserRequest(BaseModel):
    """Request body for PATCH /users/{user_id} (system admins only).

    Updates an existing user account. All fields are optional.
    """

    email: str | None = Field(
        None,
        min_length=3,
        max_length=255,
        description="User's email address (unique)",
        examples=["user@example.com"],
    )
    name: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="User's first/given name",
        examples=["John"],
    )
    surname: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="User's last/family name",
        examples=["Doe"],
    )
    nickname: str | None = Field(
        None,
        max_length=50,
        description="User's preferred display name",
        examples=["johndoe"],
    )
    password: str | None = Field(
        None,
        min_length=8,
        max_length=100,
        description="New password (min 8 characters)",
        examples=["NewSecurePass123!"],
    )
    gender: str | None = Field(
        None,
        description="User's gender (m, f, other, prefer_not_to_say)",
        examples=["prefer_not_to_say"],
    )
    country: str | None = Field(
        None,
        min_length=2,
        max_length=2,
        description="ISO-3166-1 alpha-2 country code",
        examples=["US"],
    )
    timezone: str | None = Field(
        None,
        max_length=50,
        description="IANA timezone",
        examples=["America/New_York"],
    )
    locale: str | None = Field(
        None,
        max_length=10,
        description="BCP-47 locale",
        examples=["en"],
    )
    avatar: str | None = Field(
        None,
        max_length=500,
        description="URL to profile avatar",
    )
    dob: date | None = Field(
        None,
        description="Date of birth",
    )
    address: str | None = Field(
        None,
        max_length=255,
        description="Street address",
    )
    zip: str | None = Field(
        None,
        max_length=20,
        description="Postal/ZIP code",
    )
    phone: str | None = Field(
        None,
        max_length=30,
        description="Phone number",
    )
    is_verified: bool | None = Field(
        None,
        description="Whether the user's email is verified",
    )
    is_kyc_verified: bool | None = Field(
        None,
        description="Whether the user has passed KYC verification",
    )
