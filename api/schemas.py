"""App-wide API schemas (infrastructure, platform-level responses).

Includes platform entities like organizations and departments for tenancy management.
Schemas follow FastAPI/Pydantic best practices (2026): typed responses,
Field validation, reuse of DB enums where possible for DRY.
"""
from __future__ import annotations

from datetime import datetime
import uuid
from enum import Enum  # for potential local enums; OrganizationStatus reused from model

from fastapi import Query
from pydantic import BaseModel, Field

# Reuses status enums from db/models for DRY (immutable str Enum;
# compatible with Pydantic v2 model_validate/from_attributes).
# Placed after stdlib/pydantic to follow import order in other api/ files
# (e.g., impact/api/schemas.py, api/routes/auth.py).
from db.models.department import DepartmentStatus
from db.models.organization import OrganizationStatus


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
        description="URL-safe lowercase slug (e.g. 'acme-corp')",
        examples=["acme-corp"],
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
        description="URL-safe lowercase slug (e.g. 'acme-corp')",
        examples=["acme-corp"],
    )
    description: str | None = Field(None, max_length=500)
    status: OrganizationStatus | None = Field(
        None,
        description="Organization lifecycle status (ACTIVE, DEACTIVATED, BANNED, DELETED)",
    )


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
        "case-insensitive). Example: ?search=acme",
        examples=["acme"],
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

    slug: str | None = Field(
        None,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (unique per org, e.g. 'engineering')",
        examples=["engineering"],
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
            "Case-insensitive search term matched against slug and description "
            "(contains-match on both)."
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
        description="Search departments by slug or description (contains-match, "
        "case-insensitive). Example: ?search=engineering",
        examples=["engineering"],
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

    slug: str = Field(
        ...,
        min_length=2,
        max_length=100,
        pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
        description="URL-safe lowercase slug (unique per org, e.g. 'engineering')",
        examples=["engineering"],
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
