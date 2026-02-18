"""App-wide API schemas (infrastructure, platform-level responses).

Includes platform entities like organizations for tenancy management.
Schemas follow FastAPI/Pydantic best practices (2026): typed responses,
Field validation, reuse of DB enums where possible for DRY.
"""
from __future__ import annotations

from datetime import datetime
import uuid
from enum import Enum  # for potential local enums; OrganizationStatus reused from model

from pydantic import BaseModel, Field

# Reuses OrganizationStatus enum from db/models for DRY (immutable str Enum;
# compatible with Pydantic v2 model_validate/from_attributes).
# Placed after stdlib/pydantic to follow import order in other api/ files
# (e.g., impact/api/schemas.py, api/routes/auth.py).
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
    """

    id: uuid.UUID
    slug: str
    description: str | None = None
    status: OrganizationStatus
    created_at: datetime
    updated_at: datetime


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
    For org admins/system roles (RBAC 'organizations:read').
    Future: include rel summary (depts count, active users) if needed.
    """

    # Inherits id/slug/description/status/created/updated from ListItem
    # Add scoped fields here if expanding (e.g., dept_count: int = 0)
    pass
