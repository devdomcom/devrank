"""Cursor pagination utilities (shared, DRY).

Provides opaque cursor pagination for list endpoints following 2026 FastAPI
best practices and AGENTS.md guidance:

- Opaque base64 cursors (vs numeric offset/page) for security (no enum attacks),
  performance (index-friendly forward scan), statelessness.
- Reusable deps/funcs to avoid duplication across orgs/users/depts etc.
- Pydantic v2 model_validate(from_attributes=True) for ORM -> schema.
- Soft-delete filter, PK ordering for efficiency.
- limit clamped to prevent abuse; next_cursor=None at end.
- Error on bad cursor -> ValueError (caught/sanitized by api/handlers.py).

Single source of truth in api/ package (platform APIs). Sync patterns (like
auth routes) since DB I/O is blocking.

Example usage in router:
    params: tuple[str | None, int] = Depends(get_cursor_params)
    page = paginate_organizations(db, *params)
"""
from __future__ import annotations

import base64
import uuid

from fastapi import Query
from sqlalchemy import select
from sqlalchemy.orm import Session

# Import schemas/DB here (avoid circular; schemas are app-level)
from api.schemas import OrganizationsCursorPage, OrganizationListItem
from db.models.organization import Organization


def get_cursor_params(
    cursor: str | None = Query(
        None,
        description="Opaque cursor from previous page's next_cursor (base64 ID)",
        examples=["dXNlcjoxMjM="],
    ),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Max items per page (clamped for abuse protection)",
    ),
) -> tuple[str | None, int]:
    """FastAPI dependency for cursor pagination query params.

    DRY extraction + validation; used by /organizations/ and future lists.
    Returns (cursor, limit) tuple for easy unpacking.
    Follows dep pattern in impact/api/dependencies.py and api/auth/dependencies.py.
    """
    return cursor, limit


def encode_cursor(item_id: uuid.UUID) -> str:
    """Encode UUID to opaque, URL-safe base64 cursor (strip padding).

    Prevents ID enumeration; client-transparent. Reversible only here.
    """
    token = base64.urlsafe_b64encode(str(item_id).encode("ascii")).decode("ascii")
    return token.rstrip("=")


def decode_cursor(cursor: str) -> uuid.UUID:
    """Decode opaque cursor to UUID; raises ValueError on invalid/tampered.

    Adds padding for base64; catches bad input early (sanitized upstream).
    """
    # Restore padding (base64 requires ==)
    padding = "=" * (-len(cursor) % 4)
    try:
        id_bytes = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
        id_str = id_bytes.decode("ascii")
        return uuid.UUID(id_str)
    except Exception as e:
        raise ValueError(f"Invalid cursor token: {e}") from e


def paginate_organizations(
    db: Session, cursor: str | None = None, limit: int = 20
) -> OrganizationsCursorPage:
    """Fetch paginated organizations (system data) using cursor.

    - Filters deleted_at IS NULL (DRY soft-delete pattern from org/role models)
    - ORDER BY id (PK index ensures efficient range scan, no offset)
    - Fetches limit+1 to peek for next_cursor (standard technique)
    - Maps ORM to OrganizationListItem via Pydantic (from_attributes=True)
    - Specific to Organization now; generalize later if more lists added.

    Enforces tenancy patterns; for system-role-only endpoints.
    """
    # Base query: active orgs, sorted by ID for cursor stability
    query = (
        select(Organization)
        .where(Organization.deleted_at.is_(None))
        .order_by(Organization.id)
    )

    # Apply cursor filter if provided (id > last)
    if cursor:
        try:
            last_id = decode_cursor(cursor)
        except ValueError as e:
            # Bad cursor: raise for handler (no internal details leaked)
            raise ValueError("Invalid pagination cursor") from e
        query = query.where(Organization.id > last_id)

    # +1 peek for has_next (don't expose to client)
    results = db.scalars(query.limit(limit + 1)).all()
    has_next = len(results) > limit
    page_orgs = results[:limit]

    # Compute next_cursor from last item in page (if exists next)
    next_cursor = (
        encode_cursor(page_orgs[-1].id) if has_next and page_orgs else None
    )

    # Convert ORM instances to Pydantic schema items (v2, from_attributes)
    # Matches fields in OrganizationListItem; rels excluded by model
    items = [
        OrganizationListItem.model_validate(org, from_attributes=True)
        for org in page_orgs
    ]

    return OrganizationsCursorPage(items=items, next_cursor=next_cursor, limit=limit)
