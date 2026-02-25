"""Cursor pagination utilities (shared, DRY).

Provides opaque cursor pagination for list endpoints following 2026 FastAPI
best practices and AGENTS.md guidance:

- Opaque base64 cursors (vs numeric offset/page) for security (no enum attacks),
  performance (index-friendly forward scan), statelessness.
- Reusable deps/funcs to avoid duplication across orgs/users/depts etc.
- Pydantic v2 model_validate(from_attributes=True) for ORM -> schema.
- Soft-delete filter (configurable via include_deleted), PK ordering for efficiency.
- Status filtering + free-text search via filter params (extensible).
- Search: contains-matched (ILIKE '%term%') on relevant text fields.
  User-supplied ``%``/``_`` wildcards escaped. For large tables consider adding
  ``pg_trgm`` GIN indexes on name and description.
- limit clamped to prevent abuse; next_cursor=None at end.
- Error on bad cursor -> ValueError (caught/sanitized by api/handlers.py).

Single source of truth in api/ package (platform APIs). Sync patterns (like
auth routes) since DB I/O is blocking.

Example usage in router:
    params: tuple[str | None, int] = Depends(get_cursor_params)
    page = paginate_organizations(db, *params, filters=filters, include_deleted=auth.is_system_admin)
    page = paginate_departments(db, org_id, *params, filters=filters, include_deleted=auth.is_system_admin)
    page = paginate_positions(db, org_id, *params, filters=filters, include_deleted=auth.is_system_admin)
"""
from __future__ import annotations

import base64
import uuid

from fastapi import Query
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

# Import schemas/DB here (avoid circular; schemas are app-level)
from api.schemas import (
    DepartmentFilterParams,
    DepartmentListItem,
    DepartmentsCursorPage,
    OrganizationFilterParams,
    OrganizationListItem,
    OrganizationsCursorPage,
    PositionFilterParams,
    PositionListItem,
    PositionsCursorPage,
    RoleFilterParams,
    RoleListItem,
    RolesCursorPage,
)
from db.models.department import Department
from db.models.organization import Organization
from db.models.position import Position
from db.models.role import Role


def _escape_like(term: str) -> str:
    """Escape LIKE/ILIKE wildcards in user-supplied search input.

    Prevents ``%`` and ``_`` in the term from acting as SQL wildcards.
    Uses backslash as the escape character (Postgres default for ILIKE).
    """
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


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
    db: Session,
    cursor: str | None = None,
    limit: int = 20,
    *,
    filters: OrganizationFilterParams | None = None,
    include_deleted: bool = False,
) -> OrganizationsCursorPage:
    """Fetch paginated organizations (system data) using cursor.

    - Soft-delete filter: ``include_deleted=False`` (default) hides deleted orgs;
      ``True`` (system admins) includes them. Status filters override this when
      DELETED is explicitly requested.
    - Status filter: narrows to specific statuses when provided.
    - Search filter: case-insensitive contains-match on name, slug, and
      description. User-supplied wildcards are escaped for safety.
    - ORDER BY id (PK index ensures efficient range scan, no offset)
    - Fetches limit+1 to peek for next_cursor (standard technique)
    - Maps ORM to OrganizationListItem via Pydantic (from_attributes=True)

    Enforces tenancy patterns; for system-role-only endpoints.
    """
    # Base query sorted by ID for cursor stability
    query = select(Organization).order_by(Organization.id)

    # Apply status filter if provided
    if filters and filters.status:
        query = query.where(Organization.status.in_(filters.status))
    elif not include_deleted:
        # No explicit status filter: hide soft-deleted for non-system-admins
        query = query.where(Organization.deleted_at.is_(None))

    # Apply search filter (name/slug/description contains, case-insensitive)
    if filters and filters.search:
        safe_term = _escape_like(filters.search)
        query = query.where(
            or_(
                # Contains match on name — primary human-readable field.
                Organization.name.ilike(f"%{safe_term}%"),
                # Contains match on slug — e.g. "acme" matches "sample-acme-corp".
                Organization.slug.ilike(f"%{safe_term}%"),
                # Contains match on description.
                Organization.description.ilike(f"%{safe_term}%"),
            )
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


def paginate_departments(
    db: Session,
    org_id: uuid.UUID,
    cursor: str | None = None,
    limit: int = 20,
    *,
    filters: DepartmentFilterParams | None = None,
    include_deleted: bool = False,
) -> DepartmentsCursorPage:
    """Fetch paginated departments for a specific organization using cursor.

    - Scoped to ``org_id`` (FK filter; enforces tenancy at query level).
    - Soft-delete filter: ``include_deleted=False`` (default) hides deleted depts;
      ``True`` (system admins) includes them. Status filters override this when
      DELETED is explicitly requested.
    - Status filter: narrows to specific statuses when provided.
    - Search filter: case-insensitive contains-match on slug and description.
      User-supplied wildcards are escaped for safety.
    - ORDER BY id (PK index ensures efficient range scan, no offset)
    - Fetches limit+1 to peek for next_cursor (standard technique)
    - Maps ORM to DepartmentListItem via Pydantic (from_attributes=True)

    Enforces tenancy patterns; departments are always scoped to an org.
    """
    # Base query: scoped to org, sorted by ID for cursor stability
    query = (
        select(Department)
        .where(Department.org_id == org_id)
        .order_by(Department.id)
    )

    # Apply status filter if provided
    if filters and filters.status:
        query = query.where(Department.status.in_(filters.status))
    elif not include_deleted:
        # No explicit status filter: hide soft-deleted for non-system-admins
        query = query.where(Department.deleted_at.is_(None))

    # Apply search filter (name/slug/description contains, case-insensitive)
    if filters and filters.search:
        safe_term = _escape_like(filters.search)
        query = query.where(
            or_(
                Department.name.ilike(f"%{safe_term}%"),
                Department.slug.ilike(f"%{safe_term}%"),
                Department.description.ilike(f"%{safe_term}%"),
            )
        )

    # Apply cursor filter if provided (id > last)
    if cursor:
        try:
            last_id = decode_cursor(cursor)
        except ValueError as e:
            raise ValueError("Invalid pagination cursor") from e
        query = query.where(Department.id > last_id)

    # +1 peek for has_next (don't expose to client)
    results = db.scalars(query.limit(limit + 1)).all()
    has_next = len(results) > limit
    page_depts = results[:limit]

    # Compute next_cursor from last item in page (if exists next)
    next_cursor = (
        encode_cursor(page_depts[-1].id) if has_next and page_depts else None
    )

    # Convert ORM instances to Pydantic schema items (v2, from_attributes)
    items = [
        DepartmentListItem.model_validate(dept, from_attributes=True)
        for dept in page_depts
    ]

    return DepartmentsCursorPage(items=items, next_cursor=next_cursor, limit=limit)


def paginate_positions(
    db: Session,
    org_id: uuid.UUID,
    cursor: str | None = None,
    limit: int = 20,
    *,
    filters: PositionFilterParams | None = None,
    include_deleted: bool = False,
) -> PositionsCursorPage:
    """Fetch paginated open (PUBLISHED) positions for an organization using cursor.

    - Scoped to ``org_id`` (FK filter; enforces tenancy at query level).
    - Default behaviour: only PUBLISHED positions are returned (the "open"
      positions visible to external callers). System admins can override via
      explicit ``status`` filters or by passing ``include_deleted=True``.
    - Soft-delete filter: ``include_deleted=False`` (default) hides deleted
      positions; ``True`` (system admins) includes them unless an explicit
      status filter is provided.
    - Department filter: ``dept_ids`` and/or ``dept_slugs`` narrow results to
      positions whose ``dept_id`` matches. Slugs are resolved to UUIDs scoped
      to ``org_id`` at query time. Both lists are OR-combined.
    - Search filter: case-insensitive contains-match on slug and description.
      User-supplied wildcards are escaped for safety.
    - ORDER BY id (PK index ensures efficient range scan, no offset)
    - Fetches limit+1 to peek for next_cursor (standard technique)
    - Maps ORM to PositionListItem via Pydantic (from_attributes=True)

    Enforces tenancy patterns; positions are always scoped to an org.
    """
    # Base query: scoped to org, sorted by ID for cursor stability
    query = (
        select(Position)
        .where(Position.org_id == org_id)
        .order_by(Position.id)
    )

    # ── Status filter ──────────────────────────────────────────────────────
    if filters and filters.status:
        # Explicit status filter overrides the default PUBLISHED-only behaviour.
        query = query.where(Position.status.in_(filters.status))
    elif include_deleted:
        # System admin with no explicit filter: show PUBLISHED + DELETED so
        # admins can audit soft-deleted positions without forcing a ?status= param.
        from db.models.position import PositionStatus  # local import avoids top-level circular
        query = query.where(
            Position.status.in_([PositionStatus.PUBLISHED, PositionStatus.DELETED])
        )
    else:
        # Default (non-admin, no explicit filter): only PUBLISHED (open) positions.
        # This is the "open positions" view for external callers.
        from db.models.position import PositionStatus  # local import avoids top-level circular
        query = query.where(Position.status == PositionStatus.PUBLISHED)

    # ── Department filter ──────────────────────────────────────────────────
    # Resolve the mixed depts list (UUIDs or slugs) to UUIDs scoped to org.
    resolved_dept_ids: list[uuid.UUID] = []

    if filters and filters.depts:
        dept_slugs: list[str] = []
        for entry in filters.depts:
            try:
                resolved_dept_ids.append(uuid.UUID(entry))
            except (ValueError, TypeError):
                if entry:
                    dept_slugs.append(entry)

        if dept_slugs:
            slug_rows = db.execute(
                select(Department.id).where(
                    Department.org_id == org_id,
                    Department.slug.in_(dept_slugs),
                )
            ).scalars().all()
            resolved_dept_ids.extend(slug_rows)

    if resolved_dept_ids:
        # Deduplicate to avoid redundant IN values
        unique_dept_ids = list(dict.fromkeys(resolved_dept_ids))
        query = query.where(Position.dept_id.in_(unique_dept_ids))

    # ── Search filter ──────────────────────────────────────────────────────
    if filters and filters.search:
        safe_term = _escape_like(filters.search)
        query = query.where(
            or_(
                Position.slug.ilike(f"%{safe_term}%"),
                Position.description.ilike(f"%{safe_term}%"),
            )
        )

    # ── Cursor filter ──────────────────────────────────────────────────────
    if cursor:
        try:
            last_id = decode_cursor(cursor)
        except ValueError as e:
            raise ValueError("Invalid pagination cursor") from e
        query = query.where(Position.id > last_id)

    # +1 peek for has_next (don't expose to client)
    results = db.scalars(query.limit(limit + 1)).all()
    has_next = len(results) > limit
    page_positions = results[:limit]

    # Compute next_cursor from last item in page (if exists next)
    next_cursor = (
        encode_cursor(page_positions[-1].id) if has_next and page_positions else None
    )

    # Convert ORM instances to Pydantic schema items (v2, from_attributes)
    items = [
        PositionListItem.model_validate(pos, from_attributes=True)
        for pos in page_positions
    ]

    return PositionsCursorPage(items=items, next_cursor=next_cursor, limit=limit)


def paginate_roles(
    db: Session,
    cursor: str | None = None,
    limit: int = 20,
    *,
    filters: RoleFilterParams | None = None,
    include_deleted: bool = False,
) -> RolesCursorPage:
    """Fetch paginated global roles using cursor.

    Only returns roles where ``is_global=True`` AND ``org_id IS NULL`` — these
    are the platform-wide defaults usable by every organization. Org-specific
    roles (``org_id`` set) are always excluded regardless of caller permissions.

    - Soft-delete filter: ``include_deleted=False`` (default) hides DELETED roles;
      ``True`` (system admins) includes them. Explicit status filters override
      this when DELETED is explicitly requested.
    - Status filter: narrows to specific RoleStatus values when provided.
    - Search filter: case-insensitive contains-match on slug and description.
      User-supplied wildcards are escaped for safety.
    - ORDER BY id (PK index ensures efficient range scan, no offset)
    - Fetches limit+1 to peek for next_cursor (standard technique)
    - Maps ORM to RoleListItem via Pydantic (from_attributes=True)
    """
    from db.models.role import RoleStatus  # local import avoids top-level circular

    # Base query: global roles only (no org assignment), sorted by ID
    query = (
        select(Role)
        .where(
            Role.is_global.is_(True),
            Role.org_id.is_(None),
        )
        .order_by(Role.id)
    )

    # ── Status filter ──────────────────────────────────────────────────────
    if filters and filters.status:
        # Explicit status filter — caller controls which statuses to include.
        query = query.where(Role.status.in_(filters.status))
    elif not include_deleted:
        # Default: hide DELETED roles from non-system-admin callers.
        query = query.where(Role.status != RoleStatus.DELETED)

    # ── Search filter ──────────────────────────────────────────────────────
    if filters and filters.search:
        safe_term = _escape_like(filters.search)
        query = query.where(
            or_(
                Role.slug.ilike(f"%{safe_term}%"),
                Role.description.ilike(f"%{safe_term}%"),
            )
        )

    # ── Cursor filter ──────────────────────────────────────────────────────
    if cursor:
        try:
            last_id = decode_cursor(cursor)
        except ValueError as e:
            raise ValueError("Invalid pagination cursor") from e
        query = query.where(Role.id > last_id)

    # +1 peek for has_next (don't expose to client)
    results = db.scalars(query.limit(limit + 1)).all()
    has_next = len(results) > limit
    page_roles = results[:limit]

    # Compute next_cursor from last item in page (if there is a next page)
    next_cursor = (
        encode_cursor(page_roles[-1].id) if has_next and page_roles else None
    )

    # Convert ORM instances to Pydantic schema items (v2, from_attributes)
    items = [
        RoleListItem.model_validate(role, from_attributes=True)
        for role in page_roles
    ]

    return RolesCursorPage(items=items, next_cursor=next_cursor, limit=limit)
