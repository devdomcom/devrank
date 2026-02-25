"""Users routes (platform-wide user management).

Endpoints:
- GET    /users/          — cursor-paginated list (system admins only)
- POST   /users/          — create new user (system admins only)
- GET    /users/{user_id} — user detail (own profile or system admin)

User listing and creation are restricted to system administrators (superusers).
The detail endpoint allows users to view their own profile, and system admins
to view any user's profile with additional org/department membership info.
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth.dependencies import get_current_user, get_db, require_permission
from api.auth.schemas import AuthContext
from api.pagination import get_cursor_params, paginate_users
from api.schemas import (
    CreateUserRequest,
    UpdateUserRequest,
    UserFilterParams,
    UserOrgMembership,
    UserResponse,
    UsersCursorPage,
    get_user_filters,
)
from db.models import (
    RoleType,
    SystemRole,
    User,
    UserOrgDepartment,
    UserRoleAssignment,
)
from db.models.user import Gender, UserStatus

router = APIRouter(prefix="/users", tags=["users"])

# DRY: shared bcrypt context (same config as api/routes/auth.py)
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _build_user_response(
    user: User,
    *,
    include_memberships: bool = False,
    db: Session | None = None,
) -> UserResponse:
    """Build a UserResponse from an ORM User instance.

    DRY helper shared by create_user and get_user to avoid duplicating
    the ORM-to-schema mapping in every endpoint.

    When ``include_memberships=True`` (admin view), fetches org/dept
    memberships from the DB and populates the ``memberships`` field.
    """
    memberships = None
    if include_memberships and db is not None:
        rows = db.scalars(
            select(UserOrgDepartment).where(
                UserOrgDepartment.user_id == user.id
            )
        ).all()
        memberships = [
            UserOrgMembership(
                org_id=m.org_id,
                org_name=m.organization.name,
                org_slug=m.organization.slug,
                dept_id=m.dept_id,
                dept_name=m.department.name if m.department else None,
                dept_slug=m.department.slug if m.department else None,
                status=m.status.value,
            )
            for m in rows
            if m.status.value != "DELETED"
        ]

    return UserResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        surname=user.surname,
        nickname=user.nickname,
        avatar=user.avatar,
        gender=user.gender.value if user.gender else None,
        country=user.country,
        timezone=user.timezone,
        locale=user.locale,
        status=user.status,
        is_verified=user.is_verified,
        is_kyc_verified=user.is_kyc_verified,
        created_at=user.created_at,
        updated_at=user.updated_at,
        last_login_at=user.last_login_at,
        dob=user.dob,
        address=user.address,
        zip=user.zip,
        phone=user.phone,
        memberships=memberships,
    )


# ── GET /users/ ───────────────────────────────────────────────────────────


@router.get(
    "/",
    summary="List all users (cursor paginated, filterable; system admins only)",
    response_model=UsersCursorPage,
)
def list_users(
    params: Annotated[tuple[str | None, int], Depends(get_cursor_params)],
    filters: Annotated[UserFilterParams, Depends(get_user_filters)],
    auth: AuthContext = Depends(require_permission("users:list")),
    db: Session = Depends(get_db),
) -> UsersCursorPage:
    """List all users in the platform with optional status/search filters.

    This endpoint is restricted to system administrators (superusers) only.
    It provides platform-wide visibility into all user accounts.

    Supports filtering by:
    - status: One or more UserStatus values (repeatable query param)
    - search: Case-insensitive search on name, surname, email, and nickname

    Uses cursor-based pagination for efficient traversal of large user lists.
    """
    cursor, limit = params
    return paginate_users(
        db,
        cursor,
        limit,
        filters=filters,
    )


# ── POST /users/ ──────────────────────────────────────────────────────────


@router.post(
    "/",
    summary="Create a new user (system admins only)",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_user(
    body: CreateUserRequest,
    auth: AuthContext = Depends(require_permission("users:create")),
    db: Session = Depends(get_db),
) -> UserResponse:
    """Create a new user account.

    **System admin only.** Requires ``users:create`` permission (granted to
    superuser role by default).

    Steps (single transaction):
    1. Validate email uniqueness
    2. Hash the password (bcrypt via passlib)
    3. Create User record with ACTIVE status
    4. Assign the default ``user`` system role so the new account has
       baseline permissions (e.g. ``organizations:create``, ``users:read``)

    **Default values applied server-side:**
    - ``status``: ACTIVE
    - ``role`` (legacy column): USER
    - ``gender``: prefer_not_to_say (if not provided)
    - ``timezone``: UTC (if not provided)
    - ``locale``: en (if not provided)
    """
    # 1. Email uniqueness
    existing = db.execute(
        select(User).where(User.email == body.email)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"User with email '{body.email}' already exists",
        )

    # 2. Parse + validate gender enum (if provided)
    gender = Gender.PREFER_NOT_TO_SAY
    if body.gender:
        try:
            gender = Gender(body.gender)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid gender value: '{body.gender}'. "
                f"Allowed: {', '.join(g.value for g in Gender)}",
            )

    # 3. Create user
    user = User(
        email=body.email,
        name=body.name,
        surname=body.surname,
        nickname=body.nickname,
        hashed_password=_pwd_context.hash(body.password),
        gender=gender,
        country=body.country,
        timezone=body.timezone,
        locale=body.locale,
        status=UserStatus.ACTIVE,
        is_verified=body.is_verified,
        is_kyc_verified=body.is_kyc_verified,
    )
    db.add(user)
    db.flush()  # Get user.id for FK refs

    # 4. Assign default 'user' system role (baseline RBAC permissions)
    user_role = db.execute(
        select(SystemRole).where(SystemRole.slug == "user")
    ).scalar_one_or_none()
    if user_role:
        db.add(
            UserRoleAssignment(
                user_id=user.id,
                role_type=RoleType.SYSTEM,
                role_id=user_role.id,
            )
        )

    db.commit()
    db.refresh(user)

    return _build_user_response(user)


# ── GET /users/{user_id} ──────────────────────────────────────────────────


@router.get(
    "/{user_id}",
    summary="Get user by ID",
    response_model=UserResponse,
)
def get_user(
    user_id: uuid.UUID = Path(..., description="User UUID"),
    auth: AuthContext = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserResponse:
    """Get a specific user's details.

    **Access control:**
    - Users can view their own profile (``users:read`` permission, granted
      to the default ``user`` system role)
    - System admins can view any user's profile
    - System admins see additional org/department membership information

    **Standard view** (own profile):
    - Basic user information (name, email, status, etc.)
    - ``memberships`` field is null

    **Admin view** (system admin viewing any user):
    - All basic user information
    - ``memberships`` field contains list of org/department memberships
    """
    is_own_profile = auth.user_id == user_id
    is_admin = auth.is_system_admin

    if not (is_own_profile or is_admin):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this user's profile",
        )

    user = db.get(User, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{user_id}' not found",
        )

    return _build_user_response(
        user,
        include_memberships=is_admin,
        db=db,
    )

# ── PATCH /users/{user_id} ────────────────────────────────────────────────


@router.patch(
    "/{user_id}",
    summary="Update user details (system admins only)",
    response_model=UserResponse,
)
def update_user(
    body: UpdateUserRequest,
    user_id: uuid.UUID = Path(..., description="User UUID"),
    auth: AuthContext = Depends(require_permission("users:update")),
    db: Session = Depends(get_db),
) -> UserResponse:
    """Partially update an existing user account.

    Only fields present in the request body are applied (true PATCH semantics).
    Uses Pydantic v2 ``model_fields_set`` to distinguish "not sent" from
    "explicitly sent as null" — e.g., sending ``{"nickname": null}`` clears
    the nickname, while omitting it leaves it unchanged.

    **System admin only.** Requires ``users:update`` permission.

    Allows updating all fields except:
    - ID (immutable)
    - Memberships (managed via separate endpoints)
    - Auditing dates (created_at, updated_at, last_login_at)
    - Status (managed via separate endpoint)

    **Validation:**
    - Rejects empty body (no fields to update)
    - Email and password cannot be set to null
    - Email must be unique if changed
    - Gender must be a valid enum value
    - Password is hashed if changed
    """
    sent_fields = body.model_fields_set
    if not sent_fields:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    user = db.get(User, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{user_id}' not found",
        )

    # 1. Email uniqueness check (if changing)
    if "email" in sent_fields:
        if body.email is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="email cannot be null",
            )
        if body.email != user.email:
            existing = db.execute(
                select(User).where(User.email == body.email)
            ).scalar_one_or_none()
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"User with email '{body.email}' already exists",
                )
            user.email = body.email

    # 2. Update simple fields (true PATCH: only sent fields are applied)
    if "name" in sent_fields:
        user.name = body.name
    if "surname" in sent_fields:
        user.surname = body.surname
    if "nickname" in sent_fields:
        user.nickname = body.nickname
    if "country" in sent_fields:
        user.country = body.country
    if "timezone" in sent_fields:
        user.timezone = body.timezone
    if "locale" in sent_fields:
        user.locale = body.locale
    if "avatar" in sent_fields:
        user.avatar = body.avatar
    if "dob" in sent_fields:
        user.dob = body.dob
    if "address" in sent_fields:
        user.address = body.address
    if "zip" in sent_fields:
        user.zip = body.zip
    if "phone" in sent_fields:
        user.phone = body.phone
    if "is_verified" in sent_fields:
        user.is_verified = body.is_verified
    if "is_kyc_verified" in sent_fields:
        user.is_kyc_verified = body.is_kyc_verified

    # 3. Handle password update
    if "password" in sent_fields:
        if body.password is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="password cannot be null",
            )
        user.hashed_password = _pwd_context.hash(body.password)

    # 4. Handle gender update
    if "gender" in sent_fields and body.gender is not None:
        try:
            user.gender = Gender(body.gender)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid gender value: '{body.gender}'. "
                f"Allowed: {', '.join(g.value for g in Gender)}",
            )

    db.commit()
    db.refresh(user)

    return _build_user_response(
        user,
        include_memberships=True,
        db=db,
    )
