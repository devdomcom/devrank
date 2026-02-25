"""Auth schemas for JWT/RBAC (app-level)."""
from __future__ import annotations

import uuid

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """Email/password login request."""

    email: str = Field(..., examples=["admin@devrank.local"])
    password: str = Field(..., min_length=1, examples=["admin"])


class Token(BaseModel):
    """JWT access token response (legacy; login now returns TokenPair)."""

    access_token: str
    token_type: str = "bearer"


class TokenPair(BaseModel):
    """Full token pair returned on login and refresh.

    ``access_token`` — short-lived JWT (default 30 min); send as
    ``Authorization: Bearer <access_token>`` on every API call.

    ``refresh_token`` — long-lived opaque secret (default 30 days);
    send to ``POST /api/v1/auth/refresh`` to obtain a new pair.
    Stored server-side as a SHA-256 hash; rotated on every use.
    """

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    """Request body for POST /auth/refresh."""

    refresh_token: str = Field(
        ...,
        min_length=1,
        description="Opaque refresh token obtained from /auth/login or a previous /auth/refresh call.",
    )


class AuthContext(BaseModel):
    """Current authenticated user context (JWT + DB lookup).

    Returned by /me and injected via Depends for protected routes.
    """

    user_id: uuid.UUID
    email: str
    name: str
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)

    @property
    def is_system_admin(self) -> bool:
        """True if user has a system-level admin role (superuser).

        Used for visibility decisions (e.g., showing soft-deleted orgs).
        """
        return "superuser" in self.roles
