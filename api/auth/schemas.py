"""Auth schemas for JWT/RBAC (app-level)."""
from __future__ import annotations

import uuid

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """Email/password login request."""

    email: str = Field(..., examples=["admin@example.com"])
    password: str = Field(..., min_length=1)


class Token(BaseModel):
    """JWT token response."""

    access_token: str
    token_type: str = "bearer"


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
