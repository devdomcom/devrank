"""JWT security utils for RBAC auth (app-level).

Handles access token creation/decoding (python-jose; HS256 per config).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from pydantic import BaseModel

from api.exceptions import AuthenticationError
from config import settings


class TokenData(BaseModel):
    """Claims payload for JWT."""

    user_id: str | None = None


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=settings.access_token_expire_minutes
        )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> TokenData:
    """Decode/validate JWT; raise AuthenticationError (401) on failure."""
    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.jwt_algorithm]
        )
        user_id: str | None = payload.get("sub")
        if user_id is None:
            raise AuthenticationError("Invalid token payload")
        return TokenData(user_id=user_id)
    except JWTError as e:
        raise AuthenticationError("Could not validate credentials") from e
