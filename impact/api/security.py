"""JWT security utils for RBAC auth.

Handles access token creation/decoding (python-jose; HS256 per config).

DRY: extracted to utils (separate from deps per FastAPI best practices).
Claims embed user_id/roles/perms (stateless; validate vs DB in deps for security).
Used by auth deps/services; errors sanitized by handlers.

2026 FastAPI: no plain secrets in logs, short exp, alg from settings.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import status  # For 401 on auth fails
from jose import JWTError, jwt
from pydantic import BaseModel

from config import settings
from impact.exceptions import ImpactError  # Sanitized for handlers; supports status_code attr


class TokenData(BaseModel):
    """Claims payload for JWT (minimal for security; extend for roles/perms)."""

    user_id: str | None = None
    # roles/perms embedded for fast check (synced with DB in deps)


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    """Create JWT access token.

    DRY with standard FastAPI JWT; uses settings.secret_key/algorithm/expire.
    Embed claims for AuthContext.
    """
    # Use timezone-aware UTC (deprecated utcnow() removed; prevents subtle TZ bugs
    # per Python 3.12+ best practices/DRY with datetime standards)
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=settings.access_token_expire_minutes
        )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, settings.secret_key, algorithm=settings.jwt_algorithm
    )
    return encoded_jwt


def decode_access_token(token: str) -> TokenData:
    """Decode/validate JWT; raise sanitized ImpactError with 401 status (no leak).

    Auth failures (bad/expired token) emit 401 (not 500); leverages exc.status_code
    in base_impact_error_handler (DRY; consistent with HTTPException wrapping).
    Used in get_current_user dep; full exc logged by handler.
    """
    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.jwt_algorithm]
        )
        user_id: str | None = payload.get("sub")
        if user_id is None:
            # 401 for auth fail (per OAuth2/JWT std; overrides default 500)
            raise ImpactError(
                "Invalid token payload", status_code=status.HTTP_401_UNAUTHORIZED
            )
        return TokenData(user_id=user_id)
    except JWTError as e:
        # Sanitized; full logged in error handler; 401 not 500
        raise ImpactError(
            "Could not validate credentials", status_code=status.HTTP_401_UNAUTHORIZED
        ) from e
