"""Tests for auth endpoints: POST /auth/login and POST /auth/refresh.

Follows AGENTS.md testing guidance:
- Use dedicated test apps / dependency overrides for handler testing.
- Avoid module-level conditional registration; create minimal FastAPI apps
  in fixtures to isolate handler behaviour from routing/middleware concerns.
- Mock DB sessions rather than hitting a real Postgres instance.

Design:
  Both /login and /refresh use a sync DB session (get_db) and no auth
  dependency (they *issue* tokens, not consume them).  We override get_db
  with a MagicMock that returns controlled RefreshToken / User objects.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from api.app import app
from api.auth.dependencies import get_db


# ── Helpers ───────────────────────────────────────────────────────────────────


def _sha256(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def _make_mock_db() -> MagicMock:
    """Return a MagicMock that behaves like a SQLAlchemy sync Session."""
    db = MagicMock()
    db.add = MagicMock()
    db.flush = MagicMock()
    db.commit = MagicMock()
    return db


# ── Login tests ───────────────────────────────────────────────────────────────


class TestLoginEndpoint:
    """POST /api/v1/auth/login"""

    def test_login_returns_token_pair_on_valid_credentials(self):
        """Valid email/password → 200 with access_token + refresh_token."""
        user_id = uuid.uuid4()
        mock_user = MagicMock()
        mock_user.id = user_id
        mock_user.hashed_password = "hashed"

        mock_db = _make_mock_db()

        with patch("api.routes.auth.authenticate_user", return_value=mock_user), \
             patch("api.routes.auth.create_refresh_token", return_value="raw-refresh-token"):
            app.dependency_overrides[get_db] = lambda: mock_db
            try:
                client = TestClient(app, raise_server_exceptions=True)
                resp = client.post(
                    "/api/v1/auth/login",
                    json={"email": "user@example.com", "password": "secret"},
                )
            finally:
                app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["refresh_token"] == "raw-refresh-token"
        assert data["token_type"] == "bearer"

    def test_login_returns_jwt_access_token(self):
        """The access_token field must be a decodable JWT."""
        from jose import jwt as jose_jwt
        from config import settings

        user_id = uuid.uuid4()
        mock_user = MagicMock()
        mock_user.id = user_id
        mock_user.hashed_password = "hashed"

        mock_db = _make_mock_db()

        with patch("api.routes.auth.authenticate_user", return_value=mock_user), \
             patch("api.routes.auth.create_refresh_token", return_value="raw-token"):
            app.dependency_overrides[get_db] = lambda: mock_db
            try:
                client = TestClient(app, raise_server_exceptions=True)
                resp = client.post(
                    "/api/v1/auth/login",
                    json={"email": "user@example.com", "password": "secret"},
                )
            finally:
                app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        access_token = resp.json()["access_token"]
        payload = jose_jwt.decode(
            access_token, settings.secret_key, algorithms=[settings.jwt_algorithm]
        )
        assert payload["sub"] == str(user_id)

    def test_login_bad_credentials_returns_401(self):
        """Wrong email or password → 401 with authentication_error."""
        mock_db = _make_mock_db()

        with patch("api.routes.auth.authenticate_user", return_value=None):
            app.dependency_overrides[get_db] = lambda: mock_db
            try:
                client = TestClient(app, raise_server_exceptions=False)
                resp = client.post(
                    "/api/v1/auth/login",
                    json={"email": "bad@example.com", "password": "wrong"},
                )
            finally:
                app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 401
        assert resp.json()["error"] == "authentication_error"

    def test_login_missing_password_field_returns_422(self):
        """Missing required field → 422 Unprocessable Entity."""
        mock_db = _make_mock_db()
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post("/api/v1/auth/login", json={"email": "x@x.com"})
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 422

    def test_login_commits_transaction(self):
        """Successful login must commit the DB transaction (persists refresh token)."""
        user_id = uuid.uuid4()
        mock_user = MagicMock()
        mock_user.id = user_id
        mock_user.hashed_password = "hashed"

        mock_db = _make_mock_db()

        with patch("api.routes.auth.authenticate_user", return_value=mock_user), \
             patch("api.routes.auth.create_refresh_token", return_value="tok"):
            app.dependency_overrides[get_db] = lambda: mock_db
            try:
                client = TestClient(app, raise_server_exceptions=True)
                client.post(
                    "/api/v1/auth/login",
                    json={"email": "user@example.com", "password": "pass"},
                )
            finally:
                app.dependency_overrides.pop(get_db, None)

        mock_db.commit.assert_called_once()


# ── Refresh tests ─────────────────────────────────────────────────────────────


class TestRefreshEndpoint:
    """POST /api/v1/auth/refresh"""

    def _post_refresh(self, token: str, mock_db: MagicMock | None = None) -> MagicMock:
        db = mock_db or _make_mock_db()
        app.dependency_overrides[get_db] = lambda: db
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post(
                "/api/v1/auth/refresh",
                json={"refresh_token": token},
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
        return resp

    def test_valid_refresh_token_returns_new_token_pair(self):
        """Valid refresh token → 200 with new access_token + refresh_token."""
        user_id = uuid.uuid4()

        with patch(
            "api.routes.auth.rotate_refresh_token",
            return_value=("new-raw-refresh", user_id),
        ):
            resp = self._post_refresh("valid-token")

        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["refresh_token"] == "new-raw-refresh"
        assert data["token_type"] == "bearer"

    def test_refresh_access_token_encodes_correct_user(self):
        """The new access_token JWT must encode the same user_id returned by rotate."""
        from jose import jwt as jose_jwt
        from config import settings

        user_id = uuid.uuid4()

        with patch(
            "api.routes.auth.rotate_refresh_token",
            return_value=("new-tok", user_id),
        ):
            resp = self._post_refresh("valid-token")

        assert resp.status_code == 200
        payload = jose_jwt.decode(
            resp.json()["access_token"],
            settings.secret_key,
            algorithms=[settings.jwt_algorithm],
        )
        assert payload["sub"] == str(user_id)

    def test_invalid_refresh_token_returns_401(self):
        """Unknown / invalid token → 401 authentication_error."""
        from api.exceptions import AuthenticationError

        with patch(
            "api.routes.auth.rotate_refresh_token",
            side_effect=AuthenticationError("Invalid refresh token"),
        ):
            resp = self._post_refresh("garbage-token")

        assert resp.status_code == 401
        assert resp.json()["error"] == "authentication_error"

    def test_expired_refresh_token_returns_401(self):
        """Expired token → 401 authentication_error."""
        from api.exceptions import AuthenticationError

        with patch(
            "api.routes.auth.rotate_refresh_token",
            side_effect=AuthenticationError("Refresh token has expired"),
        ):
            resp = self._post_refresh("expired-token")

        assert resp.status_code == 401
        assert "expired" in resp.json()["detail"].lower()

    def test_replayed_refresh_token_returns_401(self):
        """Replayed (already-revoked) token → 401; detail mentions sessions invalidated."""
        from api.exceptions import AuthenticationError

        with patch(
            "api.routes.auth.rotate_refresh_token",
            side_effect=AuthenticationError(
                "Refresh token has already been used; all sessions invalidated"
            ),
        ):
            resp = self._post_refresh("replayed-token")

        assert resp.status_code == 401
        assert "sessions invalidated" in resp.json()["detail"]

    def test_refresh_commits_transaction(self):
        """Successful refresh must commit the DB transaction."""
        user_id = uuid.uuid4()
        mock_db = _make_mock_db()

        with patch(
            "api.routes.auth.rotate_refresh_token",
            return_value=("new-tok", user_id),
        ):
            self._post_refresh("valid-token", mock_db)

        mock_db.commit.assert_called_once()

    def test_refresh_missing_body_returns_422(self):
        """Missing refresh_token field → 422."""
        mock_db = _make_mock_db()
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post("/api/v1/auth/refresh", json={})
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 422


# ── Security util unit tests ───────────────────────────────────────────────────


class TestRefreshTokenUtils:
    """Unit tests for api/auth/security.py refresh token helpers."""

    def test_hash_token_is_sha256_hex(self):
        from api.auth.security import _hash_token

        raw = "hello-world"
        result = _hash_token(raw)
        assert result == hashlib.sha256(raw.encode()).hexdigest()
        assert len(result) == 64

    def test_create_refresh_token_persists_hash_not_raw(self):
        """create_refresh_token must store the hash, never the raw value."""
        from api.auth.security import create_refresh_token

        user_id = uuid.uuid4()
        db = MagicMock()
        db.add = MagicMock()
        db.flush = MagicMock()

        raw = create_refresh_token(user_id, db)

        # db.add must have been called with a RefreshToken
        assert db.add.called
        stored_record = db.add.call_args[0][0]

        # The stored hash must NOT equal the raw token
        assert stored_record.token_hash != raw
        # But it must equal SHA-256 of the raw token
        assert stored_record.token_hash == _sha256(raw)
        # The user_id must match
        assert stored_record.user_id == user_id

    def test_create_refresh_token_returns_url_safe_string(self):
        """Raw token must be a non-empty URL-safe string."""
        import re
        from api.auth.security import create_refresh_token

        db = MagicMock()
        db.add = MagicMock()
        db.flush = MagicMock()

        raw = create_refresh_token(uuid.uuid4(), db)
        # URL-safe base64: alphanumeric + - and _
        assert re.match(r"^[A-Za-z0-9_\-]+$", raw)
        assert len(raw) >= 32

    def test_rotate_refresh_token_raises_on_unknown_token(self):
        """Token not in DB → AuthenticationError."""
        from sqlalchemy import select
        from api.auth.security import rotate_refresh_token
        from api.exceptions import AuthenticationError

        db = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        db.execute.return_value = mock_result

        with pytest.raises(AuthenticationError, match="Invalid refresh token"):
            rotate_refresh_token("unknown-token", db)

    def test_rotate_refresh_token_raises_on_expired_token(self):
        """Expired token → AuthenticationError."""
        from api.auth.security import rotate_refresh_token, _hash_token
        from api.exceptions import AuthenticationError
        from db.models.refresh_token import RefreshToken

        raw = "my-token"
        record = MagicMock(spec=RefreshToken)
        record.token_hash = _hash_token(raw)
        record.expires_at = datetime(2000, 1, 1, tzinfo=timezone.utc)  # past
        record.revoked_at = None

        db = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = record
        db.execute.return_value = mock_result

        with pytest.raises(AuthenticationError, match="expired"):
            rotate_refresh_token(raw, db)

    def test_rotate_refresh_token_raises_on_revoked_token(self):
        """Revoked token → AuthenticationError; all sessions invalidated."""
        from api.auth.security import rotate_refresh_token, _hash_token
        from api.exceptions import AuthenticationError
        from db.models.refresh_token import RefreshToken

        raw = "my-token"
        record = MagicMock(spec=RefreshToken)
        record.token_hash = _hash_token(raw)
        record.expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        record.revoked_at = datetime.now(timezone.utc)  # already revoked
        record.user_id = uuid.uuid4()

        db = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = record
        db.execute.return_value = mock_result

        with pytest.raises(AuthenticationError, match="sessions invalidated"):
            rotate_refresh_token(raw, db)

    def test_rotate_refresh_token_happy_path(self):
        """Valid token → old row revoked, new raw token returned with correct user_id."""
        from api.auth.security import rotate_refresh_token, _hash_token
        from db.models.refresh_token import RefreshToken

        raw = "valid-raw-token"
        user_id = uuid.uuid4()

        record = MagicMock(spec=RefreshToken)
        record.token_hash = _hash_token(raw)
        record.expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        record.revoked_at = None
        record.user_id = user_id

        db = MagicMock()
        # First execute: lookup by hash
        # Second execute (inside create_refresh_token): nothing to return
        first_result = MagicMock()
        first_result.scalar_one_or_none.return_value = record
        db.execute.return_value = first_result
        db.add = MagicMock()
        db.flush = MagicMock()

        with patch("api.auth.security.create_refresh_token", return_value="new-raw") as mock_create:
            new_raw, returned_user_id = rotate_refresh_token(raw, db)

        # Old record must be revoked
        assert record.revoked_at is not None
        db.flush.assert_called()
        # New token issued for same user
        mock_create.assert_called_once_with(user_id, db)
        assert new_raw == "new-raw"
        assert returned_user_id == user_id
