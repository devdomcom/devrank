"""Tests for RBAC permission enforcement.

Covers:
- require_permission() factory: superuser bypass, granted/denied permissions, multi-perm AND
- Integration tests via TestClient with dependency overrides
- YAML config consistency (slug format, role references)
"""
from __future__ import annotations

import uuid

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

from api.auth.dependencies import get_current_user, require_permission
from api.auth.schemas import AuthContext
from api.exceptions import AuthorizationError
from api.handlers import register_exception_handlers


# ── Helpers ───────────────────────────────────────────────────────────────

def _auth_context(
    roles: list[str] | None = None,
    permissions: list[str] | None = None,
) -> AuthContext:
    return AuthContext(
        user_id=uuid.uuid4(),
        email="test@example.com",
        name="Test User",
        roles=roles or [],
        permissions=permissions or [],
    )


def _test_app(auth_context: AuthContext | None) -> FastAPI:
    """Minimal app with protected endpoints for testing."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/single", dependencies=[Depends(require_permission("metrics:read"))])
    def single_perm():
        return {"ok": True}

    @app.get("/multi", dependencies=[Depends(require_permission("metrics:read", "dumps:upload"))])
    def multi_perm():
        return {"ok": True}

    @app.get("/user-info")
    def with_user(auth: AuthContext = Depends(require_permission("roles:list"))):
        return {"email": auth.email}

    if auth_context is not None:
        app.dependency_overrides[get_current_user] = lambda: auth_context

    return app


# ── Unit tests: factory logic ─────────────────────────────────────────────

class TestRequirePermissionFactory:
    def test_superuser_bypasses_any_permission(self):
        ctx = _auth_context(roles=["superuser"], permissions=[])
        dep = require_permission("metrics:read")
        assert dep(current_user=ctx) == ctx

    def test_superuser_bypasses_multi_permissions(self):
        ctx = _auth_context(roles=["superuser"], permissions=[])
        dep = require_permission("metrics:read", "dumps:upload")
        assert dep(current_user=ctx) == ctx

    def test_user_with_correct_permission_passes(self):
        ctx = _auth_context(permissions=["metrics:read"])
        dep = require_permission("metrics:read")
        assert dep(current_user=ctx) == ctx

    def test_user_with_all_required_permissions_passes(self):
        ctx = _auth_context(permissions=["metrics:read", "dumps:upload"])
        dep = require_permission("metrics:read", "dumps:upload")
        assert dep(current_user=ctx) == ctx

    def test_user_missing_permission_raises_403(self):
        ctx = _auth_context(permissions=["roles:list"])
        dep = require_permission("metrics:read")
        with pytest.raises(AuthorizationError):
            dep(current_user=ctx)

    def test_user_missing_one_of_multi_permissions_raises_403(self):
        ctx = _auth_context(permissions=["metrics:read"])
        dep = require_permission("metrics:read", "dumps:upload")
        with pytest.raises(AuthorizationError) as exc_info:
            dep(current_user=ctx)
        assert "dumps:upload" in str(exc_info.value)

    def test_user_with_no_roles_no_perms_raises_403(self):
        ctx = _auth_context()
        dep = require_permission("metrics:read")
        with pytest.raises(AuthorizationError):
            dep(current_user=ctx)


# ── Integration tests: HTTP responses ─────────────────────────────────────

class TestPermissionEnforcement:
    def test_superuser_allowed(self):
        ctx = _auth_context(roles=["superuser"])
        client = TestClient(_test_app(ctx))
        assert client.get("/single").status_code == 200

    def test_correct_permission_allowed(self):
        ctx = _auth_context(permissions=["metrics:read"])
        client = TestClient(_test_app(ctx))
        assert client.get("/single").status_code == 200

    def test_wrong_permission_returns_403(self):
        ctx = _auth_context(permissions=["dumps:upload"])
        client = TestClient(_test_app(ctx))
        resp = client.get("/single")
        assert resp.status_code == 403
        data = resp.json()
        assert data["error"] == "authorization_error"
        assert "metrics:read" in data["detail"]

    def test_multi_permission_all_granted(self):
        ctx = _auth_context(permissions=["metrics:read", "dumps:upload"])
        client = TestClient(_test_app(ctx))
        assert client.get("/multi").status_code == 200

    def test_multi_permission_partial_returns_403(self):
        ctx = _auth_context(permissions=["metrics:read"])
        client = TestClient(_test_app(ctx))
        resp = client.get("/multi")
        assert resp.status_code == 403
        assert "dumps:upload" in resp.json()["detail"]

    def test_unauthenticated_returns_error(self):
        # No auth override → get_current_user requires Bearer token → 401/403
        client = TestClient(_test_app(None), raise_server_exceptions=False)
        resp = client.get("/single")
        assert resp.status_code in (401, 403)

    def test_factory_returns_auth_context_to_handler(self):
        ctx = _auth_context(permissions=["roles:list"])
        client = TestClient(_test_app(ctx))
        resp = client.get("/user-info")
        assert resp.status_code == 200
        assert resp.json()["email"] == "test@example.com"


# ── YAML config consistency ───────────────────────────────────────────────

class TestRbacYamlConfig:
    def test_all_slugs_follow_resource_action_format(self):
        from api.auth.rbac import get_all_permission_slugs

        for slug in get_all_permission_slugs():
            parts = slug.split(":")
            assert len(parts) == 2, f"'{slug}' must be resource:action"
            assert all(p.strip() for p in parts)

    def test_expected_permissions_present(self):
        from api.auth.rbac import get_all_permission_slugs

        # Includes organizations:list (added for platform endpoint; system-role only)
        # Validate whenever perms.yaml changes (DRY source-of-truth test)
        expected = {
            "metrics:list", "metrics:read", "metrics:compute",
            "roles:list", "roles:read",
            "dumps:upload", "system:debug",
            "organizations:list",
        }
        assert expected == get_all_permission_slugs()

    def test_role_permissions_reference_defined_slugs(self):
        from api.auth.rbac import get_all_permission_slugs, get_role_definitions

        all_slugs = get_all_permission_slugs()
        for role_slug, role_def in get_role_definitions().items():
            perms = role_def.get("permissions", [])
            if isinstance(perms, list):
                unknown = set(perms) - all_slugs
                assert not unknown, f"Role '{role_slug}' refs undefined: {unknown}"

    def test_wildcard_resolves_to_all_permissions(self):
        from api.auth.rbac import get_all_permission_slugs, get_role_definitions

        all_slugs = get_all_permission_slugs()
        superuser = get_role_definitions()["superuser"]
        assert set(superuser["permissions"]) == all_slugs

    def test_descriptions_non_empty(self):
        from api.auth.rbac import get_permission_descriptions

        for slug, desc in get_permission_descriptions().items():
            assert desc, f"Permission '{slug}' has empty description"
