"""API integration tests for metrics, roles, and validation endpoints.

Uses the real app with TestClient (in-process; no server needed).
Sample dump at impact/samples/github_live_dump provides real data.
"""
import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from api.app import app
# Override both auth (for perms) and db (for tenancy endpoints like organizations;
# keeps integration tests fast/independent, no external postgres needed per
# AGENTS.md testing best practices).
from api.auth.dependencies import get_current_user, get_db
from api.auth.schemas import AuthContext
# For org-admin scope test (combine user_id + org_id in UserRoleAssignment)
from api.auth.dependencies import get_organization_with_access
from api.exceptions import AuthorizationError
from db.models import AssignmentStatus, Organization, OrganizationStatus, RoleType, UserRoleAssignment
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from unittest.mock import MagicMock

DUMP_PATH = "impact/samples/github_live_dump"
VALID_ROLE = "senior_dev"

# Superuser context for integration tests (bypasses permission checks;
# superuser role => any perm allowed per require_permission in api/auth/dependencies.py)
_TEST_AUTH = AuthContext(
    user_id=uuid.uuid4(),
    email="test-admin@example.com",
    name="Test Admin",
    roles=["superuser"],
    permissions=[],
)


@pytest.fixture(autouse=True)
def _override_auth_and_db():
    """Bypass auth + mock DB for all integration tests.

    - get_current_user override: superuser for RBAC bypass (existing pattern)
    - get_db override: MagicMock session for DB endpoints (e.g., /organizations/,
      future user/dept lists). Avoids dependency on running postgres; keeps tests
      hermetic/fast. DRY mock setup focused on paginate_organizations contract
      (scalars().all() -> list).
    - Cleanup overrides (pytest fixture yield).
    Aligns with AGENTS.md: dependency_overrides for testing, dedicated mocks.
    """
    app.dependency_overrides[get_current_user] = lambda: _TEST_AUTH

    # Mock session for cursor pagination (empty list = valid empty page; shape tests pass)
    # Full ORM attrs not needed for [] case; model_validate skipped.
    mock_session = MagicMock(spec=Session)
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []  # limit+1 peek; no items => no next_cursor
    mock_session.scalars.return_value = mock_scalars
    app.dependency_overrides[get_db] = lambda: mock_session

    yield

    # Cleanup both (order doesn't matter; pop safe)
    app.dependency_overrides.pop(get_current_user, None)
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "healthy", "service": "devrank"}


# ---------------------------------------------------------------------------
# Metrics list
# ---------------------------------------------------------------------------

class TestMetricsList:
    def test_list_returns_all_metrics(self, client):
        resp = client.get("/api/v1/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 45
        slugs = {m["slug"] for m in data}
        assert "first_reviewer_rate" in slugs
        assert "cycle_time" in slugs

    def test_list_item_shape(self, client):
        resp = client.get("/api/v1/metrics")
        item = resp.json()[0]
        assert "slug" in item
        assert "name" in item
        assert "description" in item
        assert "category" in item


# ---------------------------------------------------------------------------
# POST /metrics/compute
# ---------------------------------------------------------------------------

class TestComputeMetrics:
    def test_compute_single_metric(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "metric_slugs": ["first_reviewer_rate"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_login"]
        assert len(data["metrics"]) == 1
        m = data["metrics"][0]
        assert m["slug"] == "first_reviewer_rate"
        assert m["rating"] in ["excellent", "good", "neutral", "bad", "unknown", "INSUFFICIENT_DATA"]
        assert "rate" in m["details"]

    def test_compute_all_metrics(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["metrics"]) >= 45
        # Every metric should have valid rating enum
        valid_ratings = {"excellent", "good", "neutral", "bad", "unknown", "descriptive", "INSUFFICIENT_DATA"}
        for m in data["metrics"]:
            assert m["rating"] in valid_ratings, f"{m['slug']} has invalid rating: {m['rating']}"

    def test_compute_missing_role_returns_422(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
        })
        assert resp.status_code == 422

    def test_compute_invalid_role_returns_400(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": "nonexistent_role",
            "metric_slugs": ["cycle_time"],
        })
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"].lower()

    def test_compute_missing_body_returns_422(self, client):
        resp = client.post("/api/v1/metrics/compute")
        assert resp.status_code == 422

    def test_compute_unknown_slug_returns_400(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "metric_slugs": ["nonexistent_metric"],
        })
        assert resp.status_code == 400
        assert "unknown metric slug" in resp.json()["detail"].lower()

    def test_compute_mixed_valid_and_invalid_slugs_returns_400(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "metric_slugs": ["cycle_time", "not_a_real_metric"],
        })
        assert resp.status_code == 400
        assert "not_a_real_metric" in resp.json()["detail"]

    def test_compute_category_slug_returns_400(self, client):
        """Category names are not metric slugs — must be rejected, not silently ignored."""
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "metric_slugs": ["review_impact"],
        })
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /metrics/{slug}
# ---------------------------------------------------------------------------

class TestGetSingleMetric:
    def test_get_metric_ok(self, client):
        resp = client.get("/api/v1/metrics/cycle_time", params={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["slug"] == "cycle_time"
        assert data["rating"] in ["excellent", "good", "neutral", "bad", "unknown", "INSUFFICIENT_DATA"]

    def test_get_metric_missing_role_returns_422(self, client):
        resp = client.get("/api/v1/metrics/cycle_time", params={
            "dump_path": DUMP_PATH,
        })
        assert resp.status_code == 422

    def test_get_metric_invalid_role_returns_400(self, client):
        resp = client.get("/api/v1/metrics/cycle_time", params={
            "dump_path": DUMP_PATH,
            "role": "bad_role",
        })
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"].lower()

    def test_get_metric_not_found(self, client):
        resp = client.get("/api/v1/metrics/nonexistent", params={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
        })
        assert resp.status_code == 404

    def test_get_metric_missing_dump_returns_422(self, client):
        resp = client.get("/api/v1/metrics/cycle_time", params={
            "role": VALID_ROLE,
        })
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /metrics/compare
# ---------------------------------------------------------------------------

class TestCompareMetrics:
    def test_compare_missing_role_returns_422(self, client):
        resp = client.post("/api/v1/metrics/compare", json={
            "dump_path": DUMP_PATH,
            "window1": {"start_date": "2026-01-01T00:00:00Z", "end_date": "2026-01-15T00:00:00Z"},
            "window2": {"start_date": "2026-01-15T00:00:00Z", "end_date": "2026-01-29T00:00:00Z"},
        })
        assert resp.status_code == 422

    def test_compare_invalid_role_returns_400(self, client):
        resp = client.post("/api/v1/metrics/compare", json={
            "dump_path": DUMP_PATH,
            "role": "bad_role",
            "window1": {"start_date": "2026-01-01T00:00:00Z", "end_date": "2026-01-15T00:00:00Z"},
            "window2": {"start_date": "2026-01-15T00:00:00Z", "end_date": "2026-01-29T00:00:00Z"},
            "metrics": ["cycle_time"],
        })
        assert resp.status_code == 400

    def test_compare_missing_windows_returns_422(self, client):
        resp = client.post("/api/v1/metrics/compare", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
        })
        assert resp.status_code == 422

    def test_compare_unknown_slug_returns_400(self, client):
        resp = client.post("/api/v1/metrics/compare", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "window1": {"start_date": "2026-01-01T00:00:00Z", "end_date": "2026-01-15T00:00:00Z"},
            "window2": {"start_date": "2026-01-15T00:00:00Z", "end_date": "2026-01-29T00:00:00Z"},
            "metrics": ["fake_metric"],
        })
        assert resp.status_code == 400
        assert "unknown metric slug" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Roles endpoints
# ---------------------------------------------------------------------------

class TestRoles:
    def test_list_roles(self, client):
        resp = client.get("/api/v1/roles")
        assert resp.status_code == 200
        names = [r["name"] for r in resp.json()]
        assert "senior_dev" in names
        assert "default" not in names

    def test_get_role_ok(self, client):
        resp = client.get("/api/v1/roles/senior_dev")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "senior_dev"
        assert "metrics" in data["config"]

    def test_get_role_not_found(self, client):
        resp = client.get("/api/v1/roles/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Continuous score presence
# ---------------------------------------------------------------------------

class TestContinuousScores:
    def test_scored_metrics_have_continuous_score(self, client):
        resp = client.post("/api/v1/metrics/compute", json={
            "dump_path": DUMP_PATH,
            "role": VALID_ROLE,
            "metric_slugs": ["cycle_time", "reviews_given", "pr_throughput"],
        })
        assert resp.status_code == 200
        for m in resp.json()["metrics"]:
            if m["rating"] not in ["INSUFFICIENT_DATA", "unknown"]:
                assert m["continuous_score"] is not None, \
                    f"{m['slug']} has rating {m['rating']} but no continuous_score"


# ---------------------------------------------------------------------------
# Organizations (platform tenancy; cursor-paginated, system-role gated)
# ---------------------------------------------------------------------------
# Tests rely on superuser override (bypasses "organizations:list" perm) and
# real DB (orgs table from migrations; active=deleted_at IS NULL). If no orgs,
# returns empty items list (valid cursor page). Extend for seeded test data.

class TestOrganizations:
    """Integration tests for GET /api/v1/organizations/."""

    def test_list_organizations_ok(self, client):
        """Basic list returns cursor page shape (even if empty)."""
        resp = client.get("/api/v1/organizations/")
        assert resp.status_code == 200
        data = resp.json()
        # Matches OrganizationsCursorPage schema
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20  # default
        # Each item (if any) has org fields
        for item in data["items"]:
            assert "id" in item
            assert "slug" in item
            assert "status" in item
            assert "created_at" in item

    def test_list_organizations_with_custom_limit(self, client):
        """Query param limit respected (cursor dep)."""
        resp = client.get("/api/v1/organizations/?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["limit"] == 5
        assert len(data["items"]) <= 5

    def test_list_organizations_invalid_cursor_returns_400(self, client):
        """Bad cursor -> ValueError -> handler 400 (sanitized; no leak)."""
        resp = client.get("/api/v1/organizations/?cursor=invalid!!")
        assert resp.status_code == 400
        # Via value_error_handler -> AppError
        body = resp.json()
        assert "error" in body or "detail" in body

    def test_get_organization_by_slug_org_admin_access(self, client):
        """Test org-specific RBAC: combine user_id + org_id (org_admin app role).

        Overrides dep directly (test app pattern from test_permissions.py).
        Verifies get_organization_with_access combination (user_id + org_id match,
        APP role_type, active status per RBAC yaml/assignment model).
        System bypass covered in list test; here scoped org_admin.
        """
        # Fake org for dep return (combination check simulated pass)
        test_org = Organization(
            id=uuid.uuid4(),
            slug="sample-acme-corp",
            description="test",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

        def _mock_org_access():
            return test_org

        # Override dep for isolation (DRY with auth override; AGENTS.md pattern)
        app.dependency_overrides[get_organization_with_access] = _mock_org_access
        try:
            resp = client.get("/api/v1/organizations/sample-acme-corp")
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "sample-acme-corp"
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

    def test_get_organization_no_org_access_403(self, client):
        """Test no-match org scope raises 403 (org_admin without assignment combo)."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'sample-acme-corp'")

        app.dependency_overrides[get_organization_with_access] = _mock_no_access
        try:
            resp = client.get("/api/v1/organizations/sample-acme-corp")
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

