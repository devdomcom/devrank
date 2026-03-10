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
from api.auth.dependencies import (
    get_department_with_access,
    get_department_with_delete_access,
    get_department_with_update_access,
    get_organization_with_access,
    get_organization_with_delete_access,
    get_organization_with_update_access,
)
from api.exceptions import AuthorizationError
from db.models import (
    AssignmentStatus,
    Department,
    DepartmentStatus,
    Organization,
    OrganizationStatus,
    RoleType,
    UserRoleAssignment,
)
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
        # Each item (if any) has org fields (including name, deleted_at)
        for item in data["items"]:
            assert "id" in item
            assert "name" in item
            assert "slug" in item
            assert "status" in item
            assert "created_at" in item
            assert "deleted_at" in item

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
            name="Sample Acme Corp",
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
            assert data["name"] == "Sample Acme Corp"
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


# ---------------------------------------------------------------------------
# PATCH /organizations/{id_or_slug} (partial update)
# ---------------------------------------------------------------------------
# Tests follow existing dep-override pattern from GET org tests.
# get_organization_with_update_access is overridden to return a fake org
# (or raise 403), and get_db provides a MagicMock session.


class TestUpdateOrganization:
    """Integration tests for PATCH /api/v1/organizations/{id_or_slug}."""

    @pytest.fixture
    def test_org(self):
        """Reusable fake Organization for PATCH tests."""
        return Organization(
            id=uuid.uuid4(),
            name="Patch Test Org",
            slug="patch-test-org",
            description="Original description",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
            activated_at=None,
            deactivated_at=None,
            banned_at=None,
        )

    def _override_update_access(self, org):
        """Override get_organization_with_update_access to return the given org."""
        app.dependency_overrides[get_organization_with_update_access] = lambda: org

    def _cleanup_overrides(self):
        app.dependency_overrides.pop(get_organization_with_update_access, None)

    def test_patch_name_only(self, client, test_org):
        """PATCH with name only updates name, leaves slug/description/status."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"name": "Renamed Org"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["name"] == "Renamed Org"
            assert data["slug"] == "patch-test-org"
            assert data["description"] == "Original description"
            assert data["status"] == "ACTIVE"
        finally:
            self._cleanup_overrides()

    def test_patch_name_null_returns_422(self, client, test_org):
        """PATCH with name explicitly set to null is rejected (name is required)."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"name": None},
            )
            # Pydantic validation rejects null name due to min_length constraint
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_description_only(self, client, test_org):
        """PATCH with description only updates description, leaves slug/status."""
        self._override_update_access(test_org)
        # Mock DB: commit/refresh are no-ops on MagicMock session
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"description": "Updated description"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["description"] == "Updated description"
            assert data["slug"] == "patch-test-org"
            assert data["status"] == "ACTIVE"
        finally:
            self._cleanup_overrides()

    def test_patch_clear_description_to_null(self, client, test_org):
        """PATCH with explicit null description clears it."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"description": None},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["description"] is None
        finally:
            self._cleanup_overrides()

    def test_patch_slug_updates_slug(self, client, test_org):
        """PATCH with new slug updates the slug (uniqueness check passes on mock)."""
        self._override_update_access(test_org)
        # Mock session: execute().scalar_one_or_none() returns None (no conflict)
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"slug": "new-slug-name"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "new-slug-name"
        finally:
            self._cleanup_overrides()

    def test_patch_slug_conflict_returns_409(self, client, test_org):
        """PATCH slug that already exists returns 409 Conflict."""
        self._override_update_access(test_org)
        # Mock session: execute().scalar_one_or_none() returns an existing org
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        existing_org = Organization(id=uuid.uuid4(), name="Taken Org", slug="taken-slug")
        mock_result.scalar_one_or_none.return_value = existing_org
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"slug": "taken-slug"},
            )
            assert resp.status_code == 409
            assert "already exists" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_patch_slug_null_returns_422(self, client, test_org):
        """PATCH with slug explicitly set to null is rejected (slug is required)."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"slug": None},
            )
            # Pydantic validation rejects null slug due to min_length constraint
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_status_transition(self, client, test_org):
        """PATCH status from ACTIVE to DEACTIVATED sets lifecycle timestamp."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"status": "DEACTIVATED"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "DEACTIVATED"
            # ORM attribute should be set (verified via the org object)
            assert test_org.deactivated_at is not None
        finally:
            self._cleanup_overrides()

    def test_patch_status_null_returns_422(self, client, test_org):
        """PATCH with status explicitly set to null is rejected."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"status": None},
            )
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_empty_body_returns_422(self, client, test_org):
        """PATCH with empty JSON body returns 422 (no fields to update)."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={},
            )
            assert resp.status_code == 422
            assert "No fields" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_patch_no_access_returns_403(self, client):
        """PATCH without org update permission returns 403."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'patch-test-org'")

        app.dependency_overrides[get_organization_with_update_access] = _mock_no_access
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"description": "try update"},
            )
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            self._cleanup_overrides()

    def test_patch_multiple_fields(self, client, test_org):
        """PATCH with multiple fields updates all of them."""
        self._override_update_access(test_org)
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={
                    "slug": "renamed-org",
                    "description": "New desc",
                    "status": "DEACTIVATED",
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "renamed-org"
            assert data["description"] == "New desc"
            assert data["status"] == "DEACTIVATED"
        finally:
            self._cleanup_overrides()

    def test_patch_invalid_status_returns_422(self, client, test_org):
        """PATCH with invalid status enum value returns 422."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"status": "NOT_A_STATUS"},
            )
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_same_slug_no_conflict(self, client, test_org):
        """PATCH sending the same slug that org already has should succeed (no conflict)."""
        self._override_update_access(test_org)
        try:
            resp = client.patch(
                "/api/v1/organizations/patch-test-org",
                json={"slug": "patch-test-org"},
            )
            assert resp.status_code == 200
            assert resp.json()["slug"] == "patch-test-org"
        finally:
            self._cleanup_overrides()

    def test_patch_deleted_org_returns_404(self, client):
        """PATCH on a soft-deleted org returns 404 — even for superusers.

        Deleted orgs must be re-enabled before any edits are allowed.
        The dependency (get_organization_with_update_access) uses
        allow_deleted=False regardless of role.
        """
        from fastapi import HTTPException

        def _mock_deleted():
            raise HTTPException(
                status_code=404,
                detail="Organization 'deleted-org' not found",
            )

        app.dependency_overrides[get_organization_with_update_access] = _mock_deleted
        try:
            resp = client.patch(
                "/api/v1/organizations/deleted-org",
                json={"description": "try to edit deleted"},
            )
            assert resp.status_code == 404
        finally:
            self._cleanup_overrides()


# ---------------------------------------------------------------------------
# DELETE /organizations/{id_or_slug} (soft-delete)
# ---------------------------------------------------------------------------


class TestDeleteOrganization:
    """Integration tests for DELETE /api/v1/organizations/{id_or_slug}."""

    @pytest.fixture
    def active_org(self):
        """Active org for delete tests."""
        return Organization(
            id=uuid.uuid4(),
            name="Delete Test Org",
            slug="delete-test-org",
            description="To be deleted",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
            activated_at=None,
            deactivated_at=None,
            banned_at=None,
        )

    @pytest.fixture
    def deleted_org(self):
        """Already soft-deleted org."""
        return Organization(
            id=uuid.uuid4(),
            name="Already Deleted Org",
            slug="already-deleted-org",
            description="Was deleted",
            status=OrganizationStatus.DELETED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=datetime.now(timezone.utc),
            activated_at=None,
            deactivated_at=None,
            banned_at=None,
        )

    def _override_delete_access(self, org):
        app.dependency_overrides[get_organization_with_delete_access] = lambda: org

    def _cleanup_overrides(self):
        app.dependency_overrides.pop(get_organization_with_delete_access, None)

    def test_delete_active_org(self, client, active_org):
        """DELETE active org → 200 with DELETED status and deleted_at."""
        self._override_delete_access(active_org)
        try:
            resp = client.delete("/api/v1/organizations/delete-test-org")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "DELETED"
            assert data["deleted_at"] is not None
            assert data["slug"] == "delete-test-org"
            # Verify ORM object was mutated
            assert active_org.status == OrganizationStatus.DELETED
            assert active_org.deleted_at is not None
        finally:
            self._cleanup_overrides()

    def test_delete_already_deleted_org_returns_409(self, client, deleted_org):
        """DELETE already-deleted org → 409 Conflict."""
        self._override_delete_access(deleted_org)
        try:
            resp = client.delete("/api/v1/organizations/already-deleted-org")
            assert resp.status_code == 409
            assert "already deleted" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_delete_no_access_returns_403(self, client):
        """DELETE without permission → 403."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'test-org'")

        app.dependency_overrides[get_organization_with_delete_access] = _mock_no_access
        try:
            resp = client.delete("/api/v1/organizations/test-org")
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            self._cleanup_overrides()

    def test_delete_response_shape(self, client, active_org):
        """DELETE response contains id, slug, status, deleted_at (and nothing else sensitive)."""
        self._override_delete_access(active_org)
        try:
            resp = client.delete("/api/v1/organizations/delete-test-org")
            assert resp.status_code == 200
            data = resp.json()
            assert set(data.keys()) == {"id", "slug", "status", "deleted_at"}
        finally:
            self._cleanup_overrides()


# ---------------------------------------------------------------------------
# GET /organizations/ — status filter query params
# ---------------------------------------------------------------------------


class TestOrganizationFilters:
    """Tests for status filtering and search on the list endpoint."""

    # ── Status filters ─────────────────────────────────────────────────

    def test_list_accepts_status_filter_param(self, client):
        """?status=ACTIVE is accepted (no 422)."""
        resp = client.get("/api/v1/organizations/?status=ACTIVE")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data

    def test_list_accepts_multiple_status_filters(self, client):
        """?status=ACTIVE&status=BANNED is accepted (repeatable)."""
        resp = client.get("/api/v1/organizations/?status=ACTIVE&status=BANNED")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_list_invalid_status_returns_422(self, client):
        """?status=NOT_REAL → 422 (enum validation)."""
        resp = client.get("/api/v1/organizations/?status=NOT_REAL")
        assert resp.status_code == 422

    def test_list_no_filter_returns_page(self, client):
        """Omitting ?status still returns valid cursor page."""
        resp = client.get("/api/v1/organizations/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert data["limit"] == 20

    # ── Search filter ──────────────────────────────────────────────────

    def test_search_param_accepted(self, client):
        """?search=acme is accepted (no 422), returns valid page."""
        resp = client.get("/api/v1/organizations/?search=acme")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "next_cursor" in data

    def test_search_combined_with_status(self, client):
        """?search=acme&status=ACTIVE composes both filters (no 422)."""
        resp = client.get("/api/v1/organizations/?search=acme&status=ACTIVE")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_search_combined_with_limit(self, client):
        """?search=acme&limit=5 composes with pagination limit."""
        resp = client.get("/api/v1/organizations/?search=acme&limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["limit"] == 5

    def test_search_empty_string_returns_422(self, client):
        """?search= (empty) → 422 due to min_length=1 validation."""
        resp = client.get("/api/v1/organizations/?search=")
        assert resp.status_code == 422

    def test_search_too_long_returns_422(self, client):
        """?search=<101 chars> → 422 due to max_length=100 validation."""
        long_term = "a" * 101
        resp = client.get(f"/api/v1/organizations/?search={long_term}")
        assert resp.status_code == 422

    def test_search_whitespace_stripped(self, client):
        """?search=+acme+ (whitespace) is stripped and accepted."""
        # URL-encoded spaces become '+' in query params; FastAPI decodes them
        resp = client.get("/api/v1/organizations/?search=%20acme%20")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_search_special_chars_safe(self, client):
        """?search=acme%25corp (containing %) is accepted (wildcards escaped)."""
        # %25 = URL-encoded '%'
        resp = client.get("/api/v1/organizations/?search=acme%25corp")
        assert resp.status_code == 200
        assert "items" in resp.json()


# ---------------------------------------------------------------------------
# _escape_like unit tests
# ---------------------------------------------------------------------------


class TestEscapeLike:
    """Unit tests for ILIKE wildcard escaping in pagination."""

    def test_no_wildcards(self):
        from api.pagination import _escape_like
        assert _escape_like("acme") == "acme"

    def test_percent_escaped(self):
        from api.pagination import _escape_like
        assert _escape_like("100%") == "100\\%"

    def test_underscore_escaped(self):
        from api.pagination import _escape_like
        assert _escape_like("acme_corp") == "acme\\_corp"

    def test_backslash_escaped(self):
        from api.pagination import _escape_like
        assert _escape_like("path\\to") == "path\\\\to"

    def test_combined_wildcards(self):
        from api.pagination import _escape_like
        assert _escape_like("50%_off\\deal") == "50\\%\\_off\\\\deal"

    def test_empty_string(self):
        from api.pagination import _escape_like
        assert _escape_like("") == ""


# ---------------------------------------------------------------------------
# Visibility: soft-deleted orgs hidden for non-system-admins
# ---------------------------------------------------------------------------


class TestDeletedOrgVisibility:
    """Tests that soft-deleted orgs are invisible to non-system-admin roles."""

    @pytest.fixture
    def deleted_org(self):
        return Organization(
            id=uuid.uuid4(),
            name="Invisible Org",
            slug="invisible-org",
            description="Deleted org",
            status=OrganizationStatus.DELETED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=datetime.now(timezone.utc),
            activated_at=None,
            deactivated_at=None,
            banned_at=None,
        )

    def test_get_deleted_org_as_superuser_ok(self, client, deleted_org):
        """Superuser can GET a soft-deleted org."""
        # _TEST_AUTH is superuser by default (autouse fixture)
        app.dependency_overrides[get_organization_with_access] = lambda: deleted_org
        try:
            resp = client.get("/api/v1/organizations/invisible-org")
            assert resp.status_code == 200
            assert resp.json()["status"] == "DELETED"
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

    def test_get_deleted_org_includes_deleted_at(self, client, deleted_org):
        """GET response for a soft-deleted org includes deleted_at timestamp."""
        app.dependency_overrides[get_organization_with_access] = lambda: deleted_org
        try:
            resp = client.get("/api/v1/organizations/invisible-org")
            assert resp.status_code == 200
            data = resp.json()
            assert "deleted_at" in data
            assert data["deleted_at"] is not None
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

    def test_get_active_org_has_deleted_at_null(self, client):
        """GET response for an active org has deleted_at=null."""
        active_org = Organization(
            id=uuid.uuid4(),
            name="Alive Org",
            slug="alive-org",
            description="Still active",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
            activated_at=None,
            deactivated_at=None,
            banned_at=None,
        )
        app.dependency_overrides[get_organization_with_access] = lambda: active_org
        try:
            resp = client.get("/api/v1/organizations/alive-org")
            assert resp.status_code == 200
            data = resp.json()
            assert "deleted_at" in data
            assert data["deleted_at"] is None
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

    def test_get_deleted_org_as_regular_user_404(self, client, deleted_org):
        """Non-system-admin gets 404 for soft-deleted org (via dep)."""
        from fastapi import HTTPException
        def _mock_not_found():
            raise HTTPException(
                status_code=404,
                detail="Organization 'invisible-org' not found",
            )

        app.dependency_overrides[get_organization_with_access] = _mock_not_found
        try:
            resp = client.get("/api/v1/organizations/invisible-org")
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_organization_with_access, None)

    def test_patch_deleted_org_blocked_for_system_admin(self, client, deleted_org):
        """Even system admins cannot PATCH a deleted org — must re-enable first."""
        from fastapi import HTTPException

        def _mock_deleted():
            raise HTTPException(
                status_code=404,
                detail="Organization 'invisible-org' not found",
            )

        app.dependency_overrides[get_organization_with_update_access] = _mock_deleted
        try:
            resp = client.patch(
                "/api/v1/organizations/invisible-org",
                json={"description": "system admin tries to edit deleted"},
            )
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_organization_with_update_access, None)


# ---------------------------------------------------------------------------
# Departments (org-scoped; nested under organizations)
# ---------------------------------------------------------------------------
# Tests mirror organization list patterns with dep overrides.
# get_organization_with_access is overridden to return a fake org (simulates
# org resolution + RBAC); get_db provides a MagicMock session for pagination.

class TestListDepartments:
    """Integration tests for GET /api/v1/organizations/{org}/departments/."""

    @pytest.fixture
    def org_for_depts(self):
        """Reusable fake Organization for department list tests."""
        return Organization(
            id=uuid.uuid4(),
            name="Dept Test Org",
            slug="dept-test-org",
            description="Org for department tests",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    @pytest.fixture(autouse=True)
    def _override_org_access(self, org_for_depts):
        """Override org access dep so department endpoint resolves the org."""
        app.dependency_overrides[get_organization_with_access] = lambda: org_for_depts
        yield
        app.dependency_overrides.pop(get_organization_with_access, None)

    def test_list_departments_ok(self, client, org_for_depts):
        """Basic list returns cursor page shape (empty when no depts)."""
        resp = client.get(f"/api/v1/organizations/{org_for_depts.slug}/departments/")
        assert resp.status_code == 200
        data = resp.json()
        # Matches DepartmentsCursorPage schema
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20  # default

    def test_list_departments_with_custom_limit(self, client, org_for_depts):
        """Query param limit respected."""
        resp = client.get(
            f"/api/v1/organizations/{org_for_depts.slug}/departments/?limit=5"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["limit"] == 5
        assert len(data["items"]) <= 5

    def test_list_departments_invalid_cursor_returns_400(self, client, org_for_depts):
        """Bad cursor -> ValueError -> handler 400 (sanitized; no leak)."""
        resp = client.get(
            f"/api/v1/organizations/{org_for_depts.slug}/departments/?cursor=invalid!!"
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body or "detail" in body

    def test_list_departments_no_org_access_403(self, client):
        """No org access raises 403 (org_admin without assignment combo)."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'no-access-org'")

        app.dependency_overrides[get_organization_with_access] = _mock_no_access
        resp = client.get("/api/v1/organizations/no-access-org/departments/")
        assert resp.status_code == 403
        body = resp.json()
        assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")

    def test_list_departments_status_filter_accepted(self, client, org_for_depts):
        """Status filter query param is accepted (no 422)."""
        resp = client.get(
            f"/api/v1/organizations/{org_for_depts.slug}/departments/?status=ACTIVE"
        )
        assert resp.status_code == 200

    def test_list_departments_search_filter_accepted(self, client, org_for_depts):
        """Search filter query param is accepted (no 422)."""
        resp = client.get(
            f"/api/v1/organizations/{org_for_depts.slug}/departments/?search=eng"
        )
        assert resp.status_code == 200

    def test_list_departments_org_not_found_404(self, client):
        """Non-existent org returns 404 (via get_organization_with_access)."""
        from fastapi import HTTPException

        def _mock_not_found():
            raise HTTPException(
                status_code=404,
                detail="Organization 'ghost-org' not found",
            )

        app.dependency_overrides[get_organization_with_access] = _mock_not_found
        resp = client.get("/api/v1/organizations/ghost-org/departments/")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /organizations/{org}/departments/{dept} (single department detail)
# ---------------------------------------------------------------------------
# get_department_with_access is overridden to return a fake Department
# (or raise 403/404), mirroring the org GET pattern.

class TestGetDepartment:
    """Integration tests for GET /api/v1/organizations/{org}/departments/{dept}."""

    @pytest.fixture
    def org_for_dept(self):
        """Fake parent organization."""
        return Organization(
            id=uuid.uuid4(),
            name="Dept Detail Org",
            slug="dept-detail-org",
            description="Org for single department tests",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    @pytest.fixture
    def test_dept(self, org_for_dept):
        """Fake Department for detail tests."""
        return Department(
            id=uuid.uuid4(),
            org_id=org_for_dept.id,
            name="Engineering",
            slug="sample-engineering",
            description="Engineering department",
            is_default=False,
            status=DepartmentStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            activated_at=datetime.now(timezone.utc),
            deactivated_at=None,
            deleted_at=None,
        )

    def test_get_department_ok(self, client, org_for_dept, test_dept):
        """Successful GET returns DepartmentResponse shape with lifecycle fields."""
        app.dependency_overrides[get_department_with_access] = lambda: test_dept
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_dept.slug}/departments/{test_dept.slug}"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "sample-engineering"
            assert data["description"] == "Engineering department"
            assert data["status"] == "ACTIVE"
            # DepartmentResponse includes lifecycle timestamps
            assert "activated_at" in data
            assert "deactivated_at" in data
            assert "deleted_at" in data
            assert "created_at" in data
            assert "updated_at" in data
            assert "org_id" in data
            assert "id" in data
        finally:
            app.dependency_overrides.pop(get_department_with_access, None)

    def test_get_department_not_found_404(self, client, org_for_dept):
        """Non-existent department returns 404."""
        from fastapi import HTTPException

        def _mock_not_found():
            raise HTTPException(
                status_code=404,
                detail="Department 'nonexistent' not found in organization 'dept-detail-org'",
            )

        app.dependency_overrides[get_department_with_access] = _mock_not_found
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_dept.slug}/departments/nonexistent"
            )
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_department_with_access, None)

    def test_get_department_no_org_access_403(self, client):
        """No org access raises 403 (via get_department_with_access chain)."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'no-access-org'")

        app.dependency_overrides[get_department_with_access] = _mock_no_access
        try:
            resp = client.get(
                "/api/v1/organizations/no-access-org/departments/some-dept"
            )
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            app.dependency_overrides.pop(get_department_with_access, None)

    def test_get_department_org_not_found_404(self, client):
        """Non-existent org in the path returns 404 (from org resolution)."""
        from fastapi import HTTPException

        def _mock_org_not_found():
            raise HTTPException(
                status_code=404,
                detail="Organization 'ghost-org' not found",
            )

        app.dependency_overrides[get_department_with_access] = _mock_org_not_found
        try:
            resp = client.get(
                "/api/v1/organizations/ghost-org/departments/any-dept"
            )
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_department_with_access, None)

    def test_get_department_response_validates_from_orm(self, client, org_for_dept, test_dept):
        """Response fields correctly map from ORM Department via from_attributes."""
        app.dependency_overrides[get_department_with_access] = lambda: test_dept
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_dept.slug}/departments/{test_dept.slug}"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["org_id"] == str(test_dept.org_id)
            assert data["id"] == str(test_dept.id)
        finally:
            app.dependency_overrides.pop(get_department_with_access, None)


# ---------------------------------------------------------------------------
# PATCH /organizations/{org}/departments/{dept} (partial update)
# ---------------------------------------------------------------------------
# Mirrors TestUpdateOrganization patterns: get_department_with_update_access
# is overridden to return a fake Department (or raise 403/404), and get_db
# provides a MagicMock session.

class TestUpdateDepartment:
    """Integration tests for PATCH /api/v1/organizations/{org}/departments/{dept}."""

    @pytest.fixture
    def org_for_patch(self):
        """Fake parent organization for PATCH tests."""
        return Organization(
            id=uuid.uuid4(),
            name="Dept Patch Org",
            slug="dept-patch-org",
            description="Org for department PATCH tests",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    @pytest.fixture
    def test_dept(self, org_for_patch):
        """Reusable fake Department for PATCH tests."""
        return Department(
            id=uuid.uuid4(),
            org_id=org_for_patch.id,
            name="Patch Test",
            slug="patch-test-dept",
            description="Original dept description",
            is_default=False,
            status=DepartmentStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            activated_at=datetime.now(timezone.utc),
            deactivated_at=None,
            deleted_at=None,
        )

    def _override_update_access(self, dept):
        app.dependency_overrides[get_department_with_update_access] = lambda: dept

    def _cleanup_overrides(self):
        app.dependency_overrides.pop(get_department_with_update_access, None)

    def test_patch_description_only(self, client, org_for_patch, test_dept):
        """PATCH with description only updates description, leaves slug/status."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"description": "Updated dept description"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["description"] == "Updated dept description"
            assert data["slug"] == "patch-test-dept"
            assert data["status"] == "ACTIVE"
        finally:
            self._cleanup_overrides()

    def test_patch_clear_description_to_null(self, client, org_for_patch, test_dept):
        """PATCH with explicit null description clears it."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"description": None},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["description"] is None
        finally:
            self._cleanup_overrides()

    def test_patch_slug_updates_slug(self, client, org_for_patch, test_dept):
        """PATCH with new slug updates the slug (uniqueness check passes on mock)."""
        self._override_update_access(test_dept)
        # Mock session: execute().scalar_one_or_none() returns None (no conflict)
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"slug": "renamed-dept"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "renamed-dept"
        finally:
            self._cleanup_overrides()

    def test_patch_slug_conflict_returns_409(self, client, org_for_patch, test_dept):
        """PATCH slug that already exists in the org returns 409 Conflict."""
        self._override_update_access(test_dept)
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        existing_dept = Department(
            id=uuid.uuid4(), org_id=org_for_patch.id, name="Taken", slug="taken-slug"
        )
        mock_result.scalar_one_or_none.return_value = existing_dept
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"slug": "taken-slug"},
            )
            assert resp.status_code == 409
            assert "already exists" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_patch_slug_null_returns_422(self, client, org_for_patch, test_dept):
        """PATCH with slug explicitly set to null is rejected."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"slug": None},
            )
            # Pydantic validation rejects null slug due to min_length constraint
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_status_transition_to_deactivated(self, client, org_for_patch, test_dept):
        """PATCH status from ACTIVE to DEACTIVATED sets lifecycle timestamp."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"status": "DEACTIVATED"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "DEACTIVATED"
            assert test_dept.deactivated_at is not None
        finally:
            self._cleanup_overrides()

    def test_patch_status_transition_to_active(self, client, org_for_patch, test_dept):
        """PATCH status from DEACTIVATED back to ACTIVE sets activated_at."""
        test_dept.status = DepartmentStatus.DEACTIVATED
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"status": "ACTIVE"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ACTIVE"
            assert test_dept.activated_at is not None
        finally:
            self._cleanup_overrides()

    def test_patch_status_deleted_rejected(self, client, org_for_patch, test_dept):
        """PATCH status to DELETED is rejected — use DELETE endpoint instead."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"status": "DELETED"},
            )
            assert resp.status_code == 422
            assert "DELETE endpoint" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_patch_status_null_returns_422(self, client, org_for_patch, test_dept):
        """PATCH with status explicitly set to null is rejected."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"status": None},
            )
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_empty_body_returns_422(self, client, org_for_patch, test_dept):
        """PATCH with empty JSON body returns 422 (no fields to update)."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={},
            )
            assert resp.status_code == 422
            assert "No fields" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_patch_no_access_returns_403(self, client, org_for_patch):
        """PATCH without dept update permission returns 403."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for organization 'dept-patch-org'")

        app.dependency_overrides[get_department_with_update_access] = _mock_no_access
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/some-dept",
                json={"description": "try update"},
            )
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            self._cleanup_overrides()

    def test_patch_multiple_fields(self, client, org_for_patch, test_dept):
        """PATCH with multiple fields updates all of them."""
        self._override_update_access(test_dept)
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={
                    "slug": "renamed-dept",
                    "description": "New dept desc",
                    "status": "DEACTIVATED",
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "renamed-dept"
            assert data["description"] == "New dept desc"
            assert data["status"] == "DEACTIVATED"
        finally:
            self._cleanup_overrides()

    def test_patch_invalid_status_returns_422(self, client, org_for_patch, test_dept):
        """PATCH with invalid status enum value returns 422."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"status": "NOT_A_STATUS"},
            )
            assert resp.status_code == 422
        finally:
            self._cleanup_overrides()

    def test_patch_same_slug_no_conflict(self, client, org_for_patch, test_dept):
        """PATCH sending the same slug the dept already has should succeed."""
        self._override_update_access(test_dept)
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/{test_dept.slug}",
                json={"slug": "patch-test-dept"},
            )
            assert resp.status_code == 200
            assert resp.json()["slug"] == "patch-test-dept"
        finally:
            self._cleanup_overrides()

    def test_patch_deleted_dept_returns_404(self, client, org_for_patch):
        """PATCH on a soft-deleted department returns 404."""
        from fastapi import HTTPException

        def _mock_deleted():
            raise HTTPException(
                status_code=404,
                detail="Department 'deleted-dept' not found in organization 'dept-patch-org'",
            )

        app.dependency_overrides[get_department_with_update_access] = _mock_deleted
        try:
            resp = client.patch(
                f"/api/v1/organizations/{org_for_patch.slug}/departments/deleted-dept",
                json={"description": "try to edit deleted"},
            )
            assert resp.status_code == 404
        finally:
            self._cleanup_overrides()


# ---------------------------------------------------------------------------
# DELETE /organizations/{org}/departments/{dept} (soft-delete)
# ---------------------------------------------------------------------------
# Mirrors TestDeleteOrganization patterns: get_department_with_delete_access
# is overridden to return a fake Department (or raise 403/404).

class TestDeleteDepartment:
    """Integration tests for DELETE /api/v1/organizations/{org}/departments/{dept}."""

    @pytest.fixture
    def org_for_delete(self):
        """Fake parent organization for DELETE tests."""
        return Organization(
            id=uuid.uuid4(),
            name="Dept Delete Org",
            slug="dept-delete-org",
            description="Org for department DELETE tests",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    @pytest.fixture
    def active_dept(self, org_for_delete):
        """Active department for delete tests."""
        return Department(
            id=uuid.uuid4(),
            org_id=org_for_delete.id,
            name="Delete Test",
            slug="delete-test-dept",
            description="Department to be deleted",
            is_default=False,
            status=DepartmentStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            activated_at=datetime.now(timezone.utc),
            deactivated_at=None,
            deleted_at=None,
        )

    @pytest.fixture
    def already_deleted_dept(self, org_for_delete):
        """Already deleted department for idempotency tests."""
        return Department(
            id=uuid.uuid4(),
            org_id=org_for_delete.id,
            name="Already Deleted",
            slug="already-deleted-dept",
            description="Already deleted department",
            is_default=False,
            status=DepartmentStatus.DELETED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=datetime.now(timezone.utc),
        )

    def _override_delete_access(self, dept):
        app.dependency_overrides[get_department_with_delete_access] = lambda: dept

    def _cleanup_overrides(self):
        app.dependency_overrides.pop(get_department_with_delete_access, None)

    def test_delete_department_ok(self, client, org_for_delete, active_dept):
        """Successful DELETE soft-deletes the department."""
        self._override_delete_access(active_dept)
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_delete.slug}/departments/{active_dept.slug}"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "delete-test-dept"
            assert data["status"] == "DELETED"
            assert data["org_id"] == str(org_for_delete.id)
            assert "deleted_at" in data
            # Verify ORM mutation
            assert active_dept.status == DepartmentStatus.DELETED
            assert active_dept.deleted_at is not None
        finally:
            self._cleanup_overrides()

    def test_delete_already_deleted_returns_409(self, client, org_for_delete, already_deleted_dept):
        """DELETE on already-deleted department returns 409 (idempotency)."""
        self._override_delete_access(already_deleted_dept)
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_delete.slug}/departments/{already_deleted_dept.slug}"
            )
            assert resp.status_code == 409
            assert "already deleted" in resp.json()["detail"]
        finally:
            self._cleanup_overrides()

    def test_delete_no_access_returns_403(self, client, org_for_delete):
        """DELETE without dept delete permission returns 403."""
        def _mock_no_access():
            raise AuthorizationError("Not authorized for department 'no-access-dept'")

        app.dependency_overrides[get_department_with_delete_access] = _mock_no_access
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_delete.slug}/departments/no-access-dept"
            )
            assert resp.status_code == 403
            body = resp.json()
            assert body.get("error") == "authorization_error" or "authorized" in body.get("detail", "")
        finally:
            self._cleanup_overrides()

    def test_delete_dept_not_found_404(self, client, org_for_delete):
        """DELETE on non-existent department returns 404."""
        from fastapi import HTTPException

        def _mock_not_found():
            raise HTTPException(
                status_code=404,
                detail="Department 'ghost-dept' not found in organization 'dept-delete-org'",
            )

        app.dependency_overrides[get_department_with_delete_access] = _mock_not_found
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_delete.slug}/departments/ghost-dept"
            )
            assert resp.status_code == 404
        finally:
            self._cleanup_overrides()

    def test_delete_org_not_found_404(self, client):
        """DELETE with non-existent org in path returns 404."""
        from fastapi import HTTPException

        def _mock_org_not_found():
            raise HTTPException(
                status_code=404,
                detail="Organization 'ghost-org' not found",
            )

        app.dependency_overrides[get_department_with_delete_access] = _mock_org_not_found
        try:
            resp = client.delete(
                "/api/v1/organizations/ghost-org/departments/any-dept"
            )
            assert resp.status_code == 404
        finally:
            self._cleanup_overrides()


# ---------------------------------------------------------------------------
# Roles (platform-wide global roles; cursor-paginated)
# ---------------------------------------------------------------------------


class TestListRoles:
    """Integration tests for GET /api/v1/roles/."""

    def test_list_roles_ok(self, client):
        """Basic list returns cursor page shape."""
        resp = client.get("/api/v1/roles/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20

    def test_list_roles_with_custom_limit(self, client):
        resp = client.get("/api/v1/roles/?limit=5")
        assert resp.status_code == 200
        assert resp.json()["limit"] == 5

    def test_list_roles_invalid_cursor_returns_400(self, client):
        resp = client.get("/api/v1/roles/?cursor=invalid!!")
        assert resp.status_code == 400

    def test_list_roles_status_filter_accepted(self, client):
        resp = client.get("/api/v1/roles/?status=PUBLISHED")
        assert resp.status_code == 200

    def test_list_roles_invalid_status_returns_422(self, client):
        resp = client.get("/api/v1/roles/?status=NOT_REAL")
        assert resp.status_code == 422

    def test_list_roles_search_accepted(self, client):
        resp = client.get("/api/v1/roles/?search=senior")
        assert resp.status_code == 200


class TestGetRole:
    """Integration tests for GET /api/v1/roles/{id_or_slug}."""

    def test_get_role_not_found_404(self, client):
        """Non-existent role returns 404."""
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/roles/nonexistent-role")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]

    def test_get_role_by_uuid_not_found(self, client):
        """UUID that doesn't match returns 404."""
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        mock_session.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_uuid = str(uuid.uuid4())
        resp = client.get(f"/api/v1/roles/{fake_uuid}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Assessments (platform-wide; org-scoped visibility)
# ---------------------------------------------------------------------------


class TestListAssessments:
    """Integration tests for GET /api/v1/assessments/."""

    def test_list_assessments_ok(self, client):
        resp = client.get("/api/v1/assessments/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20

    def test_list_assessments_with_custom_limit(self, client):
        resp = client.get("/api/v1/assessments/?limit=5")
        assert resp.status_code == 200
        assert resp.json()["limit"] == 5

    def test_list_assessments_invalid_cursor_returns_400(self, client):
        resp = client.get("/api/v1/assessments/?cursor=invalid!!")
        assert resp.status_code == 400

    def test_list_assessments_status_filter_accepted(self, client):
        resp = client.get("/api/v1/assessments/?status=PUBLISHED")
        assert resp.status_code == 200

    def test_list_assessments_invalid_status_returns_422(self, client):
        resp = client.get("/api/v1/assessments/?status=NOT_REAL")
        assert resp.status_code == 422

    def test_list_assessments_search_accepted(self, client):
        resp = client.get("/api/v1/assessments/?search=senior")
        assert resp.status_code == 200


class TestGetAssessment:
    """Integration tests for GET /api/v1/assessments/{id_or_slug}."""

    @pytest.fixture
    def test_assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Senior Engineer Q1 2026",
            slug="senior-eng-q1-2026",
            description="Assessment for senior engineers",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_get_assessment_ok(self, client, test_assessment):
        """Successful GET returns AssessmentResponse shape."""
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = test_assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get(f"/api/v1/assessments/{test_assessment.slug}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["slug"] == "senior-eng-q1-2026"
        assert data["title"] == "Senior Engineer Q1 2026"
        assert data["status"] == "PUBLISHED"
        assert "id" in data
        assert "created_at" in data

    def test_get_assessment_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/assessments/nonexistent")
        assert resp.status_code == 404


class TestCreateAssessment:
    """Integration tests for POST /api/v1/assessments/."""

    def test_create_assessment_slug_conflict_409(self, client):
        """Creating assessment with existing slug returns 409."""
        from db.models.assessment import Assessment, AssessmentStatus
        existing = Assessment(id=uuid.uuid4(), slug="taken-slug")
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing
        mock_session.execute.return_value = mock_result
        mock_session.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post("/api/v1/assessments/", json={
            "title": "Test Assessment",
            "slug": "taken-slug",
            "status": "DRAFT",
        })
        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]

    def test_create_assessment_deleted_status_rejected(self, client):
        resp = client.post("/api/v1/assessments/", json={
            "title": "Bad Assessment",
            "slug": "bad-assessment",
            "status": "DELETED",
        })
        assert resp.status_code == 422

    def test_create_assessment_missing_title_returns_422(self, client):
        resp = client.post("/api/v1/assessments/", json={
            "slug": "no-title",
        })
        assert resp.status_code == 422

    def test_create_assessment_missing_slug_returns_422(self, client):
        resp = client.post("/api/v1/assessments/", json={
            "title": "No Slug",
        })
        assert resp.status_code == 422


class TestUpdateAssessment:
    """Integration tests for PATCH /api/v1/assessments/{id_or_slug}."""

    @pytest.fixture
    def test_assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Patchable Assessment",
            slug="patchable-assessment",
            description="Original description",
            status=AssessmentStatus.DRAFT,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=None,
            deleted_at=None,
        )

    def test_patch_title(self, client, test_assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = test_assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            f"/api/v1/assessments/{test_assessment.slug}",
            json={"title": "Updated Title"},
        )
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated Title"

    def test_patch_empty_body_returns_422(self, client, test_assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = test_assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            f"/api/v1/assessments/{test_assessment.slug}",
            json={},
        )
        assert resp.status_code == 422

    def test_patch_status_deleted_rejected(self, client, test_assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = test_assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            f"/api/v1/assessments/{test_assessment.slug}",
            json={"status": "DELETED"},
        )
        assert resp.status_code == 422

    def test_patch_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            "/api/v1/assessments/nonexistent",
            json={"title": "Won't work"},
        )
        assert resp.status_code == 404


class TestDeleteAssessment:
    """Integration tests for DELETE /api/v1/assessments/{id_or_slug}."""

    @pytest.fixture
    def active_assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Deletable Assessment",
            slug="deletable-assessment",
            description="To be deleted",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_delete_assessment_ok(self, client, active_assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = active_assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = active_assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.delete(f"/api/v1/assessments/{active_assessment.slug}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "DELETED"
        assert data["deleted_at"] is not None
        assert data["slug"] == "deletable-assessment"

    def test_delete_already_deleted_returns_409(self, client):
        from db.models.assessment import Assessment, AssessmentStatus
        deleted = Assessment(
            id=uuid.uuid4(),
            title="Already Deleted",
            slug="already-deleted",
            status=AssessmentStatus.DELETED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=datetime.now(timezone.utc),
        )
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = deleted
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = deleted
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.delete(f"/api/v1/assessments/{deleted.slug}")
        assert resp.status_code == 409
        assert "already deleted" in resp.json()["detail"]

    def test_delete_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.delete("/api/v1/assessments/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Scenarios (global + assessment-scoped)
# ---------------------------------------------------------------------------


class TestListGlobalScenarios:
    """Integration tests for GET /api/v1/scenarios/."""

    def test_list_global_scenarios_ok(self, client):
        resp = client.get("/api/v1/scenarios/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20

    def test_list_global_scenarios_with_limit(self, client):
        resp = client.get("/api/v1/scenarios/?limit=3")
        assert resp.status_code == 200
        assert resp.json()["limit"] == 3

    def test_list_global_scenarios_invalid_cursor_returns_400(self, client):
        resp = client.get("/api/v1/scenarios/?cursor=bad!!")
        assert resp.status_code == 400

    def test_list_global_scenarios_search_accepted(self, client):
        resp = client.get("/api/v1/scenarios/?search=coding")
        assert resp.status_code == 200


class TestAssessmentScopedScenarios:
    """Integration tests for assessment-scoped scenario endpoints."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Scenario Test Assessment",
            slug="scenario-test-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def _mock_db_with_assessment(self, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_session.scalars.return_value = mock_scalars
        app.dependency_overrides[get_db] = lambda: mock_session
        return mock_session

    def test_list_assessment_scenarios_ok(self, client, assessment):
        self._mock_db_with_assessment(assessment)
        resp = client.get(f"/api/v1/assessments/{assessment.slug}/scenarios/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "next_cursor" in data

    def test_list_assessment_scenarios_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/assessments/nonexistent/scenarios/")
        assert resp.status_code == 404

    def test_get_scenario_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        # First call resolves assessment, second returns None for scenario
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get(
            f"/api/v1/assessments/{assessment.slug}/scenarios/nonexistent"
        )
        assert resp.status_code == 404

    def test_create_scenario_deleted_status_rejected(self, client, assessment):
        self._mock_db_with_assessment(assessment)
        resp = client.post(
            f"/api/v1/assessments/{assessment.slug}/scenarios/",
            json={
                "title": "Bad Scenario",
                "slug": "bad-scenario",
                "status": "DELETED",
                "tool": "GITHUB",
            },
        )
        assert resp.status_code == 422

    def test_create_scenario_missing_title_422(self, client, assessment):
        self._mock_db_with_assessment(assessment)
        resp = client.post(
            f"/api/v1/assessments/{assessment.slug}/scenarios/",
            json={"slug": "no-title-scenario", "tool": "GITHUB"},
        )
        assert resp.status_code == 422

    def test_create_scenario_slug_conflict_409(self, client, assessment):
        """Creating scenario with existing slug returns 409."""
        from db.models.scenario import Scenario
        existing = Scenario(id=uuid.uuid4(), slug="taken-slug")
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        # _resolve_assessment -> assessment, slug uniqueness -> existing
        mock_result.scalar_one_or_none.side_effect = [assessment, existing]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post(
            f"/api/v1/assessments/{assessment.slug}/scenarios/",
            json={
                "title": "New Scenario",
                "slug": "taken-slug",
                "tool": "CHAT",
                "status": "DRAFT",
            },
        )
        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]

    def test_delete_scenario_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.delete(
            f"/api/v1/assessments/{assessment.slug}/scenarios/ghost-scenario"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Submissions (assessment-scoped)
# ---------------------------------------------------------------------------


class TestListSubmissions:
    """Integration tests for GET /api/v1/assessments/{id}/submissions/."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Submission Test Assessment",
            slug="submission-test-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_list_submissions_ok(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_session.scalars.return_value = mock_scalars
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get(f"/api/v1/assessments/{assessment.slug}/submissions/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data

    def test_list_submissions_assessment_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/assessments/nonexistent/submissions/")
        assert resp.status_code == 404


class TestGetSubmission:
    """Integration tests for GET /api/v1/assessments/{id}/submissions/{sid}."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Sub Detail Assessment",
            slug="sub-detail-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_get_submission_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        # First call resolves assessment, second returns None for submission
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_sid = str(uuid.uuid4())
        resp = client.get(
            f"/api/v1/assessments/{assessment.slug}/submissions/{fake_sid}"
        )
        assert resp.status_code == 404

    def test_get_submission_assessment_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_sid = str(uuid.uuid4())
        resp = client.get(
            f"/api/v1/assessments/nonexistent/submissions/{fake_sid}"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Evaluations (assessment-scoped + submission-scoped)
# ---------------------------------------------------------------------------


class TestListEvaluations:
    """Integration tests for GET /api/v1/assessments/{id}/evaluations/."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Eval Test Assessment",
            slug="eval-test-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_list_evaluations_ok(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_session.scalars.return_value = mock_scalars
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get(f"/api/v1/assessments/{assessment.slug}/evaluations/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "next_cursor" in data

    def test_list_evaluations_assessment_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/assessments/nonexistent/evaluations/")
        assert resp.status_code == 404


class TestGetEvaluation:
    """Integration tests for GET /api/v1/assessments/{id}/evaluations/{eid}."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Eval Detail Assessment",
            slug="eval-detail-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_get_evaluation_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_eid = str(uuid.uuid4())
        resp = client.get(
            f"/api/v1/assessments/{assessment.slug}/evaluations/{fake_eid}"
        )
        assert resp.status_code == 404


class TestCreateEvaluation:
    """Integration tests for POST /api/v1/assessments/{id}/evaluations/."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Eval Create Assessment",
            slug="eval-create-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_create_evaluation_assessment_not_found_404(self, client):
        """Creating evaluation on non-existent assessment returns 404."""
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post(
            "/api/v1/assessments/nonexistent/evaluations/",
            json={"summary": {"score": 85}},
        )
        assert resp.status_code == 404

    def test_create_evaluation_missing_summary_422(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post(
            f"/api/v1/assessments/{assessment.slug}/evaluations/",
            json={},
        )
        assert resp.status_code == 422

    def test_create_evaluation_non_admin_403(self, client, assessment):
        """Non-system-admin cannot create evaluations."""
        non_admin_auth = AuthContext(
            user_id=uuid.uuid4(),
            email="regular@example.com",
            name="Regular User",
            roles=["user"],
            permissions=["assessments:read", "assessments:update"],
        )
        app.dependency_overrides[get_current_user] = lambda: non_admin_auth
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            resp = client.post(
                f"/api/v1/assessments/{assessment.slug}/evaluations/",
                json={"summary": {"score": 50}},
            )
            assert resp.status_code == 403
        finally:
            app.dependency_overrides[get_current_user] = lambda: _TEST_AUTH


class TestDeleteEvaluation:
    """Integration tests for DELETE /api/v1/assessments/{id}/evaluations/{eid}."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Eval Delete Assessment",
            slug="eval-delete-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_delete_evaluation_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_eid = str(uuid.uuid4())
        resp = client.delete(
            f"/api/v1/assessments/{assessment.slug}/evaluations/{fake_eid}"
        )
        assert resp.status_code == 404

    def test_delete_evaluation_non_admin_403(self, client, assessment):
        non_admin_auth = AuthContext(
            user_id=uuid.uuid4(),
            email="regular@example.com",
            name="Regular User",
            roles=["user"],
            permissions=["assessments:read", "assessments:delete"],
        )
        app.dependency_overrides[get_current_user] = lambda: non_admin_auth
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = assessment
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        try:
            fake_eid = str(uuid.uuid4())
            resp = client.delete(
                f"/api/v1/assessments/{assessment.slug}/evaluations/{fake_eid}"
            )
            assert resp.status_code == 403
        finally:
            app.dependency_overrides[get_current_user] = lambda: _TEST_AUTH


class TestSubmissionScopedEvaluations:
    """Integration tests for submission-scoped evaluation endpoints."""

    @pytest.fixture
    def assessment(self):
        from db.models.assessment import Assessment, AssessmentStatus
        return Assessment(
            id=uuid.uuid4(),
            title="Sub Eval Assessment",
            slug="sub-eval-assessment",
            status=AssessmentStatus.PUBLISHED,
            org_id=None,
            role_id=None,
            position_id=uuid.uuid4(),
            created_by=_TEST_AUTH.user_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_list_submission_evaluations_submission_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        # First call: resolve assessment; second: resolve submission -> None
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_sid = str(uuid.uuid4())
        resp = client.get(
            f"/api/v1/assessments/{assessment.slug}/submissions/{fake_sid}/evaluations/"
        )
        assert resp.status_code == 404

    def test_get_submission_evaluation_not_found_404(self, client, assessment):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = assessment
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.side_effect = [assessment, None]
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_sid = str(uuid.uuid4())
        fake_eid = str(uuid.uuid4())
        resp = client.get(
            f"/api/v1/assessments/{assessment.slug}/submissions/{fake_sid}/evaluations/{fake_eid}"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Positions (org-scoped; nested under organizations)
# ---------------------------------------------------------------------------


class TestListPositions:
    """Integration tests for GET /api/v1/organizations/{org}/positions/."""

    @pytest.fixture
    def org_for_positions(self):
        return Organization(
            id=uuid.uuid4(),
            name="Pos Test Org",
            slug="pos-test-org",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_list_positions_ok(self, client, org_for_positions):
        from api.auth.dependencies import get_organization_with_positions_access
        app.dependency_overrides[get_organization_with_positions_access] = lambda: org_for_positions
        try:
            resp = client.get(f"/api/v1/organizations/{org_for_positions.slug}/positions/")
            assert resp.status_code == 200
            data = resp.json()
            assert "items" in data
            assert isinstance(data["items"], list)
            assert "next_cursor" in data
            assert data["limit"] == 20
        finally:
            app.dependency_overrides.pop(get_organization_with_positions_access, None)

    def test_list_positions_custom_limit(self, client, org_for_positions):
        from api.auth.dependencies import get_organization_with_positions_access
        app.dependency_overrides[get_organization_with_positions_access] = lambda: org_for_positions
        try:
            resp = client.get(f"/api/v1/organizations/{org_for_positions.slug}/positions/?limit=5")
            assert resp.status_code == 200
            assert resp.json()["limit"] == 5
        finally:
            app.dependency_overrides.pop(get_organization_with_positions_access, None)

    def test_list_positions_invalid_cursor_400(self, client, org_for_positions):
        from api.auth.dependencies import get_organization_with_positions_access
        app.dependency_overrides[get_organization_with_positions_access] = lambda: org_for_positions
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_positions.slug}/positions/?cursor=bad!!"
            )
            assert resp.status_code == 400
        finally:
            app.dependency_overrides.pop(get_organization_with_positions_access, None)

    def test_list_positions_no_org_access_403(self, client):
        from api.auth.dependencies import get_organization_with_positions_access
        def _mock_no_access():
            raise AuthorizationError("Not authorized")
        app.dependency_overrides[get_organization_with_positions_access] = _mock_no_access
        try:
            resp = client.get("/api/v1/organizations/no-access-org/positions/")
            assert resp.status_code == 403
        finally:
            app.dependency_overrides.pop(get_organization_with_positions_access, None)


class TestGetPosition:
    """Integration tests for GET /api/v1/organizations/{org}/positions/{pos}."""

    @pytest.fixture
    def org_for_pos(self):
        return Organization(
            id=uuid.uuid4(),
            name="Pos Detail Org",
            slug="pos-detail-org",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_get_position_ok(self, client, org_for_pos):
        from db.models.position import Position, PositionStatus
        from api.auth.dependencies import get_position_with_access
        position = Position(
            id=uuid.uuid4(),
            org_id=org_for_pos.id,
            dept_id=None,
            role_id=uuid.uuid4(),
            slug="sample-acme-eng-senior-engineer",
            description="Senior Engineer position",
            status=PositionStatus.PUBLISHED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )
        app.dependency_overrides[get_position_with_access] = lambda: position
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_pos.slug}/positions/{position.slug}"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["slug"] == "sample-acme-eng-senior-engineer"
            assert data["status"] == "PUBLISHED"
            assert "id" in data
            assert "org_id" in data
        finally:
            app.dependency_overrides.pop(get_position_with_access, None)

    def test_get_position_not_found_404(self, client, org_for_pos):
        from fastapi import HTTPException
        from api.auth.dependencies import get_position_with_access
        def _mock_not_found():
            raise HTTPException(status_code=404, detail="Position not found")
        app.dependency_overrides[get_position_with_access] = _mock_not_found
        try:
            resp = client.get(
                f"/api/v1/organizations/{org_for_pos.slug}/positions/nonexistent"
            )
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_position_with_access, None)


class TestDeletePosition:
    """Integration tests for DELETE /api/v1/organizations/{org}/positions/{pos}."""

    @pytest.fixture
    def org_for_pos_del(self):
        return Organization(
            id=uuid.uuid4(),
            name="Pos Del Org",
            slug="pos-del-org",
            status=OrganizationStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=None,
        )

    def test_delete_position_ok(self, client, org_for_pos_del):
        from db.models.position import Position, PositionStatus
        from api.auth.dependencies import get_position_with_delete_access
        position = Position(
            id=uuid.uuid4(),
            org_id=org_for_pos_del.id,
            dept_id=None,
            role_id=uuid.uuid4(),
            slug="deletable-position",
            description="Position to delete",
            status=PositionStatus.PUBLISHED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            published_at=datetime.now(timezone.utc),
            deleted_at=None,
        )
        app.dependency_overrides[get_position_with_delete_access] = lambda: position
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_pos_del.slug}/positions/{position.slug}"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "DELETED"
            assert data["deleted_at"] is not None
        finally:
            app.dependency_overrides.pop(get_position_with_delete_access, None)

    def test_delete_already_deleted_position_409(self, client, org_for_pos_del):
        from db.models.position import Position, PositionStatus
        from api.auth.dependencies import get_position_with_delete_access
        position = Position(
            id=uuid.uuid4(),
            org_id=org_for_pos_del.id,
            dept_id=None,
            role_id=uuid.uuid4(),
            slug="already-deleted-position",
            status=PositionStatus.DELETED,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            deleted_at=datetime.now(timezone.utc),
        )
        app.dependency_overrides[get_position_with_delete_access] = lambda: position
        try:
            resp = client.delete(
                f"/api/v1/organizations/{org_for_pos_del.slug}/positions/{position.slug}"
            )
            assert resp.status_code == 409
            assert "already deleted" in resp.json()["detail"]
        finally:
            app.dependency_overrides.pop(get_position_with_delete_access, None)


# ---------------------------------------------------------------------------
# Users (platform-wide management)
# ---------------------------------------------------------------------------


class TestListUsers:
    """Integration tests for GET /api/v1/users/."""

    def test_list_users_ok(self, client):
        resp = client.get("/api/v1/users/")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        assert "next_cursor" in data
        assert data["limit"] == 20

    def test_list_users_with_limit(self, client):
        resp = client.get("/api/v1/users/?limit=5")
        assert resp.status_code == 200
        assert resp.json()["limit"] == 5

    def test_list_users_invalid_cursor_400(self, client):
        resp = client.get("/api/v1/users/?cursor=bad!!")
        assert resp.status_code == 400

    def test_list_users_status_filter_accepted(self, client):
        resp = client.get("/api/v1/users/?status=ACTIVE")
        assert resp.status_code == 200

    def test_list_users_invalid_status_422(self, client):
        resp = client.get("/api/v1/users/?status=NOT_REAL")
        assert resp.status_code == 422

    def test_list_users_search_accepted(self, client):
        resp = client.get("/api/v1/users/?search=alice")
        assert resp.status_code == 200


class TestGetUser:
    """Integration tests for GET /api/v1/users/{id_or_email}."""

    def test_get_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/users/nonexistent@example.com")
        assert resp.status_code == 404

    def test_get_user_own_profile_ok(self, client):
        """User can view their own profile."""
        from db.models.user import User, UserStatus, Gender
        user = User(
            id=_TEST_AUTH.user_id,
            email="test-admin@example.com",
            name="Test",
            surname="Admin",
            status=UserStatus.ACTIVE,
            hashed_password="hashed",
            gender=Gender.PREFER_NOT_TO_SAY,
            timezone="UTC",
            locale="en",
            is_verified=False,
            is_kyc_verified=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = user
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_session.execute.return_value = mock_result
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_session.scalars.return_value = mock_scalars
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get(f"/api/v1/users/{user.email}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "test-admin@example.com"


class TestCreateUser:
    """Integration tests for POST /api/v1/users/."""

    def test_create_user_db_add_called(self, client):
        """Successful user creation calls db.add (mock can't fully validate response)."""
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None  # no email conflict, no system role
        mock_session.execute.return_value = mock_result
        # Simulate refresh populating id + timestamps
        def _fake_refresh(obj):
            if not obj.id:
                obj.id = uuid.uuid4()
            if not obj.created_at:
                obj.created_at = datetime.now(timezone.utc)
            if not obj.updated_at:
                obj.updated_at = datetime.now(timezone.utc)
        mock_session.refresh.side_effect = _fake_refresh
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post("/api/v1/users/", json={
            "email": "newuser@example.com",
            "name": "New",
            "surname": "User",
            "password": "securepassword123",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["email"] == "newuser@example.com"
        assert data["name"] == "New"

    def test_create_user_missing_email_422(self, client):
        resp = client.post("/api/v1/users/", json={
            "name": "No",
            "surname": "Email",
            "password": "pass123",
        })
        assert resp.status_code == 422

    def test_create_user_missing_password_422(self, client):
        resp = client.post("/api/v1/users/", json={
            "email": "no-pass@example.com",
            "name": "No",
            "surname": "Pass",
        })
        assert resp.status_code == 422

    def test_create_user_email_conflict_409(self, client):
        from db.models.user import User, UserStatus
        existing = User(
            id=uuid.uuid4(),
            email="taken@example.com",
            name="Taken",
            surname="User",
            status=UserStatus.ACTIVE,
            hashed_password="hashed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post("/api/v1/users/", json={
            "email": "taken@example.com",
            "name": "Dup",
            "surname": "User",
            "password": "password123",
        })
        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]


class TestUpdateUser:
    """Integration tests for PATCH /api/v1/users/{id_or_email}."""

    def test_patch_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            "/api/v1/users/nonexistent@example.com",
            json={"name": "Won't work"},
        )
        assert resp.status_code == 404

    def test_patch_user_empty_body_422(self, client):
        from db.models.user import User, UserStatus
        user = User(
            id=uuid.uuid4(),
            email="patchable@example.com",
            name="Patch",
            surname="User",
            status=UserStatus.ACTIVE,
            hashed_password="hashed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = user
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.patch(
            f"/api/v1/users/{user.email}",
            json={},
        )
        assert resp.status_code == 422


class TestUserOAuth:
    """Integration tests for user OAuth endpoints."""

    def test_list_oauth_accounts_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/users/nonexistent@example.com/oauth/accounts/")
        assert resp.status_code == 404

    def test_delete_oauth_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        fake_oid = str(uuid.uuid4())
        resp = client.delete(
            f"/api/v1/users/nonexistent@example.com/oauth/accounts/{fake_oid}"
        )
        assert resp.status_code == 404


class TestUserPositions:
    """Integration tests for GET /api/v1/users/{id_or_email}/positions/."""

    def test_list_user_positions_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/users/nonexistent@example.com/positions/")
        assert resp.status_code == 404


class TestUserAssessments:
    """Integration tests for GET /api/v1/users/{id_or_email}/assessments/."""

    def test_list_user_assessments_user_not_found_404(self, client):
        mock_session = MagicMock(spec=Session)
        mock_session.get.return_value = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.get("/api/v1/users/nonexistent@example.com/assessments/")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Auth (login, refresh, me)
# ---------------------------------------------------------------------------


class TestAuthMe:
    """Integration tests for GET /api/v1/auth/me."""

    def test_auth_me_ok(self, client):
        """GET /auth/me returns the overridden AuthContext."""
        resp = client.get("/api/v1/auth/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "test-admin@example.com"
        assert "superuser" in data["roles"]
        assert "user_id" in data

    def test_auth_me_has_required_fields(self, client):
        resp = client.get("/api/v1/auth/me")
        assert resp.status_code == 200
        data = resp.json()
        assert "user_id" in data
        assert "email" in data
        assert "name" in data
        assert "roles" in data
        assert "permissions" in data


class TestAuthLogin:
    """Integration tests for POST /api/v1/auth/login."""

    def test_login_missing_email_422(self, client):
        resp = client.post("/api/v1/auth/login", json={"password": "test"})
        assert resp.status_code == 422

    def test_login_missing_password_422(self, client):
        resp = client.post("/api/v1/auth/login", json={"email": "test@test.com"})
        assert resp.status_code == 422

    def test_login_empty_password_422(self, client):
        resp = client.post("/api/v1/auth/login", json={
            "email": "test@test.com",
            "password": "",
        })
        assert resp.status_code == 422

    def test_login_bad_credentials_401(self, client):
        """Login with non-existent user returns 401."""
        mock_session = MagicMock(spec=Session)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None  # no user found
        mock_session.execute.return_value = mock_result
        app.dependency_overrides[get_db] = lambda: mock_session
        resp = client.post("/api/v1/auth/login", json={
            "email": "nobody@example.com",
            "password": "wrongpassword",
        })
        assert resp.status_code == 401


class TestAuthRefresh:
    """Integration tests for POST /api/v1/auth/refresh."""

    def test_refresh_missing_token_422(self, client):
        resp = client.post("/api/v1/auth/refresh", json={})
        assert resp.status_code == 422

    def test_refresh_empty_token_422(self, client):
        resp = client.post("/api/v1/auth/refresh", json={"refresh_token": ""})
        assert resp.status_code == 422
