from starlette.testclient import TestClient

from impact.api.app import app


def test_health_returns_200() -> None:
    client = TestClient(app)
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["service"] == "devrank-impact"
