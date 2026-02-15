from starlette.testclient import TestClient

from api.app import app


def test_health_returns_200() -> None:
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["service"] == "devrank"
