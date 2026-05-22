"""Smoke tests for the kernel app's auth gate and health endpoint."""


def test_health_is_open(unauth_client):
    """Health endpoint is unauthenticated and returns 200."""
    r = unauth_client.get("/v0/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_projects_requires_auth(unauth_client):
    """Routes under /v0 are 401 without a bearer token."""
    r = unauth_client.get("/v0/projects")
    assert r.status_code == 401


def test_projects_rejects_wrong_key(app):
    """Wrong bearer token returns 401."""
    from fastapi.testclient import TestClient

    with TestClient(
        app,
        headers={"Authorization": "Bearer wrong-key"},
    ) as c:
        r = c.get("/v0/projects")
        assert r.status_code == 401


def test_projects_accepts_valid_key(client):
    """Valid bearer token returns 200 with empty list initially."""
    r = client.get("/v0/projects")
    assert r.status_code == 200
    assert r.json() == {"projects": []}


def test_local_stub_user_basic_info(client):
    """The unify-SDK compat stub for /v0/user/basic-info returns the local sentinel."""
    r = client.get("/v0/user/basic-info")
    assert r.status_code == 200
    body = r.json()
    assert body["user_id"] == "1"
    assert body["organization_id"] is None


def test_local_stub_credits_deduct(client):
    """Credits-deduct stub always reports plenty remaining."""
    r = client.post("/v0/credits/deduct", json={"amount": 1.0})
    assert r.status_code == 200
    body = r.json()
    assert body["credits_remaining"] > 0
    assert body["credits_deducted"] == 0.0


def test_local_stub_assistants(client):
    """Assistants stub returns an empty list."""
    r = client.get("/v0/assistant")
    assert r.status_code == 200
    assert r.json() == []
