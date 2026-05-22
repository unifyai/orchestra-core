"""End-to-end CRUD smoke against the kernel router surface.

Mirrors the round-trip the unify SDK performs in production: create
project, create context, create logs, fetch logs, list contexts, etc.
This is the canonical "did anything in the kernel break" test.
"""


def test_full_round_trip(client):
    # 1. Create a project.
    r = client.post(
        "/v0/project",
        json={"name": "smoke", "description": "kernel-test"},
    )
    assert r.status_code == 201, r.text
    proj = r.json()
    assert proj["name"] == "smoke"
    assert proj["description"] == "kernel-test"
    assert proj["icon"] == "folder"

    # 2. List projects -> should see the new one.
    r = client.get("/v0/projects")
    assert r.status_code == 200
    names = [p["name"] for p in r.json()["projects"]]
    assert "smoke" in names

    # 3. Get project by name.
    r = client.get("/v0/project/smoke")
    assert r.status_code == 200
    assert r.json()["name"] == "smoke"

    # 4. Create a context under the project.
    r = client.post(
        "/v0/project/smoke/contexts",
        json={"name": "events", "is_versioned": False},
    )
    assert r.status_code == 201, r.text
    ctx = r.json()
    assert ctx["name"] == "events"
    assert ctx["is_versioned"] is False

    # 5. List contexts.
    r = client.get("/v0/project/smoke/contexts")
    assert r.status_code == 200
    ctx_names = [c["name"] for c in r.json()["contexts"]]
    assert "events" in ctx_names

    # 6. Create logs scoped to the context.
    r = client.post(
        "/v0/logs",
        json={
            "project": "smoke",
            "context": "events",
            "rows": [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
        },
    )
    assert r.status_code == 201, r.text
    ids = r.json()["log_event_ids"]
    assert len(ids) == 2

    # 7. Fetch logs by project + context filter.
    r = client.get("/v0/logs", params={"project": "smoke", "context": "events"})
    assert r.status_code == 200, r.text
    logs = r.json()["logs"]
    assert len(logs) == 2
    assert {log["data"]["k"] for log in logs} == {"a", "b"}
    assert {log["data"]["v"] for log in logs} == {1, 2}
    assert all("events" in log["contexts"] for log in logs)

    # 8. Delete the project (cascades context + logs).
    r = client.delete("/v0/project/smoke")
    assert r.status_code == 204, r.text

    # 9. Verify it's gone.
    r = client.get("/v0/project/smoke")
    assert r.status_code == 404


def test_get_missing_project_404(client):
    """Reading a project that does not exist returns 404."""
    r = client.get("/v0/project/does-not-exist")
    assert r.status_code == 404


def test_create_field_type(client):
    # Need a project + context to scope the field.
    client.post("/v0/project", json={"name": "fields-proj"})
    client.post("/v0/project/fields-proj/contexts", json={"name": "ctx"})

    r = client.post(
        "/v0/logs/fields",
        json={
            "project": "fields-proj",
            "context": "ctx",
            "field_name": "score",
            "field_type": "float",
            "field_category": "entry",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["field_name"] == "score"
    assert body["field_type"] == "float"

    r = client.get(
        "/v0/logs/fields", params={"project": "fields-proj", "context": "ctx"}
    )
    assert r.status_code == 200
    names = [f["field_name"] for f in r.json()["fields"]]
    assert "score" in names
