# orchestra-core

The persistence kernel of the Unify stack: a single-tenant FastAPI + Postgres
backend providing **projects**, **contexts**, **logs**, and **object storage**
for local agent runtimes.

orchestra-core is the open-source half of the orchestra runtime. The hosted
multi-tenant features — user accounts, organizations, billing, voice
integrations, Console UI dashboards — live in a separate private package
(`orchestra-platform`) that depends on orchestra-core.

## Position in the stack

```
unity (agent runtime)
   │
   ▼
unify (Python SDK)
   │
   ▼
orchestra-core  ◄── this repo, fully local single-user persistence
```

For the multi-tenant hosted product, orchestra-platform layers on top:

```
orchestra-platform (private)
   │
   ▼
orchestra-core  ◄── shared kernel
```

## Quick start

```bash
# Bring up Postgres + run migrations + start uvicorn on :8000
ORCHESTRA_API_KEY=local-dev-key bash scripts/local.sh start
```

Then point unity at it:

```bash
ORCHESTRA_URL=http://127.0.0.1:8000/v0 \
UNIFY_KEY=local-dev-key \
unity --project_name Sandbox
```

## Authentication

Single API key compared against the `ORCHESTRA_API_KEY` environment variable.
No user accounts, no organizations, no DB rows for auth.

## What belongs in orchestra-core (vs orchestra-platform)

The dependency between the two repos is **one-way**: orchestra-platform imports
orchestra-core, never the other direction. orchestra-platform's CI runs
`scripts/check_core_purity.sh` on every PR to enforce this.

Use this litmus test when deciding where a change belongs: *"Could this run in a
fully local, single-user, no-billing context?"*

| Change you're making | Repo |
|---|---|
| Kernel tables (`project`, `context`, `log_event`, `field_type`, `embedding`, `embedding_queue`, ...) | this repo |
| DAOs / endpoints that operate on kernel tables without account or tenant context | this repo |
| OpenTelemetry primitives, prometheus middleware, file/filtering exporters | this repo |
| Local-filesystem bucket service | this repo |
| Tenant-aware tables (`user`, `organization`, `billing_*`, `assistants`, `api_key`, `voices`, etc.) | orchestra-platform |
| Stripe / Twilio / ElevenLabs / Cartesia / Deepgram / Vertex AI / OAuth integrations | orchestra-platform |
| Multi-tenant access control (anything reading `user_id` or `organization_id` for authz) | orchestra-platform |
| Console-facing UI state (`interface`, `tile`, `tab`, `dashboard_token`) | orchestra-platform |

## Releases & versioning

orchestra-core ships as a **tagged Git URL dependency**, not a PyPI package.
orchestra-platform pins a specific tag in its `pyproject.toml`. Cut a new
release when:

- A kernel table changes (column, index, constraint)
- A kernel DAO/router gets a new method or changed signature
- A kernel migration is added
- The shared `MetaData` contract changes

```bash
# After your PR merges to main
git checkout main && git pull
git tag -a vX.Y.Z -m "vX.Y.Z — short summary"
git push origin vX.Y.Z
```

Then bump `orchestra-platform`'s `pyproject.toml` to the new tag and regenerate
`poetry.lock`.

## API surface

- `/v0/projects`, `/v0/project/{name}`, `/v0/project/{name}/commits`, `/v0/project/{name}/commit`, `/v0/project/{name}/rollback`
- `/v0/project/{p}/contexts/...`
- `/v0/logs`, `/v0/logs/fields`, `/v0/logs/derived`, `/v0/logs/join`, `/v0/logs/join_query`, `/v0/logs/groups`, `/v0/logs/rename_field`, `/v0/logs/metric/{m}`, `/v0/logs/{id}/fields/{key}/atomic`
- `/v0/storage/...` (signed URLs over local filesystem)
- `/v0/health`

Plus stubbed account endpoints (`/v0/user/basic-info`, `/v0/credits/deduct`,
etc.) so the unify SDK doesn't need to branch between hosted and local modes.

## License

MIT. See [LICENSE](LICENSE).
