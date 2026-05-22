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
