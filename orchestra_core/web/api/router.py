"""Top-level kernel router. Mounts the four CRUD modules and the SDK stubs."""

from fastapi import APIRouter, Depends

from orchestra_core.web.api.context import router as context_router
from orchestra_core.web.api.dependencies import auth_api_key, check_account_not_frozen
from orchestra_core.web.api.local_stubs import router as local_stubs_router
from orchestra_core.web.api.log import router as log_router
from orchestra_core.web.api.project import router as project_router
from orchestra_core.web.api.storage import router as storage_router

API_KEY_AUTH = [
    Depends(auth_api_key),
    Depends(check_account_not_frozen),
]

api_router = APIRouter()

api_router.include_router(project_router, tags=["Projects"], dependencies=API_KEY_AUTH)
api_router.include_router(context_router, tags=["Contexts"], dependencies=API_KEY_AUTH)
api_router.include_router(log_router, tags=["Logs"], dependencies=API_KEY_AUTH)
api_router.include_router(
    storage_router, tags=["Storage"], dependencies=API_KEY_AUTH,
)

# unify-SDK compatibility stubs (no auth — these are read-only/no-op
# sentinels that exist purely so the SDK doesn't have to branch).
api_router.include_router(local_stubs_router, include_in_schema=False)


@api_router.get("/health", include_in_schema=False)
def health_check() -> dict:
    return {"status": "ok"}
