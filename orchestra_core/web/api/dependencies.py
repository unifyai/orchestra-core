"""Stub auth + DB session for orchestra-core.

Single-tenant: a single API key (env var `ORCHESTRA_API_KEY`) is compared
against the bearer token. No User table, no Organization, no DB lookup. The
sentinel user_id `1` is set on `request.state` so kernel DAOs that scope by
`user_id` continue to work.
"""

from __future__ import annotations

import logging
import os
import secrets

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from orchestra_core.db.dependencies import get_db_session  # noqa: F401  (re-export)
from orchestra_core.web.api.utils.http_responses import invalid_api_key

logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)

DEFAULT_LOCAL_USER_ID = "1"


def auth_api_key(
    request_fastapi: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> None:
    """Compare the bearer token against `ORCHESTRA_API_KEY`.

    On success, populate `request.state` with the sentinel single-tenant
    identity. On failure, raise 401.
    """
    expected = os.environ.get("ORCHESTRA_API_KEY", "")
    if not expected:
        # Open-mode (no key configured) is allowed for local dev to remove
        # all friction, but logged so it's visible in the trace.
        logger.warning(
            "ORCHESTRA_API_KEY is not set; orchestra-core is running open. "
            "Set ORCHESTRA_API_KEY to require bearer authentication.",
        )
        request_fastapi.state.user_id = DEFAULT_LOCAL_USER_ID
        request_fastapi.state.organization_id = None
        request_fastapi.state.api_key = None
        return

    if credentials is None:
        raise invalid_api_key
    if not secrets.compare_digest(credentials.credentials, expected):
        raise invalid_api_key

    request_fastapi.state.user_id = DEFAULT_LOCAL_USER_ID
    request_fastapi.state.organization_id = None
    request_fastapi.state.api_key = credentials.credentials


def check_account_not_frozen(request: Request) -> None:
    """No-op: orchestra-core has no billing concept.

    Kept as a function so router definitions that mirror the platform's
    `[Depends(auth_api_key), Depends(check_account_not_frozen)]` shape stay
    interchangeable.
    """
    return None
