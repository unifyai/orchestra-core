"""Kernel FastAPI application factory.

Provides:
- `get_app()` — full kernel app, used when running orchestra-core standalone.
- `core_middlewares(app)` — reusable middleware setup that orchestra-platform
  layers its own additional middleware on top of.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import UJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from orchestra_core.observability.prometheus_middleware import (
    PrometheusMiddleware,
    metrics,
)
from orchestra_core.observability.request_trace_middleware import (
    RequestTraceMiddleware,
)
from orchestra_core.settings import settings
from orchestra_core.web.api.router import api_router
from orchestra_core.web.lifetime import (
    register_shutdown_event,
    register_startup_event,
)

logger = logging.getLogger(__name__)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'"
        )
        return response


def core_middlewares(app: FastAPI) -> None:
    """Attach kernel-level middleware stack to the given app.

    orchestra-platform calls this from its own `get_app()` and then layers
    on its own middleware (rate limiting, staging gates, etc.).
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "Accept",
            "Origin",
            "X-Requested-With",
        ],
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(PrometheusMiddleware, app_name="orchestra")
    app.add_middleware(RequestTraceMiddleware)


def get_app() -> FastAPI:
    """Build the standalone orchestra-core FastAPI app."""
    app = FastAPI(
        title="orchestra-core",
        version="0.1.0",
        docs_url="/v0/docs",
        redoc_url=None,
        openapi_url="/v0/openapi.json",
        swagger_ui_parameters={"defaultModelsExpandDepth": -1},
        default_response_class=UJSONResponse,
    )

    core_middlewares(app)

    register_startup_event(app)
    register_shutdown_event(app)

    app.include_router(router=api_router, prefix="/v0")
    app.add_api_route("/metrics", metrics)

    return app
