import os
import re
import secrets
import time
import uuid
from typing import Tuple

from fastapi import HTTPException
from opentelemetry import trace
from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.openmetrics.exposition import (
    CONTENT_TYPE_LATEST,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_500_INTERNAL_SERVER_ERROR
from starlette.types import ASGIApp

from orchestra_core.observability.inactivity_shutdown import record_activity
from orchestra_core.observability.observability import clear_user_context, set_request_id

INFO = Gauge(
    "orchestra_app_info",
    "Orchestra application information.",
    ["app_name"],
)

REQUESTS = Counter(
    "orchestra_requests_total",
    "Total count of requests by method and path.",
    ["method", "path", "app_name"],
)

RESPONSES = Counter(
    "orchestra_responses_total",
    "Total count of responses by method, path and status codes.",
    ["method", "path", "status_code", "app_name"],
)

REQUESTS_PROCESSING_TIME = Histogram(
    "orchestra_requests_duration_seconds",
    "Histogram of requests processing time by path and user (in seconds)",
    ["method", "path", "app_name", "user_id"],
    buckets=(
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        5.0,
        10.0,
    ),
)

EXCEPTIONS = Counter(
    "orchestra_exceptions_total",
    "Total count of exceptions raised by path and exception type",
    ["method", "path", "exception_type", "app_name"],
)

REQUESTS_IN_PROGRESS = Gauge(
    "orchestra_requests_in_progress",
    "Gauge of requests by method and path currently being processed",
    ["method", "path", "app_name"],
)

REQUESTS_WITH_USER = Counter(
    "orchestra_user_requests_total",
    "Total count of requests by user, method and path.",
    ["user_id", "method", "path", "app_name", "request_id"],
)

# Billing metrics
INVOICE_CREATED_TOTAL = Counter(
    "invoice_created_total",
    "Stripe invoices created by the monthly invoicer",
    ["entity_type", "entity_id"],  # entity_type: "user" or "organization"
)

INVOICE_PAID_TOTAL = Counter(
    "invoice_paid_total",
    "Invoices reported PAID by Stripe webhook",
    ["billing_account_id"],
)

INVOICE_FAILED_TOTAL = Counter(
    "invoice_failed_total",
    "Invoices reported FAILED / ACTION_REQUIRED by Stripe webhook",
    ["billing_account_id"],
)

BILLING_SUSPENDED_TOTAL = Counter(
    "billing_suspended_total",
    "Billing accounts suspended by the daily billing-guard",
    ["billing_account_id"],
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware that collects and exposes Prometheus-style metrics about
    incoming requests to the Orchestra (FastAPI) application.
    """

    def __init__(self, app: ASGIApp, app_name: str = "orchestra") -> None:
        super().__init__(app)
        self.app_name = app_name
        # This sets a one-time gauge indicating the app is running
        INFO.labels(app_name=self.app_name).inc()

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        method = request.method
        path, is_handled_path = self.get_path(request)

        if not is_handled_path:
            # If it's not matched by a known route, skip metrics
            return await call_next(request)

        # Record API activity for inactivity timeout (exclude internal endpoints)
        if not path.endswith(
            ("/metrics", "/health", "/docs", "/redoc", "/openapi.json"),
        ):
            record_activity()

        # Pre-request metrics
        REQUESTS_IN_PROGRESS.labels(
            method=method,
            path=path,
            app_name=self.app_name,
        ).inc()

        REQUESTS.labels(
            method=method,
            path=path,
            app_name=self.app_name,
        ).inc()

        before_time = time.perf_counter()
        request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
        set_request_id(request_id)
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            status_code = HTTP_500_INTERNAL_SERVER_ERROR
            EXCEPTIONS.labels(
                method=method,
                path=path,
                exception_type=type(e).__name__,
                app_name=self.app_name,
            ).inc()
            raise e
        finally:
            after_time = time.perf_counter()

            user_id = getattr(request.state, "user_id", None)
            if user_id:
                REQUESTS_WITH_USER.labels(
                    user_id=user_id,
                    method=method,
                    path=path,
                    app_name=self.app_name,
                    request_id=request_id or "unknown",
                ).inc()

            # Retrieve trace id (if using OpenTelemetry)
            span = trace.get_current_span()
            raw_trace_id = span.get_span_context().trace_id
            trace_id = f"{raw_trace_id:032x}"

            # Create exemplar with limited data to stay under 128 character limit
            exemplar_data = {
                "traceID": trace_id,
                "userID": str(user_id) if user_id else "anon",
                "reqID": request_id,
            }
            # Record request duration with user_id if available
            REQUESTS_PROCESSING_TIME.labels(
                method=method,
                path=path,
                app_name=self.app_name,
                user_id=user_id,
            ).observe(
                after_time - before_time,
                exemplar=exemplar_data,
            )

            RESPONSES.labels(
                method=method,
                path=path,
                status_code=status_code,
                app_name=self.app_name,
            ).inc()

            REQUESTS_IN_PROGRESS.labels(
                method=method,
                path=path,
                app_name=self.app_name,
            ).dec()

            # Clear user context after request is processed
            clear_user_context()

        return response

    @staticmethod
    def get_path(request: Request) -> Tuple[str, bool]:
        """
        Try to match the request with one of the FastAPI routes.
        If it matches, return (route_path, True).
        Otherwise, return (request.url.path, False).
        """
        for route in request.app.routes:
            match, _child_scope = route.matches(request.scope)
            if match == Match.FULL:
                return route.path, True

        return request.url.path, False

    def _extract_query_pattern(self, path: str) -> str:
        """Convert paths with IDs to patterns for better grouping"""
        return re.sub(r"/\d+", "/{id}", path)


def metrics(request: Request) -> Response:
    """
    Endpoint that returns the aggregated Prometheus metrics.
    Protected by Bearer-token authentication.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    incoming_token = auth_header[len("Bearer ") :]

    expected_token = os.getenv("PROMETHEUS_METRICS_TOKEN")
    if not expected_token or not secrets.compare_digest(
        incoming_token,
        expected_token,
    ):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )

    return Response(
        generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )
