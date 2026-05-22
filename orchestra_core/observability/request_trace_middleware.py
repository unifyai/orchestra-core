"""
Middleware to capture HTTP request details for OpenTelemetry tracing.

Creates a synthetic "http.request_received" span that completes immediately,
carrying all request parameters. This ensures request details are available
in trace files even while the request is still in-flight.

Uses pure ASGI middleware pattern (not BaseHTTPMiddleware) to properly
handle request body caching, avoiding known issues with body consumption.
"""

import json
import logging
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import SpanKind
from starlette.requests import Request
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Maximum size of request body to capture (to avoid memory issues with large uploads)
MAX_BODY_SIZE = 64 * 1024  # 64KB
# Maximum size for individual attribute values (OTel has limits)
MAX_ATTR_VALUE_SIZE = 8 * 1024  # 8KB

# Tracer for creating the synthetic request span
_tracer = trace.get_tracer(__name__)


def _truncate(value: str, max_len: int = MAX_ATTR_VALUE_SIZE) -> str:
    """Truncate string if too long, adding indicator."""
    if len(value) <= max_len:
        return value
    return value[: max_len - 20] + f"... [truncated, {len(value)} total]"


def _safe_json_dumps(obj: Any) -> str:
    """Safely serialize object to JSON string."""
    try:
        return json.dumps(obj, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(obj)


class RequestTraceMiddleware:
    """
    Pure ASGI middleware that creates a synthetic span with request details.

    Creates an "http.request_received" span that completes immediately,
    ensuring request parameters are available in trace files even while
    the main request is still processing. This enables in-flight debugging
    of long-running requests.

    The span captures:
    - http.request.method: HTTP method
    - http.request.path: Request path
    - http.request.query_params: Query string parameters as JSON
    - http.request.path_params: Path parameters as JSON
    - http.request.body: Request body (for JSON content types, truncated if large)
    - http.request.headers: Selected headers (content-type, accept, user-agent)

    Uses pure ASGI pattern with body caching to avoid BaseHTTPMiddleware issues.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Check content-type to decide if we need to read body
        headers = dict(scope.get("headers", []))
        content_type = headers.get(b"content-type", b"").decode(
            "utf-8",
            errors="ignore",
        )
        should_capture_body = "application/json" in content_type

        # Check content-length to avoid reading huge bodies
        content_length_header = headers.get(b"content-length", b"").decode()
        body_too_large = False
        if content_length_header:
            try:
                if int(content_length_header) > MAX_BODY_SIZE:
                    body_too_large = True
            except ValueError:
                pass

        if should_capture_body and not body_too_large:
            # Read and cache the body, then create a new receive that replays it
            body_bytes, receive = await self._cache_request_body(receive)
        else:
            body_bytes = None
            if body_too_large:
                body_bytes = f"[body too large: {content_length_header} bytes]".encode()

        # Create the tracing span (synchronously captures all info)
        try:
            await self._create_request_received_span(scope, body_bytes)
        except Exception as e:
            # Never fail the request due to tracing issues
            logger.debug(f"Failed to create request_received span: {e}")

        # Call the next middleware/app with the (possibly modified) receive
        await self.app(scope, receive, send)

    async def _cache_request_body(
        self,
        receive: Receive,
    ) -> tuple[bytes | None, Receive]:
        """
        Read and cache the request body, returning a new receive callable.

        This pattern allows the middleware to inspect the body while still
        making it available to the rest of the application.
        """
        body_chunks: list[bytes] = []
        total_size = 0

        # Read all body chunks
        while True:
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                if chunk:
                    total_size += len(chunk)
                    # Stop accumulating if body is too large
                    if total_size <= MAX_BODY_SIZE:
                        body_chunks.append(chunk)

                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                # Client disconnected
                break

        # Combine all chunks
        if total_size > MAX_BODY_SIZE:
            body_bytes = f"[body too large: {total_size} bytes]".encode()
        else:
            body_bytes = b"".join(body_chunks) if body_chunks else None

        # Create a new receive that replays the cached body
        body_sent = False

        async def cached_receive() -> Message:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {
                    "type": "http.request",
                    "body": b"".join(body_chunks) if body_chunks else b"",
                    "more_body": False,
                }
            # After body is sent, wait for original receive (handles disconnect)
            return await receive()

        return body_bytes, cached_receive

    async def _create_request_received_span(
        self,
        scope: Scope,
        body_bytes: bytes | None,
    ) -> None:
        """Create a synthetic span that completes immediately with request details."""
        method = scope.get("method", "UNKNOWN")
        path = scope.get("path", "/")

        # Get route info for the span name
        route, path_params = self._get_route_info(scope)
        route_display = route or path

        # Create a span that starts and ends immediately
        # This exports right away, making request params visible in in-progress traces
        with _tracer.start_as_current_span(
            f"http.request_received {method} {route_display}",
            kind=SpanKind.INTERNAL,
        ) as span:
            # Basic request info
            span.set_attribute("http.request.method", method)
            span.set_attribute("http.request.path", path)
            if route:
                span.set_attribute("http.request.route", route)

            # Query parameters
            query_string = scope.get("query_string", b"").decode(
                "utf-8",
                errors="ignore",
            )
            if query_string:
                # Parse query string into dict
                from urllib.parse import parse_qs

                query_dict = parse_qs(query_string)
                # Flatten single-value lists
                query_dict = {
                    k: v[0] if len(v) == 1 else v for k, v in query_dict.items()
                }
                span.set_attribute(
                    "http.request.query_params",
                    _truncate(_safe_json_dumps(query_dict)),
                )

            # Path parameters
            if path_params:
                span.set_attribute(
                    "http.request.path_params",
                    _truncate(_safe_json_dumps(path_params)),
                )

            # Selected headers
            headers_to_capture = {
                b"content-type",
                b"accept",
                b"user-agent",
                b"x-request-id",
            }
            captured_headers = {}
            for key, value in scope.get("headers", []):
                if key.lower() in headers_to_capture:
                    captured_headers[key.decode("utf-8", errors="ignore")] = (
                        value.decode(
                            "utf-8",
                            errors="ignore",
                        )
                    )
            if captured_headers:
                span.set_attribute(
                    "http.request.headers",
                    _safe_json_dumps(captured_headers),
                )

            # Request body (if captured)
            if body_bytes:
                body_str = self._format_body(body_bytes)
                if body_str:
                    span.set_attribute("http.request.body", _truncate(body_str))

            # Span ends here and exports immediately!

    def _format_body(self, body_bytes: bytes) -> str | None:
        """Format body bytes as a string, pretty-printing JSON if possible."""
        if not body_bytes:
            return None

        try:
            body_str = body_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return "[binary body]"

        # Check if it's our "too large" marker
        if body_str.startswith("[body too large:"):
            return body_str

        # Try to pretty-print JSON
        try:
            parsed = json.loads(body_str)
            return json.dumps(parsed, indent=2, default=str, ensure_ascii=False)
        except json.JSONDecodeError:
            return body_str

    def _get_route_info(self, scope: Scope) -> tuple[str | None, dict]:
        """Extract route pattern and path parameters from the matched route."""
        app = scope.get("app")
        if not app:
            return None, {}

        # Create a minimal request object for route matching
        try:
            request = Request(scope)
            for route in app.routes:
                match, child_scope = route.matches(scope)
                if match == Match.FULL:
                    return route.path, child_scope.get("path_params", {})
        except Exception:
            pass

        return None, {}
