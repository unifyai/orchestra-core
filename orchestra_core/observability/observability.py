from contextvars import ContextVar
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.trace.span import format_trace_id
from prometheus_client import Histogram

# Consolidated context variables to store user and request information
user_id_ctx: ContextVar[Optional[str]] = ContextVar("user_id", default=None)
user_email_ctx: ContextVar[Optional[str]] = ContextVar("user_email", default=None)
request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
first_name_ctx: ContextVar[Optional[str]] = ContextVar("first_name", default=None)
last_name_ctx: ContextVar[Optional[str]] = ContextVar("last_name", default=None)

# Prometheus metrics
DB_QUERY_DURATION = Histogram(
    "orchestra_db_query_duration_seconds",
    "Duration of database queries in seconds",
    ["query_type", "table"],
    buckets=(
        0.001,
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
        7.5,
        10.0,
    ),
)

# Table-specific metrics for better analysis
TABLE_QUERY_DURATION = Histogram(
    "orchestra_table_query_duration_seconds",
    "Duration of database queries by table in seconds",
    ["table", "query_type", "query_fingerprint"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0),
)


def set_user_context(
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
):
    """
    Set user information in the current execution context.
    This context will be available to all code in the current async task.

    Args:
        user_id: User's unique identifier
        user_email: User's email address
    """
    if user_id is not None:
        user_id_ctx.set(user_id)
    if user_email is not None:
        user_email_ctx.set(user_email)
    if first_name is not None:
        first_name_ctx.set(first_name)
    if last_name is not None:
        last_name_ctx.set(last_name)


def get_user_id() -> Optional[str]:
    """Get the current user's ID from the execution context."""
    return user_id_ctx.get()


def get_user_email() -> Optional[str]:
    """Get the current user's email from the execution context."""
    return user_email_ctx.get()


def get_first_name() -> Optional[str]:
    """Get the current user's first name from the execution context."""
    return first_name_ctx.get()


def get_last_name() -> Optional[str]:
    """Get the current user's last name from the execution context."""
    return last_name_ctx.get()


def set_request_id(request_id: Optional[str] = None):
    """Set the request ID in the current execution context."""
    if request_id is not None:
        request_id_ctx.set(request_id)


def get_request_id() -> Optional[str]:
    """Get the current request ID from the execution context."""
    return request_id_ctx.get()


def clear_user_context():
    """Clear user information from the current execution context."""
    user_id_ctx.set(None)
    user_email_ctx.set(None)
    first_name_ctx.set(None)
    last_name_ctx.set(None)
    request_id_ctx.set(None)


def record_db_query_duration(
    query_type: str,
    table: str,
    duration: float,
    query_fingerprint: str = None,
):
    """
    Record database query duration in metrics.
    This function is called by the database event hooks.

    Args:
        query_type: Type of query (select, insert, update, delete)
        table: Database table name
        duration: Query execution time in seconds
        query_fingerprint: Optional fingerprint of the query for grouping similar queries
    """
    exemplar_data: Dict[str, Any] = {}

    current_span = trace.get_current_span()
    if current_span and hasattr(current_span, "get_span_context"):
        span_context = current_span.get_span_context()
        if span_context and hasattr(span_context, "trace_id"):
            trace_id = format_trace_id(span_context.trace_id)
            exemplar_data["traceID"] = trace_id

    user_id = get_user_id()
    if user_id:
        exemplar_data["userID"] = user_id

    request_id = get_request_id()
    if request_id:
        exemplar_data["reqID"] = request_id

    if exemplar_data:
        DB_QUERY_DURATION.labels(query_type=query_type, table=table).observe(
            duration,
            exemplar_data,
        )
    else:
        DB_QUERY_DURATION.labels(query_type=query_type, table=table).observe(duration)

    if query_fingerprint:
        if exemplar_data:
            TABLE_QUERY_DURATION.labels(
                table=table,
                query_type=query_type,
                query_fingerprint=query_fingerprint,
            ).observe(duration, exemplar_data)
        else:
            TABLE_QUERY_DURATION.labels(
                table=table,
                query_type=query_type,
                query_fingerprint=query_fingerprint,
            ).observe(duration)


def get_current_context():
    """Get the current user and request context as a dictionary."""
    return {
        "user_id": get_user_id(),
        "user_email": get_user_email(),
        "request_id": get_request_id(),
    }


def set_context_from_dict(context_dict):
    """Set context variables from a dictionary."""
    if context_dict.get("user_id") is not None:
        user_id_ctx.set(context_dict["user_id"])
    if context_dict.get("user_email") is not None:
        user_email_ctx.set(context_dict["user_email"])
    if context_dict.get("request_id") is not None:
        request_id_ctx.set(context_dict["request_id"])
