import datetime
import hashlib
import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, Generator

from opentelemetry import trace
from opentelemetry.trace import SpanKind
from sqlalchemy import event
from sqlalchemy.orm import Session
from starlette.requests import Request

from orchestra_core.observability.observability import (
    get_first_name,
    get_last_name,
    get_request_id,
    get_user_email,
    get_user_id,
    record_db_query_duration,
    set_request_id,
    set_user_context,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("orchestra.db")

query_start_times: Dict[int, float] = {}
active_transactions: Dict[int, Any] = {}

QUERY_START_TIMES_TTL = 60


def cleanup_query_start_times():
    """Remove stale entries from query_start_times to prevent memory leaks."""
    current_time = time.time()
    to_remove = []

    for conn_id, start_time in query_start_times.items():
        if current_time - start_time > QUERY_START_TIMES_TTL:
            to_remove.append(conn_id)

    for conn_id in to_remove:
        query_start_times.pop(conn_id, None)


query_patterns = defaultdict(int)


def convert_datetimes(obj):
    """Convert datetime objects to ISO 8601 strings."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetimes(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_datetimes(item) for item in obj]
    else:
        return obj


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Event hook that fires before SQL execution.

    Captures the start time of each query and creates an OpenTelemetry span
    with user context for tracing.
    """
    if hasattr(conn, "info") and "request_state" in conn.info:
        request_state = conn.info["request_state"]
        if hasattr(request_state, "user_id"):
            set_user_context(
                user_id=request_state.user_id,
                user_email=getattr(request_state, "user_email", None),
                first_name=getattr(request_state, "first_name", None),
                last_name=getattr(request_state, "last_name", None),
            )
        if hasattr(request_state, "request_id"):
            set_request_id(request_state.request_id)

        if not hasattr(request_state, "sql_trace"):
            request_state.sql_trace = []

    if len(query_start_times) > 100:
        cleanup_query_start_times()

    conn_id = id(conn)
    query_start_times[conn_id] = time.time()

    query_type = "unknown"
    match = re.match(
        r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|BEGIN|COMMIT)",
        statement.upper(),
    )
    if match:
        query_type = match.group(1).lower()

    if query_type == "begin":
        active_transactions[conn_id] = {
            "start_time": time.time(),
            "queries": [],
            "span": tracer.start_span(
                name="db.transaction",
                kind=SpanKind.CLIENT,
            ),
        }

    user_id = get_user_id() or "anonymous"
    user_email = get_user_email()

    request_id = get_request_id()

    table = "unknown"
    if query_type in ["select", "insert", "update", "delete"]:
        table_match = re.search(
            r"(?:FROM|INTO|UPDATE)\s+([a-zA-Z0-9_\.]+)",
            statement.upper(),
        )
        if table_match:
            table = table_match.group(1).lower()

    span = tracer.start_span(
        name=f"db.query.{query_type}.{table}",
        kind=SpanKind.CLIENT,
    )

    span.set_attribute("db.system", "postgresql")
    truncated_statement = (
        statement[:4000] + "..." if len(statement) > 4000 else statement
    )
    span.set_attribute("db.statement", truncated_statement)

    if parameters:
        try:
            params_str = str(parameters)
            if len(params_str) > 1024:
                params_str = params_str[:1024] + "..."
            span.set_attribute("db.parameters", params_str)
        except Exception:
            span.set_attribute("db.parameters", "[unserializable]")

    span.set_attribute("db.query_type", query_type)
    span.set_attribute("db.table", table)
    span.set_attribute("db.user_id", user_id)
    span.set_attribute("user.id", user_id)
    if user_email:
        span.set_attribute("db.user_email", user_email)
        span.set_attribute("user.email", user_email)
    if request_id:
        span.set_attribute("request.id", request_id)

    query_fingerprint = _fingerprint_query(statement)
    span.set_attribute("db.query_fingerprint", query_fingerprint)
    context._query_fingerprint = query_fingerprint

    context._otel_span = span

    if conn_id in active_transactions:
        active_transactions[conn_id]["queries"].append(
            {
                "query_type": query_type,
                "table": table,
                "start_time": time.time(),
                "fingerprint": query_fingerprint,
                "statement": statement,
            },
        )

    if hasattr(conn, "info") and "request_state" in conn.info:
        request_state = conn.info["request_state"]
        if hasattr(request_state, "sql_trace"):
            trace_entry = {
                "query": statement,
                "parameters": convert_datetimes(parameters),
                "query_type": query_type,
                "table": table,
                "start_time": time.time(),
                "query_fingerprint": query_fingerprint,
            }
            context._sql_trace_entry = trace_entry


def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Event hook that fires after SQL execution.

    Calculates query duration, records metrics, and logs query details. For
    slow queries (>100ms), logs the complete query text and parameters.
    """

    conn_id = id(conn)
    start_time = query_start_times.pop(conn_id, None)

    if start_time:
        duration = time.time() - start_time

        query_type = "unknown"
        match = re.match(
            r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|BEGIN|COMMIT)",
            statement.upper(),
        )
        if match:
            query_type = match.group(1).lower()

        table = "unknown"
        if query_type in ["select", "insert", "update", "delete"]:
            table_match = re.search(
                r"(?:FROM|INTO|UPDATE)\s+([a-zA-Z0-9_\.]+)",
                statement.upper(),
            )
            if table_match:
                table = table_match.group(1).lower()

        query_fingerprint = getattr(
            context,
            "_query_fingerprint",
            _fingerprint_query(statement),
        )

        user_id = get_user_id() or "anonymous"
        user_email = get_user_email()
        request_id = get_request_id()
        first_name = get_first_name()
        last_name = get_last_name()

        record_db_query_duration(
            query_type=query_type,
            table=table,
            duration=duration,
            query_fingerprint=query_fingerprint,
        )

        current_span = trace.get_current_span()
        raw_trace_id = current_span.get_span_context().trace_id
        raw_span_id = current_span.get_span_context().span_id
        trace_id = f"{raw_trace_id:032x}"
        span_id = f"{raw_span_id:016x}"
        is_slow_query = duration > 0.1
        log_level = logging.WARNING if is_slow_query else logging.DEBUG

        log_extras = {
            "query_type": query_type,
            "table": table,
            "duration": duration,
            "duration_ms": duration * 1000,
            "user_id": user_id,
            "traceID": trace_id,
            "spanID": span_id,
            "query_fingerprint": query_fingerprint,
        }

        if user_email:
            log_extras["user_email"] = user_email

        if request_id:
            log_extras["request_id"] = request_id

        if is_slow_query:
            log_extras["query_text"] = statement
            log_extras["query_params"] = str(parameters)

        if hasattr(context, "_otel_span"):
            span = context._otel_span
            if span.is_recording():
                span.set_attribute("db.duration_ms", duration * 1000)

                if is_slow_query:
                    span.set_attribute("db.slow_query", True)

                if hasattr(cursor, "rowcount"):
                    span.set_attribute("db.rows_affected", cursor.rowcount)

                span.end()

        if (
            hasattr(conn, "info")
            and "request_state" in conn.info
            and hasattr(context, "_sql_trace_entry")
        ):
            request_state = conn.info["request_state"]
            if hasattr(request_state, "sql_trace"):
                trace_entry = context._sql_trace_entry
                trace_entry["duration"] = duration
                trace_entry["duration_ms"] = duration * 1000
                if hasattr(cursor, "rowcount"):
                    trace_entry["rows_affected"] = cursor.rowcount

                request_state.sql_trace.append(trace_entry)
                request_state.first_name = first_name
                request_state.last_name = last_name
                request_state.email = user_email
                request_state.user_id = user_id

        if query_type == "commit" and conn_id in active_transactions:
            transaction = active_transactions.pop(conn_id)
            transaction_span = transaction["span"]

            transaction_duration = time.time() - transaction["start_time"]

            transaction_span.set_attribute(
                "db.transaction.duration_ms",
                transaction_duration * 1000,
            )
            transaction_span.set_attribute(
                "db.transaction.query_count",
                len(transaction["queries"]),
            )

            tables = set()
            for query in transaction["queries"]:
                if query["table"] != "unknown":
                    tables.add(query["table"])

            transaction_span.set_attribute("db.transaction.tables", list(tables))

            transaction_span.set_attribute("user.id", user_id)
            if user_email:
                transaction_span.set_attribute("user.email", user_email)
            if request_id:
                transaction_span.set_attribute("request.id", request_id)

            transaction_span.end()


def register_db_listeners(engine):
    """Register SQLAlchemy event listeners on a given engine.

    The platform layer passes its engine instance directly so the kernel does
    not need to import any platform-specific lifetime module.
    """
    try:
        event.listen(engine, "before_cursor_execute", before_cursor_execute)
        event.listen(engine, "after_cursor_execute", after_cursor_execute)
        logger.info("Successfully registered SQLAlchemy event listeners")
    except Exception as e:
        logger.error(f"Error registering SQLAlchemy event listeners: {e}")


@event.listens_for(Session, "after_begin")
def _attach_request_state(session: Session, transaction, connection):
    if "request_state" in session.info:
        connection.info["request_state"] = session.info["request_state"]


def get_db_session(request: Request) -> Generator[Session, None, None]:
    """
    Create and get database session.

    :param request: current request.
    :yield: database session.
    """
    SessionLocal = request.app.state.db_session_factory
    session: Session = SessionLocal()
    session.info["request_state"] = request.state
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def _fingerprint_query(statement: str) -> str:
    """
    Create a fingerprint of a SQL query to identify similar queries.
    """
    if not statement:
        return "empty_query"

    normalized = statement.lower()

    normalized = re.sub(r"--.*?$", "", normalized, flags=re.MULTILINE)
    normalized = re.sub(r"/\*.*?\*/", "", normalized, flags=re.DOTALL)

    normalized = re.sub(r"'[^']*'", "'?'", normalized)
    normalized = re.sub(r"\b\d+\b", "?", normalized)

    normalized = re.sub(r"\s+", " ", normalized).strip()

    normalized = re.sub(r"IN\s*\([^)]+\)", "IN (?)", normalized, flags=re.IGNORECASE)

    fingerprint = hashlib.md5(normalized.encode()).hexdigest()

    match = re.match(
        r"^\s*(select|insert|update|delete|create|alter|drop|truncate|begin|commit)",
        normalized,
    )
    if match:
        query_type = match.group(1)
        return f"{query_type}_{fingerprint[:12]}"

    return fingerprint[:16]
