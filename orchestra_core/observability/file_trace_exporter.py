"""
File-based trace exporters for local development.

Provides two exporters for different use cases:

1. FileSpanExporter (ORCHESTRA_LOG_DIR):
   - Exports spans to JSON files organized by HTTP request
   - One JSON file per trace in requests/ directory
   - Rich filename format: TIME_METHOD_route_DURATION_traceID.json
   - Best for Orchestra-centric debugging

2. JsonlSpanExporter (ORCHESTRA_OTEL_LOG_DIR):
   - Exports spans to JSONL files keyed by trace_id
   - Format: {trace_id}.jsonl (one JSON line per span)
   - Matches Unity's FileSpanExporter format
   - Best for unified traces when running from Unity's test suite

When both are configured, both exporters run (different output formats).
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

logger = logging.getLogger(__name__)

# Flush completed traces (those with root HTTP span) after this short delay
# to catch any straggler spans that arrive in the same batch
COMPLETED_TRACE_FLUSH_DELAY_SECONDS = 0.5

# Fallback timeout for traces without a root HTTP span (background jobs, etc.)
# or in case root span is somehow missed
ORPHAN_TRACE_FLUSH_TIMEOUT_SECONDS = 30.0

# Interval for writing in-progress traces to disk (enables debugging long requests)
IN_PROGRESS_WRITE_INTERVAL_SECONDS = 5.0


def _span_to_dict(span: ReadableSpan) -> dict:
    """Convert a ReadableSpan to a JSON-serializable dictionary."""
    context = span.get_span_context()

    # Convert attributes to serializable format
    attributes = {}
    if span.attributes:
        for key, value in span.attributes.items():
            # Handle various attribute types
            if hasattr(value, "tolist"):  # numpy arrays
                attributes[key] = value.tolist()
            elif isinstance(value, (list, tuple)):
                attributes[key] = list(value)
            else:
                attributes[key] = value

    # Convert events
    events = []
    if span.events:
        for event in span.events:
            event_dict = {
                "name": event.name,
                "timestamp": event.timestamp,
            }
            if event.attributes:
                event_dict["attributes"] = dict(event.attributes)
            events.append(event_dict)

    # Convert links
    links = []
    if span.links:
        for link in span.links:
            link_ctx = link.context
            link_dict = {
                "trace_id": f"{link_ctx.trace_id:032x}",
                "span_id": f"{link_ctx.span_id:016x}",
            }
            if link.attributes:
                link_dict["attributes"] = dict(link.attributes)
            links.append(link_dict)

    return {
        "trace_id": f"{context.trace_id:032x}",
        "span_id": f"{context.span_id:016x}",
        "parent_span_id": f"{span.parent.span_id:016x}" if span.parent else None,
        "name": span.name,
        "kind": span.kind.name if span.kind else None,
        "start_time": span.start_time,
        "end_time": span.end_time,
        "duration_ms": (
            (span.end_time - span.start_time) / 1_000_000
            if span.end_time and span.start_time
            else None
        ),
        "status": {
            "code": span.status.status_code.name if span.status else None,
            "description": span.status.description if span.status else None,
        },
        "attributes": attributes,
        "events": events,
        "links": links,
        "resource": {
            "attributes": dict(span.resource.attributes) if span.resource else {},
        },
    }


def _get_span_type(span_dict: dict) -> str:
    """Classify a span by type based on its attributes."""
    name = (span_dict.get("name") or "").lower()
    attributes = span_dict.get("attributes") or {}

    if "openai" in name or any(
        "openai" in str(k).lower() or "llm" in str(k).lower() for k in attributes.keys()
    ):
        return "openai"

    if any(
        k in attributes
        for k in ["http.method", "http.url", "http.route", "http.status_code"]
    ):
        return "http"

    if any(
        k in attributes
        for k in ["db.system", "db.statement", "db.name", "db.operation"]
    ):
        return "db"

    return "other"


def _is_root_http_span(span_dict: dict) -> bool:
    """Check if this is a root HTTP span (the main request handler)."""
    if _get_span_type(span_dict) != "http":
        return False
    # Root spans have no parent or their parent is from a different trace
    return span_dict.get("parent_span_id") is None


@dataclass
class TraceBuffer:
    """Buffer for collecting spans belonging to a single trace."""

    trace_id: str
    spans: list[dict] = field(default_factory=list)
    last_update: float = field(default_factory=time.monotonic)
    http_root_span: Optional[dict] = None
    # Time when root HTTP span was received (signals request completion)
    completed_at: Optional[float] = None
    # Track when we last wrote this trace to disk (for incremental writes)
    last_written_at: Optional[float] = None
    # Number of spans at last write (to detect changes)
    spans_at_last_write: int = 0

    def add_span(self, span_dict: dict) -> None:
        """Add a span to the buffer."""
        self.spans.append(span_dict)
        self.last_update = time.monotonic()

        # Track the root HTTP span for summary info
        # The root HTTP span arriving means the request is complete (response sent)
        if _is_root_http_span(span_dict):
            # Keep the one with the earliest start time (in case of duplicates)
            if self.http_root_span is None or (
                span_dict.get("start_time", 0)
                < self.http_root_span.get("start_time", float("inf"))
            ):
                self.http_root_span = span_dict
            # Mark as complete when we receive the root HTTP span
            if self.completed_at is None:
                self.completed_at = time.monotonic()

    def is_complete(self) -> bool:
        """Check if trace is complete (root HTTP span received)."""
        return self.completed_at is not None

    def _has_child_spans(self) -> bool:
        """Check if any spans have a parent (i.e., are children)."""
        return any(s.get("parent_span_id") is not None for s in self.spans)

    def is_ready_to_flush(self) -> bool:
        """Check if trace should be flushed (final write, remove from buffer).

        Flush if:
        1. Complete (has root HTTP span) and short delay passed (catch stragglers)
        2. OR orphaned (only root spans, no HTTP) and timeout passed (background jobs)

        NEVER timeout-flush traces with child spans - they're waiting for their
        root span which guarantees we capture the complete HTTP request.
        """
        now = time.monotonic()
        if self.completed_at is not None:
            # Complete trace: flush after short delay
            return (now - self.completed_at) > COMPLETED_TRACE_FLUSH_DELAY_SECONDS

        # If we have child spans, we're waiting for the root span to arrive.
        # NEVER timeout - only flush on shutdown. This guarantees 1 file = 1 request.
        if self._has_child_spans():
            return False

        # No children and no HTTP root = likely background job with only root spans.
        # Safe to timeout-flush these.
        return (now - self.last_update) > ORPHAN_TRACE_FLUSH_TIMEOUT_SECONDS

    def needs_incremental_write(self) -> bool:
        """Check if trace needs an incremental write to disk.

        For in-progress traces, write periodically so debugging is possible
        while the request is still processing.
        """
        # Don't write if already complete (will be flushed soon)
        if self.is_complete():
            return False

        # Don't write if no spans yet
        if not self.spans:
            return False

        # Don't write if no new spans since last write
        if len(self.spans) == self.spans_at_last_write:
            return False

        now = time.monotonic()

        # Write immediately on first span, then periodically
        if self.last_written_at is None:
            return True

        return (now - self.last_written_at) > IN_PROGRESS_WRITE_INTERVAL_SECONDS

    def mark_written(self) -> None:
        """Mark that we've written this trace to disk."""
        self.last_written_at = time.monotonic()
        self.spans_at_last_write = len(self.spans)

    def get_summary(self, include_status: bool = True) -> dict:
        """Generate summary info for the index file.

        Args:
            include_status: Whether to include completion status in summary.
        """
        # Count span types
        type_counts = {"http": 0, "db": 0, "openai": 0, "other": 0}
        for span in self.spans:
            type_counts[_get_span_type(span)] += 1

        # Extract info from root HTTP span (if complete) or request_received span
        root = self.http_root_span
        attrs = (root.get("attributes") or {}) if root else {}

        # If no root HTTP span yet, try to get info from request_received span
        if not root:
            for span in self.spans:
                if span.get("name", "").startswith("http.request_received"):
                    span_attrs = span.get("attributes") or {}
                    attrs = {
                        "http.method": span_attrs.get("http.request.method", ""),
                        "http.route": span_attrs.get("http.request.route", ""),
                    }
                    break

        # Calculate request timing
        start_time = (
            root.get("start_time") if root else None
        ) or self._get_earliest_start_time()
        duration_ms = root.get("duration_ms") if root else None

        # Format timestamp for filename and display
        if start_time:
            dt = datetime.fromtimestamp(start_time / 1e9, tz=timezone.utc)
            time_str = (
                dt.strftime("%Y-%m-%dT%H-%M-%S")
                + f".{int((start_time % 1e9) // 1e6):03d}"
            )
        else:
            now = datetime.now(timezone.utc)
            time_str = (
                now.strftime("%Y-%m-%dT%H-%M-%S") + f".{now.microsecond // 1000:03d}"
            )

        summary = {
            "time": time_str,
            "trace_id": self.trace_id,
            "method": attrs.get("http.method", ""),
            "route": attrs.get("http.route", attrs.get("http.target", "")),
            "status_code": attrs.get("http.status_code"),
            "duration_ms": round(duration_ms, 2) if duration_ms else None,
            "span_count": len(self.spans),
            "db_queries": type_counts["db"],
            "openai_calls": type_counts["openai"],
        }

        if include_status:
            summary["status"] = "complete" if self.is_complete() else "in_progress"

        return summary

    def _get_earliest_start_time(self) -> Optional[int]:
        """Get the earliest start time from all spans."""
        start_times = [s.get("start_time") for s in self.spans if s.get("start_time")]
        return min(start_times) if start_times else None


class FileSpanExporter(SpanExporter):
    """
    Exports spans to JSON files organized by HTTP request.

    Creates one JSON file per trace in requests/ directory with filename:
    - In-progress: TIME_METHOD_route_PENDING_traceID.json
    - Complete:    TIME_METHOD_route_DURATIONms_traceID.json

    Features:
    - Incremental writes: In-progress traces written to disk periodically
    - Duration in filename: Easy to find slow requests via ls/sort
    - File rename on completion: PENDING becomes actual duration
    """

    def __init__(self, trace_log_dir: str):
        self.trace_log_dir = Path(trace_log_dir)
        self.trace_log_dir.mkdir(parents=True, exist_ok=True)

        # Create requests subdirectory
        self.requests_dir = self.trace_log_dir / "requests"
        self.requests_dir.mkdir(exist_ok=True)

        # Buffers for collecting spans by trace_id
        self._trace_buffers: dict[str, TraceBuffer] = {}
        # Track filenames for in-progress traces (to rename on completion)
        self._trace_filenames: dict[str, str] = {}
        self._lock = threading.Lock()

        # Background thread for flushing stale traces
        self._shutdown_event = threading.Event()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

        logger.info(f"FileSpanExporter initialized at {self.trace_log_dir}")

    def _flush_loop(self) -> None:
        """Background loop to flush completed/stale traces and write in-progress ones."""
        # Check frequently (100ms) to flush completed traces promptly
        while not self._shutdown_event.wait(timeout=0.1):
            self._process_traces()

    def _process_traces(self) -> None:
        """Process all traces: flush completed ones, write in-progress ones."""
        ready_to_flush = []
        needs_incremental_write = []

        with self._lock:
            for trace_id, buffer in self._trace_buffers.items():
                if buffer.is_ready_to_flush():
                    ready_to_flush.append(trace_id)
                elif buffer.needs_incremental_write():
                    needs_incremental_write.append(trace_id)

        # Flush completed traces (removes from buffer)
        for trace_id in ready_to_flush:
            self._flush_trace(trace_id)

        # Write in-progress traces (keeps in buffer)
        for trace_id in needs_incremental_write:
            self._write_in_progress_trace(trace_id)

    def _build_filename(
        self,
        trace_id: str,
        summary: dict,
        duration_ms: float | None,
    ) -> str:
        """Build filename with duration indicator.

        Format: TIME_METHOD_route_DURATION_traceID.json
        - In-progress: ..._PENDING_...
        - Complete: ..._45ms_... or ..._1234ms_...
        """
        trace_id_short = trace_id[-8:]
        method = summary.get("method", "").upper() or "UNKNOWN"
        route = summary.get("route", "") or "unknown"

        # Sanitize route for cross-platform filename compatibility
        # Windows NTFS forbids: " : < > | * ? \r \n
        # Also replace / and remove {} from path params
        route_clean = route.strip("/")
        if route_clean.startswith("v0/"):
            route_clean = route_clean[3:]
        route_safe = route_clean.replace("/", "-").replace("{", "").replace("}", "")
        # Remove any remaining characters invalid on Windows NTFS
        route_safe = "".join(c if c not in ':"<>|*?\r\n' else "_" for c in route_safe)
        route_safe = route_safe[:40]

        # Duration: PENDING for in-progress, Xms for complete
        if duration_ms is None:
            duration_str = "PENDING"
        else:
            duration_str = f"{int(duration_ms)}ms"

        return f"{summary['time']}_{method}_{route_safe}_{duration_str}_{trace_id_short}.json"

    def _get_or_create_filename(self, trace_id: str, summary: dict) -> str:
        """Get existing filename for in-progress trace or create a new one."""
        with self._lock:
            if trace_id in self._trace_filenames:
                return self._trace_filenames[trace_id]

            # For in-progress, duration is unknown
            filename = self._build_filename(trace_id, summary, duration_ms=None)
            self._trace_filenames[trace_id] = filename
            return filename

    def _write_in_progress_trace(self, trace_id: str) -> None:
        """Write an in-progress trace to disk (incremental update)."""
        with self._lock:
            buffer = self._trace_buffers.get(trace_id)
            if buffer is None or not buffer.spans:
                return
            # Take a snapshot of current state
            spans_snapshot = list(buffer.spans)
            summary = buffer.get_summary(include_status=True)

        try:
            spans_sorted = sorted(
                spans_snapshot,
                key=lambda s: s.get("start_time") or 0,
            )

            filename = self._get_or_create_filename(trace_id, summary)
            self.requests_dir.mkdir(parents=True, exist_ok=True)

            request_file = self.requests_dir / filename
            with request_file.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "trace_id": trace_id,
                        "summary": summary,
                        "spans": spans_sorted,
                    },
                    f,
                    indent=2,
                    default=str,
                )

            # Mark as written (inside lock to avoid race)
            with self._lock:
                if trace_id in self._trace_buffers:
                    self._trace_buffers[trace_id].mark_written()

            logger.debug(
                f"Wrote in-progress trace {trace_id[-8:]} with {len(spans_sorted)} spans",
            )

        except Exception as e:
            logger.error(f"Failed to write in-progress trace {trace_id}: {e}")

    def _flush_trace(self, trace_id: str) -> None:
        """Write a trace's final state to disk and remove from buffer."""
        with self._lock:
            buffer = self._trace_buffers.pop(trace_id, None)
            # Get existing filename before removing from tracking (for rename)
            in_progress_filename = self._trace_filenames.pop(trace_id, None)

        if buffer is None or not buffer.spans:
            return

        try:
            spans_sorted = sorted(
                buffer.spans,
                key=lambda s: s.get("start_time") or 0,
            )

            summary = buffer.get_summary(include_status=True)
            duration_ms = summary.get("duration_ms")

            # Generate final filename with actual duration
            final_filename = self._build_filename(trace_id, summary, duration_ms)

            self.requests_dir.mkdir(parents=True, exist_ok=True)

            # If we had an in-progress file, rename it to the final name
            if in_progress_filename and in_progress_filename != final_filename:
                in_progress_path = self.requests_dir / in_progress_filename
                final_path = self.requests_dir / final_filename
                if in_progress_path.exists():
                    in_progress_path.rename(final_path)

            # Write the final request file
            request_file = self.requests_dir / final_filename
            with request_file.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "trace_id": trace_id,
                        "summary": summary,
                        "spans": spans_sorted,
                    },
                    f,
                    indent=2,
                    default=str,
                )

            logger.debug(
                f"Flushed trace {trace_id[-8:]} with {len(spans_sorted)} spans",
            )

        except Exception as e:
            logger.error(f"Failed to flush trace {trace_id}: {e}")

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Buffer spans by trace_id for later flushing."""
        try:
            with self._lock:
                for span in spans:
                    span_dict = _span_to_dict(span)
                    span_dict["_exported_at"] = datetime.now(timezone.utc).isoformat()

                    trace_id = span_dict["trace_id"]

                    # Get or create buffer for this trace
                    if trace_id not in self._trace_buffers:
                        self._trace_buffers[trace_id] = TraceBuffer(trace_id=trace_id)

                    self._trace_buffers[trace_id].add_span(span_dict)

            return SpanExportResult.SUCCESS
        except Exception as e:
            logger.error(f"Failed to export spans: {e}")
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Flush all pending traces and stop the background thread."""
        # Signal the flush thread to stop
        self._shutdown_event.set()
        self._flush_thread.join(timeout=5.0)

        # Flush any remaining traces
        with self._lock:
            remaining_trace_ids = list(self._trace_buffers.keys())

        for trace_id in remaining_trace_ids:
            self._flush_trace(trace_id)

        logger.info("FileSpanExporter shutdown complete")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush all pending traces immediately."""
        with self._lock:
            trace_ids = list(self._trace_buffers.keys())

        for trace_id in trace_ids:
            self._flush_trace(trace_id)

        return True


class JsonlSpanExporter(SpanExporter):
    """
    Exports spans to JSONL files, one file per trace_id.

    Format: {log_dir}/{trace_id}.jsonl (one JSON line per span)

    This matches Unity's FileSpanExporter format, enabling unified traces
    when Orchestra and Unity write to the same directory. All spans from
    a single test (Unity → Unillm → Unify → Orchestra) appear in one file.

    Use ORCHESTRA_OTEL_LOG_DIR to enable this exporter.
    """

    def __init__(self, log_dir: str, service_name: str = "orchestra"):
        self.log_dir = Path(log_dir)
        self.service_name = service_name
        self._lock = threading.Lock()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"JsonlSpanExporter initialized at {self.log_dir}")

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export spans to JSONL files keyed by trace_id."""
        try:
            for span in spans:
                self._write_span(span)
            return SpanExportResult.SUCCESS
        except Exception as e:
            logger.error(f"JsonlSpanExporter failed to export spans: {e}")
            return SpanExportResult.FAILURE

    def _write_span(self, span: ReadableSpan) -> None:
        """Write a single span to its trace file."""
        ctx = span.get_span_context()
        if ctx is None or not ctx.is_valid:
            return

        trace_id = f"{ctx.trace_id:032x}"
        span_id = f"{ctx.span_id:016x}"

        parent_span_id = None
        if span.parent is not None:
            parent_span_id = f"{span.parent.span_id:016x}"

        # Build span data matching Unity's format
        span_data = {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "name": span.name,
            "service": self.service_name,
            "start_time": (
                datetime.fromtimestamp(
                    span.start_time / 1e9,
                    tz=timezone.utc,
                ).isoformat()
                if span.start_time
                else None
            ),
            "end_time": (
                datetime.fromtimestamp(span.end_time / 1e9, tz=timezone.utc).isoformat()
                if span.end_time
                else None
            ),
            "duration_ms": (
                (span.end_time - span.start_time) / 1e6
                if span.end_time and span.start_time
                else None
            ),
            "status": span.status.status_code.name if span.status else None,
            "attributes": dict(span.attributes) if span.attributes else {},
        }

        # Write to trace file (append mode, one span per line)
        trace_file = self.log_dir / f"{trace_id}.jsonl"
        with self._lock:
            with open(trace_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(span_data, default=str) + "\n")

    def shutdown(self) -> None:
        """Shutdown the exporter (no-op for JSONL exporter)."""
        logger.info("JsonlSpanExporter shutdown complete")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Force flush any buffered spans (no-op for JSONL exporter)."""
        return True
