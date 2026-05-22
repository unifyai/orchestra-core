"""
Span processor that filters out noisy spans by name pattern.

Wraps an inner SpanProcessor and drops spans whose name matches any of
the configured exclude patterns (substring match).  Used to suppress
repetitive auth / connection-pool spans that add bulk without diagnostic
value.
"""

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor


class FilteringSpanProcessor(SpanProcessor):
    """Drops spans matching any of the exclude patterns before forwarding."""

    def __init__(self, inner: SpanProcessor, exclude_patterns: list[str]):
        self._inner = inner
        self._exclude_patterns = exclude_patterns

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        self._inner.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        name = span.name
        if not any(pattern in name for pattern in self._exclude_patterns):
            self._inner.on_end(span)

    def shutdown(self) -> None:
        self._inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._inner.force_flush(timeout_millis)
