"""OpenTelemetry setup helpers for orchestra-core.

Configures a TracerProvider with optional OTLP, Tempo, and JSONL/per-request
file exporters. The platform's `lifetime.py` calls `setup_opentelemetry(app)`
on application startup; the kernel app uses the same helpers directly.
"""

from __future__ import annotations

import logging

import starlette.routing
from fastapi import FastAPI
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_NAME,
    TELEMETRY_SDK_LANGUAGE,
    Resource,
)
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.trace import get_tracer_provider, set_tracer_provider

from orchestra_core.settings import settings

logger = logging.getLogger(__name__)

_otel_tracer_provider_initialized = False


def _create_tracer_provider() -> TracerProvider:
    """Create and configure a TracerProvider with all configured exporters."""
    resource = Resource.create(
        {
            SERVICE_NAME: "orchestra",
            TELEMETRY_SDK_LANGUAGE: "python",
            DEPLOYMENT_ENVIRONMENT: settings.environment,
        },
    )

    tracer_provider = TracerProvider(resource=resource)

    def _add_processor(proc: SpanProcessor) -> None:
        if settings.otel_exclude_patterns:
            from orchestra_core.observability.filtering_span_processor import (
                FilteringSpanProcessor,
            )

            proc = FilteringSpanProcessor(proc, settings.otel_exclude_patterns)
        tracer_provider.add_span_processor(proc)

    if settings.otel_endpoint:
        try:
            _add_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(
                        endpoint=settings.otel_endpoint,
                        insecure=not settings.otel_secure,
                        timeout=5,
                    ),
                ),
            )
            logger.info(f"Configured OTLP exporter at {settings.otel_endpoint}")
        except Exception as e:
            logger.warning(f"Failed to configure OTLP exporter: {e}")

    if settings.tempo_url:
        try:
            if ":4318" in settings.tempo_url:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter as HTTPSpanExporter,
                )

                tempo_endpoint = f"{settings.tempo_url}/v1/traces"
                tempo_exporter = HTTPSpanExporter(
                    endpoint=tempo_endpoint,
                    timeout=5,
                )
                logger.info(f"Configured Tempo HTTP exporter at {tempo_endpoint}")
            elif ":4317" in settings.tempo_url:
                tempo_exporter = OTLPSpanExporter(
                    endpoint=settings.tempo_url,
                    insecure=True,
                    timeout=5,
                )
                logger.info(f"Configured Tempo gRPC exporter at {settings.tempo_url}")
            else:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter as HTTPSpanExporter,
                )

                tempo_endpoint = f"{settings.tempo_url}/v1/traces"
                tempo_exporter = HTTPSpanExporter(
                    endpoint=tempo_endpoint,
                    timeout=5,
                )
                logger.info(f"Configured Tempo HTTP exporter at {tempo_endpoint}")
            _add_processor(BatchSpanProcessor(tempo_exporter))
        except Exception as e:
            logger.warning(f"Failed to configure Tempo exporter: {e}")

    if settings.log_enabled and settings.otel_log_dir:
        try:
            from orchestra_core.observability.file_trace_exporter import (
                JsonlSpanExporter,
            )

            jsonl_exporter = JsonlSpanExporter(
                settings.otel_log_dir,
                service_name="orchestra",
            )
            _add_processor(SimpleSpanProcessor(jsonl_exporter))
            logger.info(
                f"Configured JSONL span exporter at {settings.otel_log_dir}",
            )
        except Exception as e:
            logger.warning(f"Failed to configure JSONL span exporter: {e}")

    if settings.log_enabled and settings.log_dir:
        try:
            from orchestra_core.observability.file_trace_exporter import (
                FileSpanExporter,
            )

            file_exporter = FileSpanExporter(settings.log_dir)
            _add_processor(BatchSpanProcessor(file_exporter))
            logger.info(
                f"Configured per-request JSON exporter at {settings.log_dir}",
            )
        except Exception as e:
            logger.warning(f"Failed to configure per-request JSON exporter: {e}")

    return tracer_provider


def setup_opentelemetry(app: FastAPI) -> None:
    """Set up the global TracerProvider and per-app instrumentation.

    Idempotent: the global TracerProvider and library instrumentation
    (httpx, optional OpenAI) are configured once; per-app FastAPI and
    SQLAlchemy instrumentation runs every call so multiple app instances
    (e.g. in tests) share the same provider.
    """
    global _otel_tracer_provider_initialized

    if not settings.otel_enabled:
        return

    if (
        not settings.otel_endpoint
        and not settings.tempo_url
        and not settings.log_dir
        and not settings.otel_log_dir
    ):
        return

    if not _otel_tracer_provider_initialized:
        tracer_provider = _create_tracer_provider()
        set_tracer_provider(tracer_provider=tracer_provider)

        HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)
        logger.info("Instrumented httpx client for HTTP-level tracing")

        _otel_tracer_provider_initialized = True
        logger.info("OTel TracerProvider initialized")

    tracer_provider = get_tracer_provider()

    _exclude_names = [
        "health_check",
        "openapi",
        "swagger_ui_html",
        "swagger_ui_redirect",
        "redoc_html",
        "metrics",
    ]
    excluded_endpoints = []
    for name in _exclude_names:
        try:
            excluded_endpoints.append(str(app.url_path_for(name)))
        except starlette.routing.NoMatchFound:
            pass

    FastAPIInstrumentor().instrument_app(
        app,
        tracer_provider=tracer_provider,
        excluded_urls=",".join(excluded_endpoints),
    )

    if hasattr(app.state, "db_engine") and app.state.db_engine is not None:
        SQLAlchemyInstrumentor().instrument(
            tracer_provider=tracer_provider,
            engine=app.state.db_engine,
        )


def flush_opentelemetry(timeout_millis: int = 5000) -> None:
    """Flush all pending traces to ensure they are written to exporters."""
    if not settings.otel_enabled or not _otel_tracer_provider_initialized:
        return

    tracer_provider = get_tracer_provider()
    if hasattr(tracer_provider, "force_flush"):
        try:
            tracer_provider.force_flush(timeout_millis=timeout_millis)
            logger.debug("Flushed OTel traces")
        except Exception as e:
            logger.warning(f"Failed to flush OTel traces: {e}")


def stop_opentelemetry(app: FastAPI) -> None:
    """Disable OpenTelemetry instrumentation for a specific app."""
    if not settings.otel_enabled:
        return

    try:
        FastAPIInstrumentor().uninstrument_app(app)
    except Exception as e:
        logger.debug(f"Failed to uninstrument FastAPI app: {e}")

    try:
        SQLAlchemyInstrumentor().uninstrument()
    except Exception as e:
        logger.debug(f"Failed to uninstrument SQLAlchemy: {e}")
