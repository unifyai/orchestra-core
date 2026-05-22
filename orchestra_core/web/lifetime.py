"""Kernel app lifetime: DB engine setup, observability, startup/shutdown."""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from orchestra_core.db.dependencies import register_db_listeners
from orchestra_core.observability.inactivity_shutdown import (
    start_inactivity_monitor,
    stop_inactivity_monitor,
)
from orchestra_core.observability.otel_setup import (
    flush_opentelemetry,
    setup_opentelemetry,
    stop_opentelemetry,
)
from orchestra_core.settings import settings

logger = logging.getLogger(__name__)

_engine = None


def _setup_db(app: FastAPI) -> None:
    """Create the SQLAlchemy engine + session factory for this app."""
    global _engine
    engine = create_engine(
        str(settings.db_url),
        echo=settings.db_echo,
        pool_size=50,
        max_overflow=100,
        pool_pre_ping=True,
    )
    session_factory = sessionmaker(engine, expire_on_commit=False)

    app.state.db_engine = engine
    app.state.db_session_factory = session_factory
    _engine = engine


def get_engine():
    """Return the SQLAlchemy engine for the running app."""
    if _engine is None:
        raise RuntimeError("Database engine not initialized")
    return _engine


def setup_observability(app: FastAPI) -> None:
    """Wire OTel + DB query listeners onto a started app."""
    try:
        setup_opentelemetry(app)
    except Exception as e:
        logger.error(f"Failed to setup OpenTelemetry: {e}")

    if hasattr(app.state, "db_engine") and app.state.db_engine is not None:
        try:
            register_db_listeners(app.state.db_engine)
        except Exception as e:
            logger.error(f"Failed to register DB listeners: {e}")


def register_startup_event(app: FastAPI) -> Callable[[], None]:
    @app.on_event("startup")
    def _startup() -> None:
        app.middleware_stack = None
        _setup_db(app)
        setup_observability(app)
        app.middleware_stack = app.build_middleware_stack()
        start_inactivity_monitor()

    return _startup


def register_shutdown_event(app: FastAPI) -> Callable[[], None]:
    @app.on_event("shutdown")
    async def _shutdown() -> None:
        stop_inactivity_monitor()
        if hasattr(app.state, "db_engine") and app.state.db_engine is not None:
            app.state.db_engine.dispose()
        stop_opentelemetry(app)
        flush_opentelemetry()

    return _shutdown
