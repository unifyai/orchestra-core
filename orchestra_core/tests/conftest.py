"""Pytest fixtures for orchestra-core's kernel test suite.

Sets up a transactional Postgres database per test session, drives the
kernel alembic chain to head once, and yields a stub-auth-aware FastAPI
TestClient and per-test SQLAlchemy session that nests inside a SAVEPOINT
so individual tests can mutate state without bleeding into each other.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Generator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from orchestra_core.db.meta import meta
from orchestra_core.db.models import load_all_models
from orchestra_core.settings import settings


@pytest.fixture(scope="session")
def _engine() -> Generator[Engine, None, None]:
    """Session-scoped engine that runs the kernel alembic chain once."""
    load_all_models()
    engine = create_engine(str(settings.db_url), pool_pre_ping=True)

    # pgvector extension + clean slate. Drop everything we own AND the
    # alembic_version bookkeeping table so alembic sees an empty DB and
    # actually runs the migration.
    with engine.connect() as conn:
        conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()
        meta.drop_all(conn)
        conn.exec_driver_sql("DROP TABLE IF EXISTS alembic_version")
        conn.commit()

    # Run the alembic chain so the test DB is identical to a fresh
    # production deploy. Use sys.executable rather than `python` so the
    # subprocess inherits the test runner's interpreter (which has
    # alembic installed) regardless of how PATH is set up on the host.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
        check=True,
        cwd=repo_root,
        env={**os.environ},
    )

    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def _truncate_kernel_tables(_engine: Engine) -> Generator[None, None, None]:
    """Wipe kernel tables before each test.

    The kernel routers create their own SQLAlchemy sessions via
    `request.app.state.db_session_factory`, which means a single
    SAVEPOINT-bound test session can't isolate request-driven state.
    Truncating up front keeps each test starting from a known empty DB.
    """
    yield
    with _engine.connect() as conn:
        # Order matters only when FKs are RESTRICT; CASCADE makes this safe.
        conn.exec_driver_sql(
            "TRUNCATE TABLE "
            "embedding_queue, embedding, log_event_version, "
            "log_event_context, log_unique_constraint, log_event, "
            "active_derived_log_template, field_type, "
            "context_counter, context_version, context, "
            "project_version, project "
            "RESTART IDENTITY CASCADE"
        )
        conn.commit()


@pytest.fixture
def dbsession(_engine: Engine) -> Generator[Session, None, None]:
    """Per-test session for tests that want direct DB access."""
    SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def app(_engine: Engine) -> FastAPI:
    """Build the kernel FastAPI app, wiring its session factory at our test engine."""
    from orchestra_core.web.application import get_app

    fastapi_app = get_app()
    fastapi_app.state.db_engine = _engine
    fastapi_app.state.db_session_factory = sessionmaker(
        bind=_engine, expire_on_commit=False
    )
    return fastapi_app


@pytest.fixture
def api_key() -> str:
    """The bearer token used for authenticated API calls in tests."""
    key = "test-kernel-key"
    os.environ["ORCHESTRA_API_KEY"] = key
    return key


@pytest.fixture
def client(app: FastAPI, api_key: str) -> Generator[TestClient, None, None]:
    """Authenticated client: every request includes the test bearer token."""
    headers = {"Authorization": f"Bearer {api_key}"}
    with TestClient(app, headers=headers) as c:
        yield c


@pytest.fixture
def unauth_client(app: FastAPI) -> Generator[TestClient, None, None]:
    """Client that does NOT inject any auth header. For testing the auth gate."""
    with TestClient(app) as c:
        yield c
