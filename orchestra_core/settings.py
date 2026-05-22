"""Kernel settings for orchestra-core.

Single-tenant configuration only: server, database, and observability. The
multi-tenant platform extends this class via inheritance to add Stripe, MFA,
voice provider keys, GCP, OAuth, etc.
"""

import enum
import os
from pathlib import Path
from tempfile import gettempdir
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from yarl import URL

TEMP_DIR = Path(gettempdir())


class LogLevel(str, enum.Enum):
    """Possible log levels."""

    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    FATAL = "FATAL"


class UniqueValidationMode(str, enum.Enum):
    """
    Mode for unique field validation.

    JSONB_SCAN: Scan all logs with JSONB containment (slow, O(N x M))
    LOOKUP_TABLE: Use lookup table with B-tree index (fast, O(M x log N))
    """

    JSONB_SCAN = "jsonb_scan"
    LOOKUP_TABLE = "lookup_table"


class Settings(BaseSettings):
    """Kernel application settings.

    Configurable via environment variables prefixed with `ORCHESTRA_`.
    """

    host: str = "127.0.0.1"
    port: int = 8000
    workers_count: int = 1
    reload: bool = False
    timeout_keep_alive: int = 15

    # Inactivity timeout in seconds. If set, the server shuts itself down
    # after this many seconds without any inbound requests. Default (None) =
    # run forever.
    inactivity_timeout_seconds: Optional[int] = None

    environment: str = "dev"

    log_level: LogLevel = LogLevel.INFO

    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = os.environ.get("ORCHESTRA_DB_USER", "")
    db_pass: str = os.environ.get("ORCHESTRA_DB_PASS", "")
    db_base: str = os.environ.get("ORCHESTRA_DB_BASE", "")
    db_path_query: str = ""
    db_send_host: bool = True
    db_echo: bool = False

    prometheus_dir: Path = TEMP_DIR / "prom"

    # OpenTelemetry master switch.
    otel_enabled: bool = os.environ.get("ORCHESTRA_OTEL", "true").lower() in (
        "true",
        "1",
    )

    # OTLP endpoint for OpenTelemetry export (e.g. http://localhost:4317).
    otel_endpoint: Optional[str] = os.environ.get("ORCHESTRA_OTEL_ENDPOINT")
    otel_secure: bool = os.environ.get("ORCHESTRA_OTEL_SECURE", "").lower() == "true"

    loki_url: Optional[str] = os.environ.get("ORCHESTRA_LOKI_URL", None)
    loki_username: Optional[str] = os.environ.get("ORCHESTRA_LOKI_USERNAME")
    loki_password: Optional[str] = os.environ.get("ORCHESTRA_LOKI_PASSWORD")

    tempo_url: Optional[str] = os.environ.get("ORCHESTRA_TEMPO_URL", None)

    grafana_url: Optional[str] = os.environ.get("ORCHESTRA_GRAFANA_URL", None)

    log_enabled: bool = os.environ.get("ORCHESTRA_LOG", "true").lower() in ("true", "1")

    log_dir: Optional[str] = os.environ.get("ORCHESTRA_LOG_DIR", None)
    otel_log_dir: Optional[str] = os.environ.get("ORCHESTRA_OTEL_LOG_DIR", None)

    otel_exclude_patterns: list[str] = [
        p.strip()
        for p in os.environ.get(
            "ORCHESTRA_OTEL_EXCLUDE_PATTERNS",
            "connect,db.query.select.users,db.query.select.api_key,"
            "db.query.select.team_member,db.query.select.resource_access",
        ).split(",")
        if p.strip()
    ]

    cors_allow_origins: list[str] = []

    @property
    def db_url(self) -> URL:
        """Assemble database URL from settings."""
        host = self.db_host
        port = self.db_port
        if not self.db_send_host:
            host = ""
            port = None  # type: ignore

        return URL.build(
            scheme="postgresql+psycopg2",
            host=host,
            port=port,
            user=self.db_user,
            password=self.db_pass,
            path=f"/{self.db_base}",
            query=self.db_path_query,
        )

    @property
    def use_aggregation_cte_optimization(self) -> bool:
        """Pre-compute aggregations in CTEs instead of correlated subqueries."""
        return (
            os.environ.get(
                "ORCHESTRA_USE_AGGREGATION_CTE_OPTIMIZATION",
                "true",
            ).lower()
            == "true"
        )

    @property
    def unique_validation_mode(self) -> UniqueValidationMode:
        """Get the unique field validation mode."""
        mode_str = os.environ.get(
            "ORCHESTRA_UNIQUE_VALIDATION_MODE",
            UniqueValidationMode.LOOKUP_TABLE.value,
        )
        try:
            return UniqueValidationMode(mode_str)
        except ValueError:
            return UniqueValidationMode.LOOKUP_TABLE

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ORCHESTRA_",
        env_file_encoding="utf-8",
        extra="allow",
    )


settings = Settings()
