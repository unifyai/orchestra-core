"""Alembic env for orchestra-core.

Loads only the kernel models (`orchestra_core.db.models`) so autogenerate
sees the kernel schema in isolation.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, create_engine

from orchestra_core.db.meta import meta
from orchestra_core.db.models import load_all_models
from orchestra_core.settings import settings

config = context.config

load_all_models()

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = meta


def run_migrations_offline() -> None:
    context.configure(
        url=str(settings.db_url),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(str(settings.db_url))
    with connectable.connect() as connection:
        do_run_migrations(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
