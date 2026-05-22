from sqlalchemy.orm import DeclarativeBase

from orchestra_core.db.meta import meta


class Base(DeclarativeBase):
    """Base for all models."""

    metadata = meta
