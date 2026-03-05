"""FastAPI dependency injection for database sessions."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from core.settings import get_settings

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a lazy singleton engine from settings."""
    global _engine
    if _engine is None:
        _engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    return _engine


def get_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session, closing it after the request."""
    engine = get_engine()
    with Session(engine) as session:
        yield session
