"""Database engine and session helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from core.settings import get_settings


def get_engine(database_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine for the configured database."""

    return create_engine(database_url or get_settings().database_url, future=True)


def get_session_factory(database_url: str | None = None) -> sessionmaker[Session]:
    """Return a configured sessionmaker."""

    return sessionmaker(bind=get_engine(database_url), autoflush=False, expire_on_commit=False)


@contextmanager
def session_scope(database_url: str | None = None) -> Iterator[Session]:
    """Context manager that commits on success and rolls back on error."""

    session = get_session_factory(database_url)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
