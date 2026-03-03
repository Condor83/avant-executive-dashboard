"""Postgres fixtures for analytics integration tests."""

from __future__ import annotations

import os
from collections.abc import Generator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

DEFAULT_TEST_ADMIN_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"


@pytest.fixture()
def postgres_database_url() -> Generator[str, None, None]:
    """Create an isolated Postgres database and return its SQLAlchemy URL."""

    admin_url = make_url(os.getenv("AVANT_TEST_DATABASE_URL", DEFAULT_TEST_ADMIN_URL))
    db_name = f"avant_analytics_test_{uuid4().hex[:12]}"

    admin_psycopg_dsn = admin_url.render_as_string(hide_password=False).replace("+psycopg", "")
    test_url = admin_url.set(database=db_name).render_as_string(hide_password=False)

    try:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable for analytics tests: {exc}")

    try:
        yield test_url
    finally:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
