"""Settings parsing tests for Sprint 00."""

import pytest
from pydantic import ValidationError

from core.settings import Settings


def test_settings_defaults(monkeypatch, tmp_path) -> None:
    """Defaults apply when no AVANT_* environment variables are set."""

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AVANT_APP_ENV", raising=False)
    monkeypatch.delenv("AVANT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("AVANT_DATABASE_URL", raising=False)

    settings = Settings()

    assert settings.app_env == "dev"
    assert settings.log_level == "INFO"
    assert (
        settings.database_url
        == "postgresql+psycopg://postgres:postgres@localhost:5432/avant_exec_dashboard"
    )
    assert settings.avant_api_base_url == "https://app.avantprotocol.com/api"
    assert settings.bracket_graphql_url == "https://app.bracket.fi/api/vaults/graphql"
    assert settings.debank_cloud_base_url == "https://pro-openapi.debank.com"
    assert settings.debank_cloud_api_key is None
    assert settings.pendle_api_base_url == "https://api-v2.pendle.finance/core"


def test_settings_parses_environment(monkeypatch, tmp_path) -> None:
    """Environment variables are parsed and cast to expected types."""

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AVANT_APP_ENV", "test")
    monkeypatch.setenv("AVANT_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv(
        "AVANT_DATABASE_URL",
        "postgresql+psycopg://app:secret@db.internal:6543/avant_exec_dashboard",
    )
    monkeypatch.setenv("AVANT_AVANT_API_BASE_URL", "https://app.avantprotocol.test/api")
    monkeypatch.setenv("AVANT_BRACKET_GRAPHQL_URL", "https://app.bracket.test/api/vaults/graphql")
    monkeypatch.setenv("AVANT_DEBANK_CLOUD_API_KEY", "debank-key")
    monkeypatch.setenv("AVANT_PENDLE_API_BASE_URL", "https://api-v2.pendle.test/core")

    settings = Settings()

    assert settings.app_env == "test"
    assert settings.log_level == "DEBUG"
    assert settings.database_url == (
        "postgresql+psycopg://app:secret@db.internal:6543/avant_exec_dashboard"
    )
    assert settings.avant_api_base_url == "https://app.avantprotocol.test/api"
    assert settings.bracket_graphql_url == "https://app.bracket.test/api/vaults/graphql"
    assert settings.debank_cloud_api_key == "debank-key"
    assert settings.pendle_api_base_url == "https://api-v2.pendle.test/core"


@pytest.mark.parametrize(
    ("env_name", "env_value"),
    [
        ("AVANT_REQUEST_TIMEOUT_SECONDS", "0"),
        ("AVANT_REQUEST_TIMEOUT_SECONDS", "-1"),
        ("AVANT_MERKL_TIMEOUT_SECONDS", "0"),
        ("AVANT_MERKL_TIMEOUT_SECONDS", "-1"),
        ("AVANT_SILO_TOP_HOLDERS_LIMIT", "0"),
        ("AVANT_SILO_TOP_HOLDERS_LIMIT", "-5"),
    ],
)
def test_settings_rejects_non_positive_runtime_values(
    monkeypatch, tmp_path, env_name: str, env_value: str
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValidationError):
        Settings()
