"""Settings parsing tests for Sprint 00."""

from core.settings import Settings


def test_settings_defaults(monkeypatch, tmp_path) -> None:
    """Defaults apply when no AVANT_* environment variables are set."""

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AVANT_APP_ENV", raising=False)
    monkeypatch.delenv("AVANT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("AVANT_POSTGRES_PORT", raising=False)

    settings = Settings()

    assert settings.app_env == "dev"
    assert settings.log_level == "INFO"
    assert settings.postgres_port == 5432


def test_settings_parses_environment(monkeypatch, tmp_path) -> None:
    """Environment variables are parsed and cast to expected types."""

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AVANT_APP_ENV", "test")
    monkeypatch.setenv("AVANT_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("AVANT_POSTGRES_PORT", "6543")

    settings = Settings()

    assert settings.app_env == "test"
    assert settings.log_level == "DEBUG"
    assert settings.postgres_port == 6543
