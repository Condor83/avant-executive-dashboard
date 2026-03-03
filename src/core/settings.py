"""Application settings and environment parsing."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="AVANT_",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = Field(default="dev", description="Runtime environment name")
    log_level: str = Field(default="INFO", description="Log verbosity")
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/avant_exec_dashboard",
        description="Database URL used by SQLAlchemy",
    )

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "avant_exec_dashboard"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance for the process lifetime."""

    return Settings()
