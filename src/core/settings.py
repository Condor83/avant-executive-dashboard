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
    defillama_base_url: str = Field(
        default="https://coins.llama.fi",
        description="DefiLlama base URL for token price requests",
    )
    defillama_yields_base_url: str = Field(
        default="https://yields.llama.fi",
        description="DefiLlama base URL for yield pool requests",
    )
    merkl_base_url: str = Field(
        default="https://api.merkl.xyz",
        description="Merkl API base URL for reward campaign APY requests",
    )
    request_timeout_seconds: float = Field(
        default=15.0,
        description="HTTP request timeout used by external service clients",
    )
    merkl_timeout_seconds: float = Field(
        default=15.0,
        description="HTTP request timeout used for Merkl API requests",
    )
    evm_rpc_urls: dict[str, str] = Field(
        default_factory=dict,
        description="Chain code -> EVM RPC URL mapping for wallet balance reads",
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
