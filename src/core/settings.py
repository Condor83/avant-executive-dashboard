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
        min_length=1,
    )
    defillama_base_url: str = Field(
        default="https://coins.llama.fi",
        description="DefiLlama base URL for token price requests",
    )
    debank_cloud_base_url: str = Field(
        default="https://pro-openapi.debank.com",
        description="DeBank Cloud API base URL for discovery and reconciliation scans",
    )
    debank_cloud_api_key: str | None = Field(
        default=None,
        description="DeBank Cloud API key used for authenticated requests",
    )
    defillama_yields_base_url: str = Field(
        default="https://yields.llama.fi",
        description="DefiLlama base URL for pool yield requests",
    )
    merkl_base_url: str = Field(
        default="https://api.merkl.xyz",
        description="Merkl API base URL for reward campaign APY requests",
    )
    request_timeout_seconds: float = Field(
        default=15.0,
        description="HTTP request timeout used by external service clients",
        gt=0,
    )
    merkl_timeout_seconds: float = Field(
        default=15.0,
        description="HTTP request timeout used for Merkl API requests",
        gt=0,
    )
    evm_rpc_urls: dict[str, str] = Field(
        default_factory=dict,
        description="Chain code -> EVM RPC URL mapping for wallet balance reads",
    )
    solana_rpc_urls: dict[str, str] = Field(
        default_factory=dict,
        description="Chain code -> Solana RPC URL mapping",
    )
    stacks_api_base_url: str = Field(
        default="https://api.hiro.so",
        description="Stacks API base URL used for read-only balance queries",
    )
    zest_api_base_url: str | None = Field(
        default=None,
        description="Optional Zest API base URL for market totals/rates and borrow reads",
    )
    kamino_api_base_url: str = Field(
        default="https://api.kamino.finance",
        description="Kamino API base URL for market snapshots",
    )
    silo_api_base_url: str = Field(
        default="https://app.silo.finance",
        description="Silo app API base URL for consumer market snapshots",
    )
    silo_points_api_base_url: str | None = Field(
        default="https://api-points.silo.finance",
        description="Silo points API base URL for consumer top-holder reads",
    )
    silo_top_holders_limit: int = Field(
        default=50,
        description="Top-holder limit for Silo consumer position ingestion",
        gt=0,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance for the process lifetime."""

    return Settings()
