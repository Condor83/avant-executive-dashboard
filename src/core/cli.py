"""CLI entrypoints for config inspection and ingestion workflows."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import typer
from sqlalchemy.orm import Session

from adapters.aave_v3 import AaveV3Adapter, EvmRpcAaveV3Client
from adapters.wallet_balances import EvmRpcBalanceClient, WalletBalancesAdapter
from core.config import load_markets_config
from core.db.session import get_engine
from core.pricing import PriceOracle
from core.runner import SnapshotRunner
from core.settings import get_settings

app = typer.Typer(add_completion=False, help="Avant executive dashboard command line interface.")
sync_app = typer.Typer(help="Ingestion sync commands")
compute_app = typer.Typer(help="Analytics computation commands")

app.add_typer(sync_app, name="sync")
app.add_typer(compute_app, name="compute")

AS_OF_OPTION = typer.Option(default=None, help="UTC timestamp in ISO-8601 format")
MARKETS_PATH_OPTION = typer.Option(default=Path("config/markets.yaml"), help="markets config path")
DATE_OPTION = typer.Option(..., "--date", help="Business date (YYYY-MM-DD)")


def _parse_as_of(as_of: str | None) -> datetime:
    if as_of is None:
        return datetime.now(UTC)

    parsed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_runner(
    markets_path: Path,
) -> tuple[SnapshotRunner, Session, EvmRpcBalanceClient, EvmRpcAaveV3Client, PriceOracle]:
    settings = get_settings()
    markets_config = load_markets_config(markets_path)

    balance_client = EvmRpcBalanceClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    wallet_adapter = WalletBalancesAdapter(
        markets_config=markets_config, balance_client=balance_client
    )
    aave_client = EvmRpcAaveV3Client(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    aave_adapter = AaveV3Adapter(
        markets_config=markets_config,
        rpc_client=aave_client,
        defillama_timeout_seconds=settings.request_timeout_seconds,
        merkl_base_url=settings.merkl_base_url,
        merkl_timeout_seconds=settings.merkl_timeout_seconds,
    )

    price_oracle = PriceOracle(
        base_url=settings.defillama_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    session = Session(get_engine())
    runner = SnapshotRunner(
        session=session,
        markets_config=markets_config,
        price_oracle=price_oracle,
        position_adapters=[wallet_adapter, aave_adapter],
        market_adapters=[aave_adapter],
    )
    return runner, session, balance_client, aave_client, price_oracle


@app.command("show-config")
def show_config() -> None:
    """Print a minimal runtime configuration summary."""

    settings = get_settings()
    typer.echo(f"app_env={settings.app_env}")
    typer.echo(f"database_url={settings.database_url}")


@sync_app.command("snapshot")
def sync_snapshot(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Sync position snapshots from configured adapters."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, balance_client, aave_client, price_oracle = _build_runner(markets_path)

    try:
        result = runner.sync_snapshot(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync snapshot complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        balance_client.close()
        aave_client.close()
        price_oracle.close()


@sync_app.command("prices")
def sync_prices(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Sync token prices via shared price oracle."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, balance_client, aave_client, price_oracle = _build_runner(markets_path)

    try:
        result = runner.sync_prices(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync prices complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        balance_client.close()
        aave_client.close()
        price_oracle.close()


@sync_app.command("markets")
def sync_markets(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Sync market health snapshots from configured market adapters."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, balance_client, aave_client, price_oracle = _build_runner(markets_path)

    try:
        result = runner.sync_markets(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync markets complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        balance_client.close()
        aave_client.close()
        price_oracle.close()


@compute_app.command("daily")
def compute_daily(target_date: str = DATE_OPTION) -> None:
    """Stub command reserved for Sprint 06 yield analytics."""

    try:
        parsed_date = date.fromisoformat(target_date)
    except ValueError as exc:
        raise typer.BadParameter("date must be formatted as YYYY-MM-DD") from exc

    typer.echo(f"compute daily for {parsed_date.isoformat()} is not implemented until Sprint 06")
    raise typer.Exit(
        code=1,
    )


def main() -> None:
    """Run the CLI application."""

    app()


if __name__ == "__main__":
    main()
