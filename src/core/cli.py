"""CLI entrypoints for config inspection and ingestion workflows."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import typer
from sqlalchemy.orm import Session

from adapters.aave_v3 import AaveV3Adapter, EvmRpcAaveV3Client
from adapters.kamino import KaminoAdapter, KaminoApiClient
from adapters.silo_v2 import SiloApiClient, SiloV2Adapter
from adapters.wallet_balances import EvmRpcBalanceClient, WalletBalancesAdapter
from adapters.zest import StacksApiZestClient, ZestAdapter
from core.config import load_consumer_markets_config, load_markets_config
from core.db.session import get_engine
from core.pricing import PriceOracle
from core.runner import SnapshotRunner
from core.settings import get_settings
from core.stacks_client import StacksClient
from core.yields import DefiLlamaYieldOracle

app = typer.Typer(add_completion=False, help="Avant executive dashboard command line interface.")
sync_app = typer.Typer(help="Ingestion sync commands")
compute_app = typer.Typer(help="Analytics computation commands")

app.add_typer(sync_app, name="sync")
app.add_typer(compute_app, name="compute")

AS_OF_OPTION = typer.Option(default=None, help="UTC timestamp in ISO-8601 format")
MARKETS_PATH_OPTION = typer.Option(default=Path("config/markets.yaml"), help="markets config path")
CONSUMER_MARKETS_PATH_OPTION = typer.Option(
    default=Path("config/consumer_markets.yaml"),
    help="consumer markets config path",
)
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
    consumer_markets_path: Path,
) -> tuple[SnapshotRunner, Session, list[object]]:
    settings = get_settings()
    markets_config = load_markets_config(markets_path)
    consumer_markets_config = load_consumer_markets_config(consumer_markets_path)

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
    kamino_client = KaminoApiClient(
        base_url=settings.kamino_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    yield_oracle = DefiLlamaYieldOracle(
        base_url=settings.defillama_yields_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    kamino_adapter = KaminoAdapter(
        markets_config=markets_config,
        client=kamino_client,
        yield_oracle=yield_oracle,
    )
    stacks_client = StacksClient(
        base_url=settings.stacks_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    zest_client = StacksApiZestClient(
        stacks_client=stacks_client,
        zest_api_base_url=settings.zest_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    zest_adapter = ZestAdapter(markets_config=markets_config, client=zest_client)
    silo_client = SiloApiClient(
        base_url=settings.silo_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        points_base_url=settings.silo_points_api_base_url,
    )
    silo_adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets_config,
        client=silo_client,
        top_holders_limit=settings.silo_top_holders_limit,
    )

    price_oracle = PriceOracle(
        base_url=settings.defillama_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    session = Session(get_engine())
    runner = SnapshotRunner(
        session=session,
        markets_config=markets_config,
        consumer_markets_config=consumer_markets_config,
        price_oracle=price_oracle,
        position_adapters=[
            wallet_adapter,
            aave_adapter,
            kamino_adapter,
            zest_adapter,
            silo_adapter,
        ],
        market_adapters=[aave_adapter, kamino_adapter, zest_adapter, silo_adapter],
    )
    closeables: list[object] = [
        balance_client,
        aave_client,
        kamino_client,
        yield_oracle,
        zest_client,
        stacks_client,
        silo_client,
        price_oracle,
    ]
    return runner, session, closeables


def _close_clients(closeables: list[object]) -> None:
    for item in closeables:
        close_fn = getattr(item, "close", None)
        if callable(close_fn):
            close_fn()


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
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
) -> None:
    """Sync position snapshots from configured adapters."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(markets_path, consumer_markets_path)

    try:
        result = runner.sync_snapshot(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync snapshot complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        _close_clients(closeables)


@sync_app.command("prices")
def sync_prices(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
) -> None:
    """Sync token prices via shared price oracle."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(markets_path, consumer_markets_path)

    try:
        result = runner.sync_prices(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync prices complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        _close_clients(closeables)


@sync_app.command("markets")
def sync_markets(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
) -> None:
    """Sync market health snapshots from configured market adapters."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(markets_path, consumer_markets_path)

    try:
        result = runner.sync_markets(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync markets complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        _close_clients(closeables)


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
