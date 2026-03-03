"""CLI entrypoints for config inspection and ingestion workflows."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import typer
from sqlalchemy.orm import Session

from adapters.dolomite import DolomiteAdapter, EvmRpcDolomiteClient
from adapters.euler_v2 import EulerV2Adapter, EvmRpcEulerV2Client
from adapters.morpho import EvmRpcMorphoClient, MorphoAdapter
from adapters.wallet_balances import EvmRpcBalanceClient, WalletBalancesAdapter
from core.config import load_markets_config
from core.coverage_report import build_coverage_report
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


class Closeable(Protocol):
    """Simple close protocol for client cleanup in CLI commands."""

    def close(self) -> None:
        """Close underlying resources."""


def _parse_as_of(as_of: str | None) -> datetime:
    if as_of is None:
        return datetime.now(UTC)

    parsed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_runner(
    markets_path: Path,
) -> tuple[SnapshotRunner, Session, list[Closeable]]:
    settings = get_settings()
    markets_config = load_markets_config(markets_path)

    balance_client = EvmRpcBalanceClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    morpho_client = EvmRpcMorphoClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    euler_client = EvmRpcEulerV2Client(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    dolomite_client = EvmRpcDolomiteClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    wallet_adapter = WalletBalancesAdapter(
        markets_config=markets_config, balance_client=balance_client
    )
    morpho_adapter = MorphoAdapter(
        markets_config=markets_config,
        rpc_client=morpho_client,
    )
    euler_adapter = EulerV2Adapter(
        markets_config=markets_config,
        rpc_client=euler_client,
    )
    dolomite_adapter = DolomiteAdapter(
        markets_config=markets_config,
        rpc_client=dolomite_client,
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
        position_adapters=[
            wallet_adapter,
            morpho_adapter,
            euler_adapter,
            dolomite_adapter,
        ],
        market_adapters=[
            morpho_adapter,
            euler_adapter,
            dolomite_adapter,
        ],
    )
    closeables: list[Closeable] = [
        balance_client,
        morpho_client,
        euler_client,
        dolomite_client,
        price_oracle,
    ]
    return runner, session, closeables


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
    runner, session, closeables = _build_runner(markets_path)

    try:
        result = runner.sync_snapshot(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync snapshot complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        for closeable in closeables:
            closeable.close()


@sync_app.command("prices")
def sync_prices(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Sync token prices via shared price oracle."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(markets_path)

    try:
        result = runner.sync_prices(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync prices complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        for closeable in closeables:
            closeable.close()


@sync_app.command("markets")
def sync_markets(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Sync market health snapshots (scaffold for protocol adapters)."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(markets_path)

    try:
        result = runner.sync_markets(as_of_ts_utc=as_of_ts_utc)
        session.commit()
        typer.echo(
            f"sync markets complete as_of={as_of_ts_utc.isoformat()} "
            f"rows_written={result.rows_written} issues_written={result.issues_written}"
        )
    finally:
        session.close()
        for closeable in closeables:
            closeable.close()


@sync_app.command("coverage-report")
def sync_coverage_report(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
) -> None:
    """Print wallet/market coverage and failures for adapter protocols."""

    as_of_ts_utc = _parse_as_of(as_of)
    markets_config = load_markets_config(markets_path)
    session = Session(get_engine())

    try:
        report = build_coverage_report(
            session=session,
            markets_config=markets_config,
            as_of_ts_utc=as_of_ts_utc,
        )
        typer.echo(report)
    finally:
        session.close()


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
