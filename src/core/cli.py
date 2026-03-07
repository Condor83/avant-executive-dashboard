"""CLI entrypoints for config inspection and ingestion workflows."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

import typer
import yaml
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from adapters.aave_v3 import AaveV3Adapter, EvmRpcAaveV3Client
from adapters.bracket import BracketNavYieldOracle
from adapters.dolomite import DolomiteAdapter, EvmRpcDolomiteClient
from adapters.etherex import EtherexAdapter, EvmRpcEtherexClient
from adapters.euler_v2 import EulerV2Adapter, EvmRpcEulerV2Client
from adapters.kamino import KaminoAdapter, KaminoApiClient
from adapters.morpho import EvmRpcMorphoClient, MorphoAdapter, MorphoVaultYieldClient
from adapters.pendle import PendleHistoryClient
from adapters.silo_v2 import SiloApiClient, SiloV2Adapter
from adapters.spark import EvmRpcSparkClient, SparkAdapter
from adapters.stakedao import EvmRpcStakedaoClient, StakedaoAdapter
from adapters.traderjoe_lp import EvmRpcTraderJoeLpClient, TraderJoeLpAdapter
from adapters.wallet_balances import EvmRpcBalanceClient, WalletBalancesAdapter
from adapters.zest import StacksApiZestClient, ZestAdapter
from analytics.alerts import AlertEngine
from analytics.executive_summary import ExecutiveSummaryEngine
from analytics.market_engine import MarketEngine
from analytics.market_views import MarketViewEngine
from analytics.portfolio_views import PortfolioViewEngine
from analytics.risk_engine import (
    RiskEngine,
    top_markets_by_kink_risk,
    top_positions_by_worst_net_spread,
)
from analytics.rollups import compute_window_rollups
from analytics.yield_engine import YieldEngine, select_business_day_boundaries
from core.config import (
    canonical_address,
    load_consumer_markets_config,
    load_markets_config,
    load_pt_fixed_yield_overrides_config,
    load_risk_thresholds_config,
    load_wallet_products_config,
)
from core.coverage_report import build_coverage_report
from core.customer_cohort import (
    ROUTESCAN_BASE_URL,
    RouteScanClient,
    build_customer_wallet_cohort,
    build_wallet_cohort_config_payload,
    collect_evm_addresses_from_yaml,
    collect_strategy_wallets,
    minimum_balance_raw_for_usd_threshold,
    rpc_contract_addresses,
)
from core.db.models import Market, PositionSnapshot, YieldDaily
from core.db.session import get_engine
from core.debank_cloud import DebankCloudClient
from core.debank_coverage import run_debank_coverage_audit, serialize_audit_result
from core.dolomite_discovery import discover_wallet_positions, wallet_candidates_for_groups
from core.pricing import PriceOracle
from core.runner import RunnerSummary, SnapshotRunner
from core.settings import get_settings
from core.stacks_client import StacksClient
from core.yields import AvantYieldOracle, DefiLlamaYieldOracle

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
WALLET_PRODUCTS_PATH_OPTION = typer.Option(
    default=Path("config/wallet_products.yaml"),
    help="wallet products config path",
)
DOLOMITE_CHAIN_CODE_OPTION = typer.Option("bera", "--chain-code", help="Dolomite chain code")
DOLOMITE_WALLET_GROUPS_OPTION = typer.Option(
    "avETH,avETHx",
    "--wallet-groups",
    help="Comma-separated wallet groups from wallet_products (for example avETH,avETHx)",
)
DOLOMITE_MAX_ACCOUNT_NUMBER_OPTION = typer.Option(
    32, "--max-account-number", help="Maximum Dolomite account number to scan (inclusive)"
)
DOLOMITE_MIN_USD_OPTION = typer.Option(
    "1000",
    "--min-usd",
    help="Minimum absolute supplied+borrowed USD exposure to show",
)
DOLOMITE_FALLBACK_PROBE_MAX_MARKET_ID_OPTION = typer.Option(
    63,
    "--fallback-probe-max-market-id",
    help="Fallback max market id probe when getNumMarkets RPC fails",
)
DATE_OPTION = typer.Option(..., "--date", help="Business date (YYYY-MM-DD)")
RISK_DATE_OPTION = typer.Option(None, "--date", help="Business date (YYYY-MM-DD)")
DAILY_BOUNDARY_POLICY_OPTION = typer.Option(
    "auto",
    "--boundary-policy",
    help="Daily boundary policy: auto, in_day, or latest_snapshot",
)
RISK_THRESHOLDS_PATH_OPTION = typer.Option(
    default=Path("config/risk_thresholds.yaml"),
    help="Risk thresholds config path",
)
WINDOW_OPTION = typer.Option(..., "--window", help="Rollup window (7d or 30d)")
END_DATE_OPTION = typer.Option(
    None,
    "--end-date",
    help="Window end date (YYYY-MM-DD). Defaults to latest available business_date.",
)
PT_FIXED_YIELD_OVERRIDES_PATH = Path("config/pt_fixed_yield_overrides.yaml")
COHORT_CHAIN_CODE_OPTION = typer.Option(
    "avalanche",
    "--chain-code",
    help="Chain code used for RPC contract checks and metadata",
)
COHORT_CHAIN_ID_OPTION = typer.Option(
    "43114",
    "--chain-id",
    help="EVM chain id used in RouteScan holder API path",
)
COHORT_TOKEN_SYMBOL_OPTION = typer.Option(
    "savUSD",
    "--token-symbol",
    help="Token symbol metadata for the generated cohort config",
)
COHORT_TOKEN_ADDRESS_OPTION = typer.Option(
    "0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
    "--token-address",
    help="ERC20 token address to query from RouteScan",
)
COHORT_TOKEN_DECIMALS_OPTION = typer.Option(
    18,
    "--token-decimals",
    help="Token decimals used to convert raw balances to token units",
)
COHORT_THRESHOLD_USD_OPTION = typer.Option(
    "50000",
    "--threshold-usd",
    help="Minimum balance threshold in USD",
)
COHORT_TOKEN_PRICE_USD_OPTION = typer.Option(
    "1",
    "--token-price-usd",
    help="Token price assumption used to convert USD threshold to token units",
)
COHORT_ROUTESCAN_LIMIT_OPTION = typer.Option(
    200,
    "--routescan-limit",
    help="Page size for RouteScan holders API pagination",
)
COHORT_NAME_OPTION = typer.Option(
    "savusd_avalanche_50k_users",
    "--cohort-name",
    help="Logical cohort name in generated YAML",
)
COHORT_INCLUDE_CONTRACT_WALLETS_OPTION = typer.Option(
    False,
    "--include-contract-wallets",
    help="Include contract wallets instead of excluding them as protocol wallets",
)
COHORT_OUTPUT_PATH_OPTION = typer.Option(
    Path("config/consumer_wallets_savusd_50k.yaml"),
    "--output",
    help="Output path for generated wallet cohort YAML",
)
DEBANK_MIN_LEG_USD_OPTION = typer.Option(
    "1",
    "--min-leg-usd",
    help="Minimum absolute USD value for DeBank and DB legs to include in matching",
)
DEBANK_MATCH_TOLERANCE_USD_OPTION = typer.Option(
    "1",
    "--match-tolerance-usd",
    help="Absolute USD tolerance used to mark a DeBank leg as matched",
)
DEBANK_MAX_CONCURRENCY_OPTION = typer.Option(
    6,
    "--max-concurrency",
    help="Maximum concurrent DeBank wallet requests",
)
DEBANK_MAX_WALLETS_OPTION = typer.Option(
    None,
    "--max-wallets",
    help="Optional cap on number of strategy EVM wallets to scan",
)
DEBANK_UNMATCHED_LIMIT_OPTION = typer.Option(
    25,
    "--unmatched-limit",
    help="Maximum unmatched rows to print and include in JSON output",
)
DEBANK_OUTPUT_JSON_OPTION = typer.Option(
    None,
    "--output-json",
    help="Optional path to write full audit output as JSON",
)


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
    consumer_markets_path: Path,
    *,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[SnapshotRunner, Session, list[Closeable]]:
    settings = get_settings()
    markets_config = load_markets_config(markets_path)
    consumer_markets_config = load_consumer_markets_config(consumer_markets_path)
    pt_fixed_yield_overrides = load_pt_fixed_yield_overrides_config(PT_FIXED_YIELD_OVERRIDES_PATH)

    balance_client = EvmRpcBalanceClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    aave_client = EvmRpcAaveV3Client(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    spark_client = EvmRpcSparkClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    morpho_client = EvmRpcMorphoClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    morpho_vault_yield_client = MorphoVaultYieldClient(
        timeout_seconds=settings.request_timeout_seconds
    )
    euler_client = EvmRpcEulerV2Client(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    dolomite_client = EvmRpcDolomiteClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    traderjoe_client = EvmRpcTraderJoeLpClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    stakedao_client = EvmRpcStakedaoClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    etherex_client = EvmRpcEtherexClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    yield_oracle = DefiLlamaYieldOracle(
        base_url=settings.defillama_yields_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    avant_yield_oracle = AvantYieldOracle(
        base_url=settings.avant_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    bracket_yield_oracle = BracketNavYieldOracle(
        graphql_url=settings.bracket_graphql_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    pendle_history_client = PendleHistoryClient(
        base_url=settings.pendle_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )

    wallet_adapter = WalletBalancesAdapter(
        markets_config=markets_config, balance_client=balance_client
    )
    aave_adapter = AaveV3Adapter(
        markets_config=markets_config,
        rpc_client=aave_client,
        defillama_timeout_seconds=settings.request_timeout_seconds,
        merkl_base_url=settings.merkl_base_url,
        merkl_timeout_seconds=settings.merkl_timeout_seconds,
        yield_oracle=yield_oracle,
    )
    spark_adapter = SparkAdapter(
        markets_config=markets_config,
        rpc_client=spark_client,
        defillama_timeout_seconds=settings.request_timeout_seconds,
        yield_oracle=yield_oracle,
    )
    morpho_adapter = MorphoAdapter(
        markets_config=markets_config,
        rpc_client=morpho_client,
        defillama_timeout_seconds=settings.request_timeout_seconds,
        yield_oracle=yield_oracle,
        avant_yield_oracle=avant_yield_oracle,
        bracket_yield_oracle=bracket_yield_oracle,
        vault_yield_client=morpho_vault_yield_client,
    )
    euler_adapter = EulerV2Adapter(
        markets_config=markets_config,
        rpc_client=euler_client,
        avant_yield_oracle=avant_yield_oracle,
    )
    dolomite_adapter = DolomiteAdapter(
        markets_config=markets_config,
        rpc_client=dolomite_client,
        avant_yield_oracle=avant_yield_oracle,
        yield_oracle=yield_oracle,
    )
    traderjoe_adapter = TraderJoeLpAdapter(
        markets_config=markets_config,
        rpc_client=traderjoe_client,
    )
    stakedao_adapter = StakedaoAdapter(
        markets_config=markets_config,
        rpc_client=stakedao_client,
    )
    etherex_adapter = EtherexAdapter(
        markets_config=markets_config,
        rpc_client=etherex_client,
    )
    kamino_client = KaminoApiClient(
        base_url=settings.kamino_api_base_url,
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
        include_strategy_wallets=True,
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
        pendle_history_client=pendle_history_client,
        pt_fixed_yield_overrides=pt_fixed_yield_overrides,
        progress_callback=progress_callback,
        position_adapters=[
            wallet_adapter,
            aave_adapter,
            spark_adapter,
            morpho_adapter,
            euler_adapter,
            dolomite_adapter,
            traderjoe_adapter,
            stakedao_adapter,
            etherex_adapter,
            kamino_adapter,
            zest_adapter,
            silo_adapter,
        ],
        market_adapters=[
            aave_adapter,
            spark_adapter,
            morpho_adapter,
            euler_adapter,
            dolomite_adapter,
            kamino_adapter,
            zest_adapter,
            silo_adapter,
        ],
    )
    closeables: list[Closeable] = [
        balance_client,
        aave_client,
        spark_client,
        morpho_client,
        morpho_vault_yield_client,
        euler_client,
        dolomite_client,
        traderjoe_client,
        stakedao_client,
        etherex_client,
        kamino_client,
        stacks_client,
        zest_client,
        silo_client,
        price_oracle,
        yield_oracle,
        avant_yield_oracle,
        bracket_yield_oracle,
        pendle_history_client,
    ]
    return runner, session, closeables


def _finalize_sync_command(
    *,
    session: Session,
    result: RunnerSummary,
    operation: str,
    as_of_ts_utc: datetime,
) -> None:
    session.commit()
    typer.echo(
        f"{operation} complete as_of={as_of_ts_utc.isoformat()} "
        f"rows_written={result.rows_written} issues_written={result.issues_written} "
        f"component_failures={result.component_failures}"
    )
    if result.component_failures > 0:
        raise typer.Exit(code=1)


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
    runner, session, closeables = _build_runner(
        markets_path,
        consumer_markets_path,
        progress_callback=typer.echo,
    )

    try:
        result = runner.sync_snapshot(as_of_ts_utc=as_of_ts_utc)
        _finalize_sync_command(
            session=session,
            result=result,
            operation="sync snapshot",
            as_of_ts_utc=as_of_ts_utc,
        )
    finally:
        session.close()
        for closeable in closeables:
            closeable.close()


@sync_app.command("prices")
def sync_prices(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
) -> None:
    """Sync token prices via shared price oracle."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(
        markets_path,
        consumer_markets_path,
        progress_callback=typer.echo,
    )

    try:
        result = runner.sync_prices(as_of_ts_utc=as_of_ts_utc)
        _finalize_sync_command(
            session=session,
            result=result,
            operation="sync prices",
            as_of_ts_utc=as_of_ts_utc,
        )
    finally:
        session.close()
        for closeable in closeables:
            closeable.close()


@sync_app.command("markets")
def sync_markets(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
) -> None:
    """Sync market health snapshots from configured market adapters."""

    as_of_ts_utc = _parse_as_of(as_of)
    runner, session, closeables = _build_runner(
        markets_path,
        consumer_markets_path,
        progress_callback=typer.echo,
    )

    try:
        result = runner.sync_markets(as_of_ts_utc=as_of_ts_utc)
        _finalize_sync_command(
            session=session,
            result=result,
            operation="sync markets",
            as_of_ts_utc=as_of_ts_utc,
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


@sync_app.command("debank-coverage-audit")
def sync_debank_coverage_audit(
    as_of: str | None = AS_OF_OPTION,
    markets_path: Path = MARKETS_PATH_OPTION,
    min_leg_usd: str = DEBANK_MIN_LEG_USD_OPTION,
    match_tolerance_usd: str = DEBANK_MATCH_TOLERANCE_USD_OPTION,
    max_concurrency: int = DEBANK_MAX_CONCURRENCY_OPTION,
    max_wallets: int | None = DEBANK_MAX_WALLETS_OPTION,
    unmatched_limit: int = DEBANK_UNMATCHED_LIMIT_OPTION,
    output_json: Path | None = DEBANK_OUTPUT_JSON_OPTION,
) -> None:
    """Audit DeBank leg coverage against strategy positions in the DB."""

    try:
        min_leg_usd_decimal = Decimal(min_leg_usd)
    except (InvalidOperation, ValueError) as exc:
        raise typer.BadParameter("min-leg-usd must be a valid decimal") from exc
    if min_leg_usd_decimal < 0:
        raise typer.BadParameter("min-leg-usd must be non-negative")

    try:
        match_tolerance_usd_decimal = Decimal(match_tolerance_usd)
    except (InvalidOperation, ValueError) as exc:
        raise typer.BadParameter("match-tolerance-usd must be a valid decimal") from exc
    if match_tolerance_usd_decimal < 0:
        raise typer.BadParameter("match-tolerance-usd must be non-negative")

    if max_concurrency < 1:
        raise typer.BadParameter("max-concurrency must be >= 1")
    if max_wallets is not None and max_wallets < 1:
        raise typer.BadParameter("max-wallets must be >= 1 when provided")
    if unmatched_limit < 0:
        raise typer.BadParameter("unmatched-limit must be non-negative")

    settings = get_settings()
    api_key = (settings.debank_cloud_api_key or "").strip()
    if not api_key:
        typer.echo("missing AVANT_DEBANK_CLOUD_API_KEY; unable to run DeBank coverage audit")
        raise typer.Exit(code=1)

    requested_as_of = _parse_as_of(as_of) if as_of is not None else None
    markets_config = load_markets_config(markets_path)
    session = Session(get_engine())
    client = DebankCloudClient(
        base_url=settings.debank_cloud_base_url,
        api_key=api_key,
        timeout_seconds=settings.request_timeout_seconds,
    )

    try:
        result = run_debank_coverage_audit(
            session=session,
            client=client,
            markets_config=markets_config,
            as_of_ts_utc=requested_as_of,
            min_leg_usd=min_leg_usd_decimal,
            match_tolerance_usd=match_tolerance_usd_decimal,
            max_concurrency=max_concurrency,
            max_wallets=max_wallets,
        )
    finally:
        session.close()
        client.close()

    typer.echo(f"debank coverage audit as_of={result.as_of_ts_utc.isoformat()}")
    typer.echo(
        "wallets:"
        f" total_strategy={result.wallets_total}"
        f" evm_scanned={result.wallets_scanned}"
        f" non_evm_skipped={result.non_evm_wallets_skipped}"
        f" wallet_errors={len(result.wallet_errors)}"
    )
    if result.wallet_errors:
        for wallet_error in result.wallet_errors[: min(len(result.wallet_errors), 10)]:
            typer.echo(
                "warning: wallet_error"
                f" wallet={wallet_error.wallet_address}"
                f" error={wallet_error.error_message}"
            )
        if len(result.wallet_errors) > 10:
            remaining = len(result.wallet_errors) - 10
            typer.echo(f"warning: {remaining} additional wallet errors not shown")

    if result.preflight.missing_protocol_dimensions:
        typer.echo(
            "warning: missing protocol dimensions: "
            + ",".join(result.preflight.missing_protocol_dimensions)
        )
    if result.preflight.zero_snapshot_protocols:
        typer.echo(
            "warning: zero snapshot rows at as_of for protocols: "
            + ",".join(result.preflight.zero_snapshot_protocols)
        )
    spark_count = result.preflight.snapshot_counts_by_protocol.get("spark", 0)
    typer.echo(f"preflight: spark_snapshot_rows={spark_count}")

    typer.echo(
        "coverage (all debank legs):"
        f" matched={result.totals_all.matched_legs}/{result.totals_all.total_legs}"
        f" ({result.totals_all.coverage_pct:.2f}%)"
        f" matched_usd={result.totals_all.matched_usd:.2f}/{result.totals_all.debank_total_usd:.2f}"
        f" ({result.totals_all.usd_coverage_pct:.2f}%)"
    )
    typer.echo(
        "coverage (configured surface only):"
        f" matched={result.totals_configured_surface.matched_legs}/"
        f"{result.totals_configured_surface.total_legs}"
        f" ({result.totals_configured_surface.coverage_pct:.2f}%)"
        f" matched_usd={result.totals_configured_surface.matched_usd:.2f}/"
        f"{result.totals_configured_surface.debank_total_usd:.2f}"
        f" ({result.totals_configured_surface.usd_coverage_pct:.2f}%)"
    )
    typer.echo(f"db_only_leg_count={result.db_only_leg_count}")

    if result.protocol_rows:
        typer.echo(
            "protocol\tdebank_legs\tmatched_legs\tcoverage_pct\tdebank_usd\tmatched_usd\tusd_coverage_pct"
        )
        for protocol_row in result.protocol_rows:
            typer.echo(
                f"{protocol_row.protocol_code}\t"
                f"{protocol_row.total_legs}\t"
                f"{protocol_row.matched_legs}\t"
                f"{protocol_row.coverage_pct:.2f}\t"
                f"{protocol_row.debank_total_usd:.2f}\t"
                f"{protocol_row.matched_usd:.2f}\t"
                f"{protocol_row.usd_coverage_pct:.2f}"
            )

    if result.unmatched_rows:
        typer.echo("top unmatched legs:")
        typer.echo(
            "wallet\tchain\tprotocol\tleg\ttoken\tdebank_usd\tdb_usd\tdelta_usd\tin_config_surface"
        )
        for unmatched_row in result.unmatched_rows[:unmatched_limit]:
            db_usd = f"{unmatched_row.db_usd:.2f}" if unmatched_row.db_usd is not None else ""
            delta_usd = (
                f"{unmatched_row.delta_usd:.2f}" if unmatched_row.delta_usd is not None else ""
            )
            typer.echo(
                f"{unmatched_row.key.wallet_address}\t"
                f"{unmatched_row.key.chain_code}\t"
                f"{unmatched_row.key.protocol_code}\t"
                f"{unmatched_row.key.leg_type}\t"
                f"{unmatched_row.key.token_symbol}\t"
                f"{unmatched_row.debank_usd:.2f}\t"
                f"{db_usd}\t"
                f"{delta_usd}\t"
                f"{str(unmatched_row.in_config_surface).lower()}"
            )

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        payload = serialize_audit_result(result, unmatched_limit=unmatched_limit)
        output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        typer.echo(f"wrote json report: {output_json}")


@sync_app.command("discover-dolomite-wallets")
def sync_discover_dolomite_wallets(
    markets_path: Path = MARKETS_PATH_OPTION,
    wallet_products_path: Path = WALLET_PRODUCTS_PATH_OPTION,
    chain_code: str = DOLOMITE_CHAIN_CODE_OPTION,
    wallet_groups: str = DOLOMITE_WALLET_GROUPS_OPTION,
    max_account_number: int = DOLOMITE_MAX_ACCOUNT_NUMBER_OPTION,
    min_usd: str = DOLOMITE_MIN_USD_OPTION,
    fallback_probe_max_market_id: int = DOLOMITE_FALLBACK_PROBE_MAX_MARKET_ID_OPTION,
) -> None:
    """Read-only scan to discover Dolomite wallets/accounts with meaningful balances."""

    if max_account_number < 0:
        raise typer.BadParameter("max-account-number must be non-negative")
    try:
        min_usd_decimal = Decimal(min_usd)
    except (InvalidOperation, ValueError) as exc:
        raise typer.BadParameter("min-usd must be a valid decimal number") from exc
    if min_usd_decimal < 0:
        raise typer.BadParameter("min-usd must be non-negative")
    if fallback_probe_max_market_id < 0:
        raise typer.BadParameter("fallback-probe-max-market-id must be non-negative")

    requested_groups = [token.strip() for token in wallet_groups.split(",") if token.strip()]
    if not requested_groups:
        raise typer.BadParameter("wallet-groups must contain at least one group")

    markets_config = load_markets_config(markets_path)
    chain_config = markets_config.dolomite.get(chain_code)
    if chain_config is None:
        raise typer.BadParameter(
            f"dolomite chain '{chain_code}' not found in {markets_path}",
            param_hint="--chain-code",
        )

    wallet_products = load_wallet_products_config(wallet_products_path)
    candidate_wallets, wallet_warnings = wallet_candidates_for_groups(
        wallet_products_path=wallet_products_path,
        wallet_groups=requested_groups,
        assignments=wallet_products.assignments,
    )

    if not candidate_wallets:
        for warning in wallet_warnings:
            typer.echo(f"warning: {warning}")
        typer.echo("no candidate wallets found")
        raise typer.Exit(code=1)

    settings = get_settings()
    client = EvmRpcDolomiteClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    try:
        result = discover_wallet_positions(
            chain_code=chain_code,
            chain_config=chain_config,
            wallets=candidate_wallets,
            rpc_client=client,
            max_account_number=max_account_number,
            min_abs_exposure_usd=min_usd_decimal,
            fallback_probe_max_market_id=fallback_probe_max_market_id,
        )
    finally:
        client.close()

    for warning in wallet_warnings:
        typer.echo(f"warning: {warning}")
    for warning in result.warnings:
        typer.echo(f"warning: {warning}")

    typer.echo(
        "scan summary:"
        f" chain={chain_code}"
        f" wallet_groups={','.join(requested_groups)}"
        f" candidate_wallets={len(candidate_wallets)}"
        f" markets_scanned={len(result.market_ids_scanned)}"
        f" max_account_number={max_account_number}"
        f" min_usd={min_usd_decimal}"
    )

    if not result.rows:
        typer.echo("no dolomite exposures matched the filter")
        return

    typer.echo(
        "wallet\taccount\tmarket_id\tsymbol\ttoken\tsupplied_usd\tborrowed_usd\tnet_usd\tabs_exposure_usd"
    )
    for row in result.rows:
        typer.echo(
            f"{row.wallet_address}\t"
            f"{row.account_number}\t"
            f"{row.market_id}\t"
            f"{row.symbol}\t"
            f"{row.token_address}\t"
            f"{row.supplied_usd:.2f}\t"
            f"{row.borrowed_usd:.2f}\t"
            f"{row.net_usd:.2f}\t"
            f"{row.abs_exposure_usd:.2f}"
        )


@sync_app.command("build-holder-cohort")
def sync_build_holder_cohort(
    markets_path: Path = MARKETS_PATH_OPTION,
    consumer_markets_path: Path = CONSUMER_MARKETS_PATH_OPTION,
    wallet_products_path: Path = WALLET_PRODUCTS_PATH_OPTION,
    chain_code: str = COHORT_CHAIN_CODE_OPTION,
    chain_id: str = COHORT_CHAIN_ID_OPTION,
    token_symbol: str = COHORT_TOKEN_SYMBOL_OPTION,
    token_address: str = COHORT_TOKEN_ADDRESS_OPTION,
    token_decimals: int = COHORT_TOKEN_DECIMALS_OPTION,
    threshold_usd: str = COHORT_THRESHOLD_USD_OPTION,
    token_price_usd: str = COHORT_TOKEN_PRICE_USD_OPTION,
    routescan_limit: int = COHORT_ROUTESCAN_LIMIT_OPTION,
    cohort_name: str = COHORT_NAME_OPTION,
    include_contract_wallets: bool = COHORT_INCLUDE_CONTRACT_WALLETS_OPTION,
    output: Path = COHORT_OUTPUT_PATH_OPTION,
) -> None:
    """Build user-only holder cohort config from RouteScan + local exclusions."""

    if token_decimals < 0:
        raise typer.BadParameter("token-decimals must be non-negative")
    if routescan_limit <= 0:
        raise typer.BadParameter("routescan-limit must be positive")

    try:
        threshold_usd_decimal = Decimal(threshold_usd)
    except (InvalidOperation, ValueError) as exc:
        raise typer.BadParameter("threshold-usd must be a valid decimal") from exc
    if threshold_usd_decimal < 0:
        raise typer.BadParameter("threshold-usd must be non-negative")

    try:
        token_price_usd_decimal = Decimal(token_price_usd)
    except (InvalidOperation, ValueError) as exc:
        raise typer.BadParameter("token-price-usd must be a valid decimal") from exc
    if token_price_usd_decimal <= 0:
        raise typer.BadParameter("token-price-usd must be positive")

    settings = get_settings()
    markets_config = load_markets_config(markets_path)
    wallet_products_config = load_wallet_products_config(wallet_products_path)

    routescan_client = RouteScanClient(timeout_seconds=settings.request_timeout_seconds)
    try:
        holders = routescan_client.get_erc20_holders(
            chain_id=chain_id,
            token_address=token_address,
            limit=routescan_limit,
        )
    finally:
        routescan_client.close()

    minimum_balance_raw = minimum_balance_raw_for_usd_threshold(
        threshold_usd=threshold_usd_decimal,
        token_price_usd=token_price_usd_decimal,
        token_decimals=token_decimals,
    )
    strategy_wallets = collect_strategy_wallets(
        markets_config=markets_config,
        wallet_products_config=wallet_products_config,
    )
    protocol_wallets = collect_evm_addresses_from_yaml(
        [markets_path, consumer_markets_path, wallet_products_path]
    )

    pre_contract_result = build_customer_wallet_cohort(
        holders=holders,
        minimum_balance_raw=minimum_balance_raw,
        strategy_wallets=strategy_wallets,
        protocol_wallets=protocol_wallets,
        contract_wallets=set(),
    )
    contract_wallets: set[str] = set()
    if not include_contract_wallets:
        rpc_url = settings.evm_rpc_urls.get(chain_code)
        if not rpc_url:
            raise typer.BadParameter(
                f"missing AVANT_EVM_RPC_URLS entry for chain '{chain_code}'",
                param_hint="--chain-code",
            )
        contract_wallets = rpc_contract_addresses(
            rpc_url=rpc_url,
            addresses=[wallet.address for wallet in pre_contract_result.wallets],
            timeout_seconds=settings.request_timeout_seconds,
        )

    result = build_customer_wallet_cohort(
        holders=holders,
        minimum_balance_raw=minimum_balance_raw,
        strategy_wallets=strategy_wallets,
        protocol_wallets=protocol_wallets,
        contract_wallets=contract_wallets,
    )

    source_url = (
        f"{ROUTESCAN_BASE_URL}/v2/network/mainnet/evm/{chain_id}/erc20/"
        f"{canonical_address(token_address)}/holders"
    )
    payload = build_wallet_cohort_config_payload(
        cohort_name=cohort_name,
        chain_code=chain_code,
        chain_id=chain_id,
        token_symbol=token_symbol,
        token_address=token_address,
        token_decimals=token_decimals,
        threshold_usd=threshold_usd_decimal,
        token_price_usd=token_price_usd_decimal,
        minimum_balance_raw=minimum_balance_raw,
        source_url=source_url,
        result=result,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    typer.echo(
        f"build holder cohort complete chain={chain_code} chain_id={chain_id}"
        f" token={token_symbol} fetched_rows={result.fetched_rows}"
        f" threshold_rows={result.threshold_rows} strategy_excluded={result.strategy_excluded}"
        f" protocol_excluded={result.protocol_excluded}"
        f" contract_excluded={result.contract_excluded}"
        f" wallets={len(result.wallets)} output={output}"
    )


@compute_app.command("daily")
def compute_daily(
    target_date: str = DATE_OPTION,
    boundary_policy: str = DAILY_BOUNDARY_POLICY_OPTION,
) -> None:
    """Compute daily yield + fees + rollups for a Denver business date."""

    try:
        parsed_date = date.fromisoformat(target_date)
    except ValueError as exc:
        raise typer.BadParameter("date must be formatted as YYYY-MM-DD") from exc

    session = Session(get_engine())
    gross_roe_total: str | Decimal = "none"
    post_strategy_fee_roe_total: str | Decimal = "none"
    net_roe_total: str | Decimal = "none"
    avant_gop_roe_total: str | Decimal = "none"
    try:
        try:
            summary = YieldEngine(
                session,
                boundary_policy=boundary_policy,
            ).compute_daily(business_date=parsed_date)
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--boundary-policy") from exc
        portfolio_summary = PortfolioViewEngine(session).compute_daily(
            business_date=parsed_date,
            as_of_ts_utc=summary.eod_ts_utc or summary.sod_ts_utc,
        )
        executive_summary = ExecutiveSummaryEngine(session).compute_daily(business_date=parsed_date)
        total_row = session.execute(
            select(
                YieldDaily.gross_roe,
                YieldDaily.post_strategy_fee_roe,
                YieldDaily.net_roe,
                YieldDaily.avant_gop_roe,
            ).where(
                YieldDaily.business_date == parsed_date,
                YieldDaily.method == "apy_prorated_sod_eod",
                YieldDaily.position_key.is_(None),
                YieldDaily.wallet_id.is_(None),
                YieldDaily.product_id.is_(None),
                YieldDaily.protocol_id.is_(None),
            )
        ).one_or_none()
        if total_row is not None:
            gross_roe_total = total_row[0] if total_row[0] is not None else "none"
            post_strategy_fee_roe_total = total_row[1] if total_row[1] is not None else "none"
            net_roe_total = total_row[2] if total_row[2] is not None else "none"
            avant_gop_roe_total = total_row[3] if total_row[3] is not None else "none"
        session.commit()
    finally:
        session.close()

    typer.echo(
        "compute daily complete"
        f" business_date={summary.business_date.isoformat()}"
        f" sod_ts_utc={summary.sod_ts_utc.isoformat() if summary.sod_ts_utc else 'none'}"
        f" eod_ts_utc={summary.eod_ts_utc.isoformat() if summary.eod_ts_utc else 'none'}"
        f" position_rows={summary.position_rows_written}"
        f" rollup_rows={summary.rollup_rows_written}"
        f" issues_written={summary.issues_written}"
        f" gross_roe_total={gross_roe_total}"
        f" post_strategy_fee_roe_total={post_strategy_fee_roe_total}"
        f" net_roe_total={net_roe_total}"
        f" avant_gop_roe_total={avant_gop_roe_total}"
        f" portfolio_daily_rows={portfolio_summary.daily_rows_written}"
        f" portfolio_current_rows={portfolio_summary.current_rows_written}"
        f" executive_rows={executive_summary.rows_written}"
    )


@compute_app.command("markets")
def compute_markets(
    target_date: str = DATE_OPTION,
    thresholds_path: Path = RISK_THRESHOLDS_PATH_OPTION,
) -> None:
    """Compute daily market overview metrics from canonical snapshots."""

    try:
        parsed_date = date.fromisoformat(target_date)
    except ValueError as exc:
        raise typer.BadParameter("date must be formatted as YYYY-MM-DD") from exc

    try:
        thresholds = load_risk_thresholds_config(thresholds_path)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--thresholds-path") from exc

    session = Session(get_engine())
    try:
        summary = MarketEngine(session).compute_daily(business_date=parsed_date)
        market_view_summary = MarketViewEngine(session, thresholds=thresholds).compute_daily(
            business_date=parsed_date
        )
        executive_summary = ExecutiveSummaryEngine(session).compute_daily(business_date=parsed_date)
        session.commit()
    finally:
        session.close()

    typer.echo(
        "compute markets complete"
        f" business_date={summary.business_date.isoformat()}"
        f" as_of_ts_utc={summary.as_of_ts_utc.isoformat()}"
        f" rows_written={summary.rows_written}"
        f" health_rows={market_view_summary.health_rows_written}"
        f" exposure_rows={market_view_summary.exposure_rows_written}"
        f" executive_rows={executive_summary.rows_written}"
    )


@compute_app.command("boundary-check")
def compute_boundary_check(target_date: str = DATE_OPTION) -> None:
    """Check exact Denver SOD/EOD snapshot availability for metric signoff."""

    try:
        parsed_date = date.fromisoformat(target_date)
    except ValueError as exc:
        raise typer.BadParameter("date must be formatted as YYYY-MM-DD") from exc

    session = Session(get_engine())
    try:
        result = select_business_day_boundaries(session, business_date=parsed_date)
    finally:
        session.close()

    status = "pass" if result.ready_for_signoff else "warn"
    typer.echo(
        "compute boundary-check"
        f" status={status}"
        f" business_date={result.business_date.isoformat()}"
        f" sod_exact_ts_utc={result.sod_exact_ts_utc.isoformat()}"
        f" eod_exact_ts_utc={result.eod_exact_ts_utc.isoformat()}"
        f" exact_sod_present={result.exact_sod_present}"
        f" exact_eod_present={result.exact_eod_present}"
        f" selected_sod_ts_utc={result.sod_ts_utc.isoformat() if result.sod_ts_utc else 'none'}"
        f" selected_eod_ts_utc={result.eod_ts_utc.isoformat() if result.eod_ts_utc else 'none'}"
        f" used_sod_fallback={result.used_sod_fallback}"
        f" used_eod_fallback={result.used_eod_fallback}"
    )


@compute_app.command("risk")
def compute_risk(
    target_date: str | None = RISK_DATE_OPTION,
    as_of: str | None = AS_OF_OPTION,
    thresholds_path: Path = RISK_THRESHOLDS_PATH_OPTION,
) -> None:
    """Compute risk signals and synchronize alerts from canonical snapshots."""

    if (target_date is None) == (as_of is None):
        raise typer.BadParameter("provide exactly one of --date or --as-of")

    try:
        thresholds = load_risk_thresholds_config(thresholds_path)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--thresholds-path") from exc
    parsed_date: date | None = None
    requested_as_of: datetime | None = None

    if target_date is not None:
        try:
            parsed_date = date.fromisoformat(target_date)
        except ValueError as exc:
            raise typer.BadParameter("date must be formatted as YYYY-MM-DD") from exc
    else:
        requested_as_of = _parse_as_of(as_of)

    session = Session(get_engine())
    market_rows = 0
    position_rows = 0
    top_market_id: str = "none"
    top_position_key: str = "none"
    alerts_opened = 0
    alerts_updated = 0
    alerts_resolved = 0
    open_alerts = 0
    selected_as_of: datetime | None = None
    try:
        risk_engine = RiskEngine(session, thresholds=thresholds)
        try:
            if parsed_date is not None:
                risk_result = risk_engine.compute_for_date(business_date=parsed_date)
            else:
                assert requested_as_of is not None
                risk_result = risk_engine.compute_as_of(as_of_ts_utc=requested_as_of)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

        selected_as_of = risk_result.as_of_ts_utc
        market_rows = len(risk_result.market_rows)
        position_rows = len(risk_result.position_rows)

        market_watchlist = top_markets_by_kink_risk(risk_result, limit=10)
        if market_watchlist:
            top_market_id = str(market_watchlist[0].market_id)

        position_watchlist = top_positions_by_worst_net_spread(risk_result, limit=10)
        if position_watchlist:
            top_position_key = position_watchlist[0].position_key

        alert_engine = AlertEngine(session, thresholds=thresholds)
        alert_candidates = alert_engine.build_candidates(risk_result)
        alert_summary = alert_engine.sync_candidates(
            as_of_ts_utc=risk_result.as_of_ts_utc,
            candidates=alert_candidates,
        )

        alerts_opened = alert_summary.opened
        alerts_updated = alert_summary.updated
        alerts_resolved = alert_summary.resolved
        open_alerts = alert_summary.open_alerts
        session.commit()
    finally:
        session.close()

    typer.echo(
        "compute risk complete"
        f" as_of_ts_utc={selected_as_of.isoformat() if selected_as_of else 'none'}"
        f" market_rows={market_rows}"
        f" position_rows={position_rows}"
        f" alerts_opened={alerts_opened}"
        f" alerts_updated={alerts_updated}"
        f" alerts_resolved={alerts_resolved}"
        f" open_alerts={open_alerts}"
        f" top_market_id={top_market_id}"
        f" top_position_key={top_position_key}"
    )


@compute_app.command("capital-buckets")
def compute_capital_buckets(as_of: str | None = AS_OF_OPTION) -> None:
    """Summarize latest snapshot capital by bucket (strategy/ops/pending deployment)."""

    session = Session(get_engine())
    try:
        if as_of is None:
            as_of_ts_utc = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
            if as_of_ts_utc is None:
                raise typer.BadParameter("position_snapshots table is empty")
        else:
            requested = _parse_as_of(as_of)
            as_of_ts_utc = session.scalar(
                select(func.max(PositionSnapshot.as_of_ts_utc)).where(
                    PositionSnapshot.as_of_ts_utc <= requested
                )
            )
            if as_of_ts_utc is None:
                raise typer.BadParameter(
                    f"no position snapshots found at or before {requested.isoformat()}"
                )

        economic_supply_usd = case(
            (
                (PositionSnapshot.collateral_usd.is_not(None))
                & (PositionSnapshot.collateral_usd > Decimal("0")),
                PositionSnapshot.collateral_usd,
            ),
            else_=PositionSnapshot.supplied_usd,
        )
        rows = session.execute(
            select(
                economic_supply_usd.label("supplied_usd"),
                PositionSnapshot.borrowed_usd,
                PositionSnapshot.equity_usd,
                Market.metadata_json,
            )
            .join(Market, Market.market_id == PositionSnapshot.market_id)
            .where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()
    finally:
        session.close()

    totals_by_bucket: dict[str, dict[str, Decimal | int]] = defaultdict(
        lambda: {
            "supplied_usd": Decimal("0"),
            "borrowed_usd": Decimal("0"),
            "equity_usd": Decimal("0"),
            "positions": 0,
        }
    )
    for supplied_usd, borrowed_usd, equity_usd, metadata_json in rows:
        bucket = "strategy_deployed"
        if isinstance(metadata_json, dict):
            raw_bucket = metadata_json.get("capital_bucket")
            if isinstance(raw_bucket, str) and raw_bucket.strip():
                bucket = raw_bucket.strip()

        bucket_totals = totals_by_bucket[bucket]
        bucket_totals["supplied_usd"] = Decimal(bucket_totals["supplied_usd"]) + supplied_usd
        bucket_totals["borrowed_usd"] = Decimal(bucket_totals["borrowed_usd"]) + borrowed_usd
        bucket_totals["equity_usd"] = Decimal(bucket_totals["equity_usd"]) + equity_usd
        bucket_totals["positions"] = int(bucket_totals["positions"]) + 1

    for bucket in ("strategy_deployed", "pending_deployment", "market_stability_ops"):
        totals_by_bucket.setdefault(
            bucket,
            {
                "supplied_usd": Decimal("0"),
                "borrowed_usd": Decimal("0"),
                "equity_usd": Decimal("0"),
                "positions": 0,
            },
        )

    typer.echo(f"compute capital-buckets as_of={as_of_ts_utc.isoformat()}")
    for bucket in sorted(totals_by_bucket):
        bucket_totals = totals_by_bucket[bucket]
        typer.echo(
            f"{bucket}\tpositions={bucket_totals['positions']}"
            f"\tsupplied_usd={Decimal(bucket_totals['supplied_usd']):.2f}"
            f"\tborrowed_usd={Decimal(bucket_totals['borrowed_usd']):.2f}"
            f"\tequity_usd={Decimal(bucket_totals['equity_usd']):.2f}"
        )


@compute_app.command("rollups")
def compute_rollups(window: str = WINDOW_OPTION, end_date: str | None = END_DATE_OPTION) -> None:
    """Compute trailing 7d/30d rollups from persisted daily position rows."""

    parsed_end_date: date | None = None
    if end_date is not None:
        try:
            parsed_end_date = date.fromisoformat(end_date)
        except ValueError as exc:
            raise typer.BadParameter("end-date must be formatted as YYYY-MM-DD") from exc

    session = Session(get_engine())
    try:
        try:
            result = compute_window_rollups(session, window=window, end_date=parsed_end_date)
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--window") from exc
    finally:
        session.close()

    if result.end_date is None or result.start_date is None:
        typer.echo(f"compute rollups window={window} has no daily rows to aggregate")
        return

    gross_roe_total = result.total.gross_roe if result.total.gross_roe is not None else "none"
    post_strategy_fee_roe_total = (
        result.total.post_strategy_fee_roe
        if result.total.post_strategy_fee_roe is not None
        else "none"
    )
    net_roe_total = result.total.net_roe if result.total.net_roe is not None else "none"
    avant_gop_roe_total = (
        result.total.avant_gop_roe if result.total.avant_gop_roe is not None else "none"
    )

    typer.echo(
        "compute rollups complete"
        f" window={window}"
        f" start_date={result.start_date.isoformat()}"
        f" end_date={result.end_date.isoformat()}"
        f" wallet_rows={len(result.wallet_rollups)}"
        f" product_rows={len(result.product_rollups)}"
        f" protocol_rows={len(result.protocol_rollups)}"
        f" gross_total={result.total.gross_yield_usd}"
        f" strategy_fee_total={result.total.strategy_fee_usd}"
        f" avant_gop_total={result.total.avant_gop_usd}"
        f" net_total={result.total.net_yield_usd}"
        f" avg_equity_total={result.total.avg_equity_usd}"
        f" gross_roe_total={gross_roe_total}"
        f" post_strategy_fee_roe_total={post_strategy_fee_roe_total}"
        f" net_roe_total={net_roe_total}"
        f" avant_gop_roe_total={avant_gop_roe_total}"
    )


def main() -> None:
    """Run the CLI application."""

    app()


if __name__ == "__main__":
    main()
