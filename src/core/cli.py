"""CLI entrypoints for config inspection and ingestion workflows."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

import typer
import yaml
from sqlalchemy.orm import Session

from adapters.aave_v3 import AaveV3Adapter, EvmRpcAaveV3Client
from adapters.dolomite import DolomiteAdapter, EvmRpcDolomiteClient
from adapters.euler_v2 import EulerV2Adapter, EvmRpcEulerV2Client
from adapters.kamino import KaminoAdapter, KaminoApiClient
from adapters.morpho import EvmRpcMorphoClient, MorphoAdapter
from adapters.silo_v2 import SiloApiClient, SiloV2Adapter
from adapters.wallet_balances import EvmRpcBalanceClient, WalletBalancesAdapter
from adapters.zest import StacksApiZestClient, ZestAdapter
from core.config import (
    canonical_address,
    load_consumer_markets_config,
    load_markets_config,
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
from core.db.session import get_engine
from core.dolomite_discovery import discover_wallet_positions, wallet_candidates_for_groups
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
) -> tuple[SnapshotRunner, Session, list[Closeable]]:
    settings = get_settings()
    markets_config = load_markets_config(markets_path)
    consumer_markets_config = load_consumer_markets_config(consumer_markets_path)

    balance_client = EvmRpcBalanceClient(
        rpc_urls=settings.evm_rpc_urls,
        timeout_seconds=settings.request_timeout_seconds,
    )
    aave_client = EvmRpcAaveV3Client(
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
    yield_oracle = DefiLlamaYieldOracle(
        base_url=settings.defillama_yields_base_url,
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
    morpho_adapter = MorphoAdapter(
        markets_config=markets_config,
        rpc_client=morpho_client,
        defillama_timeout_seconds=settings.request_timeout_seconds,
        yield_oracle=yield_oracle,
    )
    euler_adapter = EulerV2Adapter(markets_config=markets_config, rpc_client=euler_client)
    dolomite_adapter = DolomiteAdapter(markets_config=markets_config, rpc_client=dolomite_client)
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
            aave_adapter,
            morpho_adapter,
            euler_adapter,
            dolomite_adapter,
            kamino_adapter,
            zest_adapter,
            silo_adapter,
        ],
        market_adapters=[
            aave_adapter,
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
        morpho_client,
        euler_client,
        dolomite_client,
        kamino_client,
        stacks_client,
        zest_client,
        silo_client,
        price_oracle,
        yield_oracle,
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
