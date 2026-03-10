"""Holder-ledger and DeBank token-leg sync for supply-coverage scorecards."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import core.debank_coverage as debank_coverage
from analytics.yield_engine import denver_business_bounds_utc
from core.config import (
    AvantTokensConfig,
    ConsumerMarketsConfig,
    ConsumerThresholdsConfig,
    HolderExclusion,
    HolderExclusionsConfig,
    MarketsConfig,
    WalletProductsConfig,
    canonical_address,
)
from core.consumer_debank_visibility import _configured_surface
from core.customer_cohort import (
    DEFAULT_CONSUMER_CHAIN_IDS,
    RouteScanClient,
    active_avant_tokens,
    collect_evm_addresses_from_yaml,
    collect_strategy_wallets,
    fetch_consumer_price_map,
    upsert_customer_wallets,
)
from core.db.models import (
    ConsumerDebankTokenDaily,
    ConsumerTokenHolderDaily,
    DataQuality,
    Wallet,
)
from core.debank_cloud import DebankCloudClient
from core.pricing import PriceOracle
from core.types import DataQualityIssue

ZERO = Decimal("0")


@dataclass(frozen=True)
class HolderSupplyTarget:
    chain_code: str
    chain_id: str
    token_symbol: str
    token_address: str
    token_decimals: int


@dataclass(frozen=True)
class HolderSupplySyncSummary:
    business_date: date
    as_of_ts_utc: datetime
    chain_code: str
    token_symbol: str
    raw_holder_rows: int
    monitoring_wallet_count: int
    holder_rows_written: int
    debank_token_rows_written: int
    debank_wallets_scanned: int
    issues_written: int


@dataclass(frozen=True)
class HolderSupplyClassification:
    holder_class: str
    exclude_from_monitoring: bool
    exclude_from_customer_float: bool


def resolve_supply_coverage_target(
    *,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    business_date: date,
) -> HolderSupplyTarget:
    """Resolve the configured primary supply-coverage token from the token registry."""

    target_chain_code = thresholds.supply_coverage.primary_chain_code.strip().lower()
    target_symbol = thresholds.supply_coverage.primary_token_symbol.strip()
    chain_id = DEFAULT_CONSUMER_CHAIN_IDS.get(target_chain_code)
    if chain_id is None:
        raise ValueError(f"unsupported supply coverage chain '{target_chain_code}'")

    active_tokens = active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope={target_chain_code},
    )
    for token in active_tokens:
        if token.chain_code == target_chain_code and token.symbol == target_symbol:
            return HolderSupplyTarget(
                chain_code=token.chain_code,
                chain_id=chain_id,
                token_symbol=token.symbol,
                token_address=canonical_address(token.token_address),
                token_decimals=token.decimals,
            )

    raise ValueError(
        "configured supply coverage token is not active in avant_tokens.yaml: "
        f"chain={target_chain_code} symbol={target_symbol}"
    )


def _holder_exclusion_map(
    exclusions: HolderExclusionsConfig,
) -> dict[tuple[str | None, str], HolderExclusion]:
    return {
        (exclusion.chain_code, canonical_address(exclusion.address)): exclusion
        for exclusion in exclusions.exclusions
    }


def _strategy_internal_maps(session: Session) -> tuple[set[str], set[str]]:
    rows = session.execute(select(Wallet.address, Wallet.wallet_type)).all()
    strategy_wallets = {
        canonical_address(address)
        for address, wallet_type in rows
        if wallet_type == "strategy" and isinstance(address, str)
    }
    internal_wallets = {
        canonical_address(address)
        for address, wallet_type in rows
        if wallet_type == "internal" and isinstance(address, str)
    }
    return strategy_wallets, internal_wallets


def classify_holder_address(
    *,
    wallet_address: str,
    chain_code: str,
    strategy_wallets: set[str],
    internal_wallets: set[str],
    protocol_wallets: set[str],
    exclusions: dict[tuple[str | None, str], HolderExclusion],
) -> HolderSupplyClassification:
    """Classify a holder-ledger address for monitoring and customer-float rules."""

    normalized = canonical_address(wallet_address)
    if normalized in strategy_wallets:
        return HolderSupplyClassification(
            holder_class="strategy",
            exclude_from_monitoring=True,
            exclude_from_customer_float=True,
        )
    if normalized in internal_wallets:
        return HolderSupplyClassification(
            holder_class="internal",
            exclude_from_monitoring=True,
            exclude_from_customer_float=True,
        )

    explicit = exclusions.get((chain_code, normalized)) or exclusions.get((None, normalized))
    if explicit is not None:
        return HolderSupplyClassification(
            holder_class=explicit.classification,
            exclude_from_monitoring=explicit.exclude_from_monitoring,
            exclude_from_customer_float=explicit.exclude_from_customer_float,
        )

    if normalized in protocol_wallets:
        return HolderSupplyClassification(
            holder_class="protocol",
            exclude_from_monitoring=True,
            exclude_from_customer_float=False,
        )

    return HolderSupplyClassification(
        holder_class="customer",
        exclude_from_monitoring=False,
        exclude_from_customer_float=False,
    )


def sync_holder_supply_inputs(
    *,
    session: Session,
    business_date: date,
    routescan_client: RouteScanClient,
    debank_client: DebankCloudClient,
    price_oracle: PriceOracle,
    markets_config: MarketsConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    wallet_products_config: WalletProductsConfig,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    holder_exclusions: HolderExclusionsConfig,
    markets_path: Path = Path("config/markets.yaml"),
    consumer_markets_path: Path = Path("config/consumer_markets.yaml"),
    wallet_products_path: Path = Path("config/wallet_products.yaml"),
    avant_tokens_path: Path = Path("config/avant_tokens.yaml"),
    min_leg_usd: Decimal = Decimal("1"),
    max_concurrency: int = 8,
) -> HolderSupplySyncSummary:
    """Sync raw holder-ledger rows and token-level DeBank legs for the primary coverage token."""

    if min_leg_usd < 0:
        raise ValueError("min_leg_usd must be non-negative")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")

    target = resolve_supply_coverage_target(
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        business_date=business_date,
    )
    _start_utc, as_of_ts_utc = denver_business_bounds_utc(business_date)
    strategy_wallets = collect_strategy_wallets(
        markets_config=markets_config,
        wallet_products_config=wallet_products_config,
    )
    db_strategy_wallets, internal_wallets = _strategy_internal_maps(session)
    strategy_wallets |= db_strategy_wallets
    protocol_wallets = collect_evm_addresses_from_yaml(
        [markets_path, consumer_markets_path, wallet_products_path, avant_tokens_path]
    )
    exclusion_map = _holder_exclusion_map(holder_exclusions)

    price_map, issues = fetch_consumer_price_map(
        session=session,
        as_of_ts_utc=as_of_ts_utc,
        avant_tokens=avant_tokens,
        business_date=business_date,
        price_oracle=price_oracle,
    )
    target_price = price_map.get((target.chain_code, target.token_address))
    if target_price is None:
        raise ValueError(
            "missing price for supply coverage token "
            f"chain={target.chain_code} address={target.token_address}"
        )

    holders = routescan_client.get_erc20_holders(
        chain_id=target.chain_id,
        token_address=target.token_address,
        limit=200,
    )
    wallet_ids = upsert_customer_wallets(session, [holder.address for holder in holders])
    scale = Decimal(10) ** target.token_decimals

    holder_rows: list[dict[str, object]] = []
    monitoring_wallets: list[str] = []
    for holder in holders:
        wallet_address = canonical_address(holder.address)
        classification = classify_holder_address(
            wallet_address=wallet_address,
            chain_code=target.chain_code,
            strategy_wallets=strategy_wallets,
            internal_wallets=internal_wallets,
            protocol_wallets=protocol_wallets,
            exclusions=exclusion_map,
        )
        balance_tokens = Decimal(holder.balance_raw) / scale
        usd_value = balance_tokens * target_price
        holder_rows.append(
            {
                "business_date": business_date,
                "as_of_ts_utc": as_of_ts_utc,
                "chain_code": target.chain_code,
                "token_symbol": target.token_symbol,
                "token_address": target.token_address,
                "wallet_id": wallet_ids[wallet_address],
                "wallet_address": wallet_address,
                "balance_tokens": balance_tokens,
                "usd_value": usd_value,
                "holder_class": classification.holder_class,
                "exclude_from_monitoring": classification.exclude_from_monitoring,
                "exclude_from_customer_float": classification.exclude_from_customer_float,
                "source_provider": "routescan",
            }
        )
        if not classification.exclude_from_monitoring:
            monitoring_wallets.append(wallet_address)

    monitoring_wallet_set = sorted(set(monitoring_wallets))
    configured_chains, configured_protocols = _configured_surface(consumer_markets_config)
    debank_rows: list[dict[str, object]] = []

    def _scan_wallet(
        wallet_address: str,
    ) -> tuple[str, list[dict[str, object]], list[dict[str, object]], str | None]:
        try:
            protocol_payload = debank_client.get_user_complex_protocols(wallet_address)
            token_payload = debank_client.get_user_all_tokens(wallet_address)
        except Exception as exc:  # pragma: no cover - network failures
            return wallet_address, [], [], str(exc)
        return wallet_address, protocol_payload, token_payload, None

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [
            pool.submit(_scan_wallet, wallet_address) for wallet_address in monitoring_wallet_set
        ]
        for future in as_completed(futures):
            wallet_address, protocol_payload, token_payload, fetch_error = future.result()
            wallet_id = wallet_ids[wallet_address]
            if fetch_error is not None:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_holder_supply_inputs",
                        error_type="debank_wallet_fetch_failed",
                        error_message=fetch_error,
                        wallet_address=wallet_address,
                        chain_code=target.chain_code,
                        market_ref=target.token_address,
                        payload_json={"token_symbol": target.token_symbol},
                    )
                )
                continue

            legs, in_scope = debank_coverage._flatten_debank_payload_legs(
                wallet_address=wallet_address,
                payload=protocol_payload,
                configured_chains=configured_chains,
                configured_protocols=configured_protocols,
                min_leg_usd=min_leg_usd,
            )
            aggregated_rows: dict[tuple[str, str, str], Decimal] = defaultdict(lambda: ZERO)
            aggregated_scope: dict[tuple[str, str, str], bool] = {}
            normalized_target_symbol = debank_coverage.normalize_token_symbol(target.token_symbol)
            for key, usd_value in legs.items():
                if key.token_symbol != normalized_target_symbol:
                    continue
                row_key = (key.chain_code, key.protocol_code, key.leg_type)
                aggregated_rows[row_key] += usd_value
                aggregated_scope[row_key] = aggregated_scope.get(row_key, False) or in_scope.get(
                    key, False
                )

            normalized_target_symbol = debank_coverage.normalize_token_symbol(target.token_symbol)
            for token_row in token_payload:
                token_symbol = None
                for candidate_key in ("optimized_symbol", "display_symbol", "symbol", "name"):
                    candidate = token_row.get(candidate_key)
                    if isinstance(candidate, str) and candidate.strip():
                        token_symbol = debank_coverage.normalize_token_symbol(candidate)
                        break
                if token_symbol != normalized_target_symbol:
                    continue

                if token_row.get("is_wallet") is False:
                    continue

                chain_id = token_row.get("chain")
                if not isinstance(chain_id, str) or not chain_id.strip():
                    continue
                chain_code = debank_coverage.normalize_chain_code(chain_id)
                if chain_code == target.chain_code:
                    continue

                amount = debank_coverage._to_decimal(token_row.get("amount"))
                price = debank_coverage._to_decimal(token_row.get("price"))
                usd_value = debank_coverage._to_decimal(token_row.get("price_usd"))
                if usd_value is None and amount is not None and price is not None:
                    usd_value = amount * price
                if usd_value is None or usd_value < min_leg_usd:
                    continue

                row_key = (chain_code, "wallet_balance", "wallet")
                aggregated_rows[row_key] += usd_value
                aggregated_scope[row_key] = False

            for (chain_code, protocol_code, leg_type), usd_value in aggregated_rows.items():
                debank_rows.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "chain_code": chain_code,
                        "protocol_code": protocol_code,
                        "token_symbol": target.token_symbol,
                        "leg_type": leg_type,
                        "in_config_surface": aggregated_scope.get(
                            (chain_code, protocol_code, leg_type),
                            False,
                        ),
                        "usd_value": usd_value,
                    }
                )

    session.execute(
        delete(ConsumerTokenHolderDaily).where(
            ConsumerTokenHolderDaily.business_date == business_date,
            ConsumerTokenHolderDaily.chain_code == target.chain_code,
            ConsumerTokenHolderDaily.token_symbol == target.token_symbol,
        )
    )
    session.execute(
        delete(ConsumerDebankTokenDaily).where(
            ConsumerDebankTokenDaily.business_date == business_date,
            ConsumerDebankTokenDaily.token_symbol == target.token_symbol,
        )
    )
    if holder_rows:
        session.execute(insert(ConsumerTokenHolderDaily).values(holder_rows))
    if debank_rows:
        session.execute(insert(ConsumerDebankTokenDaily).values(debank_rows))

    issues_written = 0
    if issues:
        stmt = insert(DataQuality).values(
            [
                {
                    "as_of_ts_utc": issue.as_of_ts_utc,
                    "stage": issue.stage,
                    "protocol_code": issue.protocol_code,
                    "chain_code": issue.chain_code,
                    "wallet_address": issue.wallet_address,
                    "market_ref": issue.market_ref,
                    "error_type": issue.error_type,
                    "error_message": issue.error_message,
                    "payload_json": issue.payload_json,
                }
                for issue in issues
            ]
        )
        session.execute(stmt)
        issues_written = len(issues)

    return HolderSupplySyncSummary(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        chain_code=target.chain_code,
        token_symbol=target.token_symbol,
        raw_holder_rows=len(holder_rows),
        monitoring_wallet_count=len(monitoring_wallet_set),
        holder_rows_written=len(holder_rows),
        debank_token_rows_written=len(debank_rows),
        debank_wallets_scanned=len(monitoring_wallet_set),
        issues_written=issues_written,
    )
