"""Consumer wallet DeBank visibility snapshots built from seed + discovered cohorts."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import yaml
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import core.debank_coverage as debank_coverage
from analytics.yield_engine import denver_business_bounds_utc
from core.config import ConsumerMarketsConfig, canonical_address
from core.db.models import (
    ConsumerHolderUniverseDaily,
    ConsumerDebankProtocolDaily,
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    DataQuality,
    Wallet,
)
from core.debank_cloud import DebankCloudClient
from core.types import DataQualityIssue

ZERO = Decimal("0")
EXCLUDED_VISIBILITY_PROTOCOL_CODES = frozenset(
    {
        "avantprotocol",
        "avax_avantprotocol",
    }
)


def is_excluded_visibility_protocol(protocol_code: str | None) -> bool:
    """Return whether a DeBank protocol code should be excluded from visibility analytics."""

    if protocol_code is None:
        return False
    return protocol_code in EXCLUDED_VISIBILITY_PROTOCOL_CODES


@dataclass(frozen=True)
class ConsumerVisibilityWalletScope:
    """Wallet universe row for consumer DeBank visibility sync."""

    wallet_address: str
    seed_sources: tuple[str, ...]
    discovery_sources: tuple[str, ...]
    in_seed_set: bool
    in_verified_cohort: bool
    in_signoff_cohort: bool


@dataclass(frozen=True)
class ConsumerDebankVisibilitySyncSummary:
    """CLI-facing summary for one consumer DeBank visibility sync."""

    business_date: date
    as_of_ts_utc: datetime
    union_wallet_count: int
    seed_wallet_count: int
    verified_cohort_wallet_count: int
    signoff_cohort_wallet_count: int
    new_discovered_not_in_seed_count: int
    fetched_wallet_count: int
    fetch_error_count: int
    active_wallet_count: int
    borrow_wallet_count: int
    configured_surface_wallet_count: int
    wallet_rows_written: int
    protocol_rows_written: int
    issues_written: int


def load_consumer_seed_wallet_sources(config_dir: Path) -> dict[str, set[str]]:
    """Load legacy consumer wallet seeds keyed by wallet with source file provenance."""

    wallet_sources: dict[str, set[str]] = defaultdict(set)
    for path in sorted(config_dir.glob("consumer_wallets_*.yaml")):
        source_name = path.name
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            continue
        cohort = payload.get("cohort")
        if not isinstance(cohort, dict):
            continue

        def _record(address: object, *, source: str) -> None:
            if not isinstance(address, str):
                return
            normalized = canonical_address(address)
            if debank_coverage.is_evm_address(normalized):
                wallet_sources[normalized].add(source)

        for item in cohort.get("wallet_addresses") or []:
            _record(item, source=source_name)
        for row in cohort.get("wallets") or []:
            if isinstance(row, dict):
                _record(row.get("address"), source=source_name)

    return {wallet: sources for wallet, sources in wallet_sources.items()}


def merge_consumer_visibility_wallet_scopes(
    *,
    seed_wallet_sources: dict[str, set[str]],
    cohort_rows: Iterable[ConsumerHolderUniverseDaily],
) -> dict[str, ConsumerVisibilityWalletScope]:
    """Merge seed and verified-cohort wallets into one visibility universe."""

    merged: dict[str, ConsumerVisibilityWalletScope] = {}
    cohort_by_wallet: dict[str, ConsumerHolderUniverseDaily] = {
        canonical_address(row.wallet_address): row for row in cohort_rows
    }
    all_wallets = sorted(set(seed_wallet_sources) | set(cohort_by_wallet))

    for wallet_address in all_wallets:
        cohort_row = cohort_by_wallet.get(wallet_address)
        discovery_sources: tuple[str, ...] = ()
        in_verified_cohort = False
        in_signoff_cohort = False
        if cohort_row is not None:
            in_verified_cohort = True
            in_signoff_cohort = bool(cohort_row.is_signoff_eligible)
            payload = cohort_row.discovery_sources_json or {}
            if isinstance(payload, dict):
                source_values = payload.get("sources") or []
                if isinstance(source_values, list):
                    discovery_sources = tuple(
                        sorted(str(item) for item in source_values if isinstance(item, str))
                    )

        seed_sources = tuple(sorted(seed_wallet_sources.get(wallet_address, set())))
        merged[wallet_address] = ConsumerVisibilityWalletScope(
            wallet_address=wallet_address,
            seed_sources=seed_sources,
            discovery_sources=discovery_sources,
            in_seed_set=bool(seed_sources),
            in_verified_cohort=in_verified_cohort,
            in_signoff_cohort=in_signoff_cohort,
        )

    return merged


def _write_data_quality_issues(session: Session, issues: list[DataQualityIssue]) -> int:
    if not issues:
        return 0
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
    return len(issues)


def _upsert_customer_wallets(session: Session, wallet_addresses: Iterable[str]) -> dict[str, int]:
    normalized = sorted({canonical_address(address) for address in wallet_addresses})
    if normalized:
        stmt = insert(Wallet).values(
            [
                {"address": address, "wallet_type": "customer", "label": None}
                for address in normalized
            ]
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=[Wallet.address])
        session.execute(stmt)

    rows = session.execute(
        select(Wallet.address, Wallet.wallet_id).where(Wallet.address.in_(normalized))
    ).all()
    return {canonical_address(address): int(wallet_id) for address, wallet_id in rows}


def _configured_surface(
    consumer_markets_config: ConsumerMarketsConfig,
) -> tuple[set[str], set[str]]:
    configured_chains = {market.chain for market in consumer_markets_config.markets}
    configured_protocols = {
        debank_coverage.normalize_protocol_code(market.protocol)
        for market in consumer_markets_config.markets
    }
    return configured_chains, configured_protocols


def sync_consumer_debank_visibility(
    *,
    session: Session,
    client: DebankCloudClient,
    business_date: date,
    consumer_markets_config: ConsumerMarketsConfig,
    config_dir: Path = Path("config"),
    min_leg_usd: Decimal = Decimal("1"),
    max_concurrency: int = 6,
    max_wallets: int | None = None,
) -> ConsumerDebankVisibilitySyncSummary:
    """Build DeBank visibility snapshots for the consumer wallet universe."""

    if min_leg_usd < 0:
        raise ValueError("min_leg_usd must be non-negative")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
    if max_wallets is not None and max_wallets < 1:
        raise ValueError("max_wallets must be >= 1 when provided")

    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    seed_wallet_sources = load_consumer_seed_wallet_sources(config_dir)
    cohort_rows = session.scalars(
        select(ConsumerHolderUniverseDaily).where(
            ConsumerHolderUniverseDaily.business_date == business_date
        )
    ).all()
    wallet_scopes = merge_consumer_visibility_wallet_scopes(
        seed_wallet_sources=seed_wallet_sources,
        cohort_rows=cohort_rows,
    )
    wallet_addresses = sorted(wallet_scopes)
    if max_wallets is not None:
        wallet_addresses = wallet_addresses[:max_wallets]
        wallet_scopes = {wallet: wallet_scopes[wallet] for wallet in wallet_addresses}

    wallet_ids = _upsert_customer_wallets(session, wallet_addresses)
    configured_chains, configured_protocols = _configured_surface(consumer_markets_config)

    protocol_rows_out: list[dict[str, object]] = []
    token_rows_out: list[dict[str, object]] = []
    wallet_rows_out: list[dict[str, object]] = []
    issues: list[DataQualityIssue] = []

    fetched_wallet_count = 0
    fetch_error_count = 0
    active_wallet_count = 0
    borrow_wallet_count = 0
    configured_surface_wallet_count = 0

    def _scan_wallet(wallet_address: str) -> tuple[str, list[dict[str, object]], str | None]:
        try:
            payload = client.get_user_complex_protocols(wallet_address)
        except Exception as exc:  # pragma: no cover - network failures
            return wallet_address, [], str(exc)
        return wallet_address, payload, None

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [pool.submit(_scan_wallet, wallet_address) for wallet_address in wallet_addresses]
        for future in as_completed(futures):
            wallet_address, payload, fetch_error = future.result()
            scope = wallet_scopes[wallet_address]
            wallet_id = wallet_ids[wallet_address]

            if fetch_error is not None:
                fetch_error_count += 1
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_debank_visibility",
                        error_type="debank_wallet_fetch_failed",
                        error_message=fetch_error,
                        wallet_address=wallet_address,
                    )
                )
                wallet_rows_out.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "in_seed_set": scope.in_seed_set,
                        "in_verified_cohort": scope.in_verified_cohort,
                        "in_signoff_cohort": scope.in_signoff_cohort,
                        "seed_sources_json": list(scope.seed_sources) or None,
                        "discovery_sources_json": list(scope.discovery_sources) or None,
                        "fetch_succeeded": False,
                        "fetch_error_message": fetch_error,
                        "has_any_activity": False,
                        "has_any_borrow": False,
                        "has_configured_surface_activity": False,
                        "protocol_count": 0,
                        "chain_count": 0,
                        "configured_protocol_count": 0,
                        "total_supply_usd": ZERO,
                        "total_borrow_usd": ZERO,
                        "configured_surface_supply_usd": ZERO,
                        "configured_surface_borrow_usd": ZERO,
                    }
                )
                continue

            fetched_wallet_count += 1
            legs, in_scope = debank_coverage._flatten_debank_payload_legs(
                wallet_address=wallet_address,
                payload=payload,
                configured_chains=configured_chains,
                configured_protocols=configured_protocols,
                min_leg_usd=min_leg_usd,
            )

            protocol_totals: dict[tuple[str, str], dict[str, object]] = defaultdict(
                lambda: {
                    "supply_usd": ZERO,
                    "borrow_usd": ZERO,
                    "in_config_surface": False,
                }
            )
            for key, usd_value in legs.items():
                if is_excluded_visibility_protocol(key.protocol_code):
                    continue
                protocol_key = (key.chain_code, key.protocol_code)
                bucket = protocol_totals[protocol_key]
                if key.leg_type == "borrow":
                    bucket["borrow_usd"] = Decimal(str(bucket["borrow_usd"])) + usd_value
                else:
                    bucket["supply_usd"] = Decimal(str(bucket["supply_usd"])) + usd_value
                bucket["in_config_surface"] = bool(bucket["in_config_surface"]) or bool(
                    in_scope.get(key, False)
                )
                token_rows_out.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "chain_code": key.chain_code,
                        "protocol_code": key.protocol_code,
                        "token_symbol": key.token_symbol,
                        "leg_type": key.leg_type,
                        "in_config_surface": bool(in_scope.get(key, False)),
                        "usd_value": usd_value,
                    }
                )

            total_supply_usd = sum(
                (Decimal(str(row["supply_usd"])) for row in protocol_totals.values()),
                ZERO,
            )
            total_borrow_usd = sum(
                (Decimal(str(row["borrow_usd"])) for row in protocol_totals.values()),
                ZERO,
            )
            configured_surface_supply_usd = sum(
                (
                    Decimal(str(row["supply_usd"]))
                    for row in protocol_totals.values()
                    if bool(row["in_config_surface"])
                ),
                ZERO,
            )
            configured_surface_borrow_usd = sum(
                (
                    Decimal(str(row["borrow_usd"]))
                    for row in protocol_totals.values()
                    if bool(row["in_config_surface"])
                ),
                ZERO,
            )
            unique_protocols = {protocol_code for _chain_code, protocol_code in protocol_totals}
            unique_chains = {chain_code for chain_code, _protocol_code in protocol_totals}
            configured_protocols_seen = {
                protocol_code
                for (_chain_code, protocol_code), row in protocol_totals.items()
                if bool(row["in_config_surface"])
            }

            has_any_activity = bool(protocol_totals)
            has_any_borrow = total_borrow_usd > ZERO
            has_configured_surface_activity = (
                configured_surface_supply_usd > ZERO or configured_surface_borrow_usd > ZERO
            )
            if has_any_activity:
                active_wallet_count += 1
            if has_any_borrow:
                borrow_wallet_count += 1
            if has_configured_surface_activity:
                configured_surface_wallet_count += 1

            wallet_rows_out.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "wallet_id": wallet_id,
                    "wallet_address": wallet_address,
                    "in_seed_set": scope.in_seed_set,
                    "in_verified_cohort": scope.in_verified_cohort,
                    "in_signoff_cohort": scope.in_signoff_cohort,
                    "seed_sources_json": list(scope.seed_sources) or None,
                    "discovery_sources_json": list(scope.discovery_sources) or None,
                    "fetch_succeeded": True,
                    "fetch_error_message": None,
                    "has_any_activity": has_any_activity,
                    "has_any_borrow": has_any_borrow,
                    "has_configured_surface_activity": has_configured_surface_activity,
                    "protocol_count": len(unique_protocols),
                    "chain_count": len(unique_chains),
                    "configured_protocol_count": len(configured_protocols_seen),
                    "total_supply_usd": total_supply_usd,
                    "total_borrow_usd": total_borrow_usd,
                    "configured_surface_supply_usd": configured_surface_supply_usd,
                    "configured_surface_borrow_usd": configured_surface_borrow_usd,
                }
            )

            for (chain_code, protocol_code), row in sorted(protocol_totals.items()):
                protocol_rows_out.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "chain_code": chain_code,
                        "protocol_code": protocol_code,
                        "in_config_surface": bool(row["in_config_surface"]),
                        "supply_usd": Decimal(str(row["supply_usd"])),
                        "borrow_usd": Decimal(str(row["borrow_usd"])),
                    }
                )

    session.execute(
        delete(ConsumerDebankProtocolDaily).where(
            ConsumerDebankProtocolDaily.business_date == business_date
        )
    )
    session.execute(
        delete(ConsumerDebankTokenDaily).where(
            ConsumerDebankTokenDaily.business_date == business_date
        )
    )
    session.execute(
        delete(ConsumerDebankWalletDaily).where(
            ConsumerDebankWalletDaily.business_date == business_date
        )
    )
    if wallet_rows_out:
        session.execute(insert(ConsumerDebankWalletDaily).values(wallet_rows_out))
    if protocol_rows_out:
        session.execute(insert(ConsumerDebankProtocolDaily).values(protocol_rows_out))
    if token_rows_out:
        session.execute(insert(ConsumerDebankTokenDaily).values(token_rows_out))
    issues_written = _write_data_quality_issues(session, issues)

    verified_cohort_wallet_count = sum(
        1 for scope in wallet_scopes.values() if scope.in_verified_cohort
    )
    signoff_cohort_wallet_count = sum(
        1 for scope in wallet_scopes.values() if scope.in_signoff_cohort
    )
    new_discovered_not_in_seed_count = sum(
        1 for scope in wallet_scopes.values() if scope.in_verified_cohort and not scope.in_seed_set
    )

    return ConsumerDebankVisibilitySyncSummary(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        union_wallet_count=len(wallet_scopes),
        seed_wallet_count=sum(1 for scope in wallet_scopes.values() if scope.in_seed_set),
        verified_cohort_wallet_count=verified_cohort_wallet_count,
        signoff_cohort_wallet_count=signoff_cohort_wallet_count,
        new_discovered_not_in_seed_count=new_discovered_not_in_seed_count,
        fetched_wallet_count=fetched_wallet_count,
        fetch_error_count=fetch_error_count,
        active_wallet_count=active_wallet_count,
        borrow_wallet_count=borrow_wallet_count,
        configured_surface_wallet_count=configured_surface_wallet_count,
        wallet_rows_written=len(wallet_rows_out),
        protocol_rows_written=len(protocol_rows_out),
        issues_written=issues_written,
    )
