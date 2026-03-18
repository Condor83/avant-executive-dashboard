"""Reconciliation helpers for DeBank-vs-DB position leg coverage audits."""

from __future__ import annotations

import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, aliased

from core.config import MarketsConfig, canonical_address
from core.db.models import Chain, Market, PositionSnapshot, Token, Wallet
from core.db.models import Protocol as ProtocolModel

EVM_ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")

DEBANK_CHAIN_TO_LOCAL: dict[str, str] = {
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "avax": "avalanche",
    "base": "base",
    "bera": "bera",
    "bsc": "bsc",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "ink": "ink",
    "mnt": "mantle",
    "mantle": "mantle",
    "plasma": "plasma",
    "sol": "solana",
    "solana": "solana",
    "sonic": "sonic",
}

PROTOCOL_ALIASES: dict[str, str] = {
    "aave": "aave_v3",
    "aave3": "aave_v3",
    "aave_v3": "aave_v3",
    "arb_morphoblue": "morpho",
    "avax_aave3": "aave_v3",
    "avax_euler2": "euler_v2",
    "avax_silo": "silo_v2",
    "base_aave3": "aave_v3",
    "bera_dolomite": "dolomite",
    "avax_traderjoexyz": "traderjoe_lp",
    "bera_euler2": "euler_v2",
    "dolomite": "dolomite",
    "etherex": "etherex",
    "etherexfi": "etherex",
    "euler2": "euler_v2",
    "euler_v2": "euler_v2",
    "eulerv2": "euler_v2",
    "evk": "euler_v2",
    "kamino": "kamino",
    "morpho": "morpho",
    "morpho_blue": "morpho",
    "morphoblue": "morpho",
    "pendle": "pendle",
    "pendle2": "pendle",
    "plasma_aave3": "aave_v3",
    "spark": "spark",
    "stakedao": "stakedao",
    "traderjoexyz": "traderjoe_lp",
    "traderjoe": "traderjoe_lp",
    "silo": "silo_v2",
    "linea_etherexfi": "etherex",
    "zest": "zest",
}

TOKEN_CANONICALIZATION_MAX_RELATIVE_DELTA = Decimal("0.05")
NON_CONFIG_TOKEN_CANONICALIZATION_MAX_RELATIVE_DELTA = Decimal("0.60")
TOKEN_EQUIVALENCE_MAX_RELATIVE_DELTA = Decimal("0.10")

TOKEN_EQUIVALENCE_GROUPS: tuple[set[str], ...] = (
    {"ETH", "WETH", "WEETH"},
    {"USDE", "SUSDE"},
    {"AVUSD", "SAVUSD"},
    {"AVBTC", "SAVBTC"},
    {"AVETH", "SAVETH"},
)

# Known DeBank semantic mismatches that should not be treated as open ingest gaps.
# These are wallet/chain/protocol/leg/token specific until upstream classification is fixed.
MANUALLY_RESOLVED_DEBANK_LEGS: set[tuple[str, str, str, str, str]] = {
    (
        "0x920eefbcf1f5756109952e6ff6da1cab950c64d7",
        "ethereum",
        "morpho",
        "supply",
        "PYUSD",
    ),
}

DEBANK_EXCLUDED_REWARD_PROTOCOLS: set[str] = {
    "avax_merkl",
    "avax_yieldyak",
    "arb_merkl",
    "bsc_solv",
    "etherfi",
    "kingprotocol",
    "merkl",
    "plasma_merkl",
}


@dataclass(frozen=True)
class LegKey:
    wallet_address: str
    chain_code: str
    protocol_code: str
    leg_type: str
    token_symbol: str


@dataclass(frozen=True)
class WalletFetchError:
    wallet_address: str
    error_message: str


@dataclass(frozen=True)
class LegMatchRow:
    key: LegKey
    debank_usd: Decimal
    db_usd: Decimal | None
    matched: bool
    within_tolerance: bool | None
    delta_usd: Decimal | None
    in_config_surface: bool


@dataclass(frozen=True)
class CoverageTotals:
    total_legs: int
    matched_legs: int
    coverage_pct: Decimal
    debank_total_usd: Decimal
    matched_usd: Decimal
    usd_coverage_pct: Decimal


@dataclass(frozen=True)
class ProtocolCoverageRow:
    protocol_code: str
    total_legs: int
    matched_legs: int
    coverage_pct: Decimal
    debank_total_usd: Decimal
    matched_usd: Decimal
    usd_coverage_pct: Decimal


@dataclass(frozen=True)
class PreflightStatus:
    missing_protocol_dimensions: list[str]
    zero_snapshot_protocols: list[str]
    snapshot_counts_by_protocol: dict[str, int]


@dataclass(frozen=True)
class DebankCoverageAuditResult:
    as_of_ts_utc: datetime
    wallets_total: int
    wallets_scanned: int
    non_evm_wallets_skipped: int
    wallet_errors: list[WalletFetchError]
    preflight: PreflightStatus
    totals_all: CoverageTotals
    totals_configured_surface: CoverageTotals
    protocol_rows: list[ProtocolCoverageRow]
    unmatched_rows: list[LegMatchRow]
    db_only_leg_count: int


class DebankCoverageClient(Protocol):
    """Protocol for DeBank client behavior used by this module."""

    def get_user_complex_protocols(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        """Return wallet protocol positions from DeBank."""


def is_evm_address(value: str) -> bool:
    """Return whether a value is a canonical EVM wallet address."""

    return bool(EVM_ADDRESS_PATTERN.match(value.strip()))


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _to_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return str(value)


def _to_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    return None


def normalize_chain_code(debank_chain_id: str) -> str:
    """Normalize DeBank chain ids into local chain codes."""

    normalized = debank_chain_id.strip().lower()
    return DEBANK_CHAIN_TO_LOCAL.get(normalized, normalized)


def normalize_protocol_code(protocol_id: str) -> str:
    """Normalize DeBank protocol ids into local canonical protocol code guesses."""

    normalized = _slug(protocol_id)
    mapped = PROTOCOL_ALIASES.get(normalized)
    if mapped:
        return mapped
    if "morpho" in normalized:
        return "morpho"
    if "aave" in normalized:
        return "aave_v3"
    if "euler" in normalized or "evk" in normalized:
        return "euler_v2"
    if "dolomite" in normalized:
        return "dolomite"
    if "kamino" in normalized:
        return "kamino"
    if "spark" in normalized:
        return "spark"
    if "zest" in normalized:
        return "zest"
    if "silo" in normalized:
        return "silo_v2"
    return normalized


def normalize_token_symbol(symbol: str) -> str:
    """Normalize token symbol aliases used for cross-source matching."""

    normalized = symbol.strip().upper().replace(" ", "").replace("₮", "T")
    compact = normalized.replace("_", "").replace("-", "")

    if compact in {"ETH", "WETH"}:
        return "ETH"
    if normalized in {"USDC.E", "USDCE"} or compact == "USDCE":
        return "USDC"
    if compact in {"WBRAVUSDC", "BRAVUSDC"}:
        return "USDC"
    if compact == "USDT0":
        return "USDT0"
    return normalized


def _leg_type_from_detail_key(detail_key: str) -> str | None:
    lower = detail_key.lower()
    if "borrow" in lower:
        return "borrow"
    if "supply" in lower or "deposit" in lower or "collateral" in lower:
        return "supply"
    return None


def _configured_surface(markets_config: MarketsConfig) -> tuple[set[str], set[str]]:
    configured_chains: set[str] = set()
    for section in (
        markets_config.aave_v3,
        markets_config.spark,
        markets_config.morpho,
        markets_config.euler_v2,
        markets_config.dolomite,
        markets_config.kamino,
        markets_config.pendle,
        markets_config.zest,
        markets_config.wallet_balances,
        markets_config.traderjoe_lp,
        markets_config.stakedao,
        markets_config.etherex,
    ):
        configured_chains.update(chain.lower() for chain in section.keys())

    configured_protocols: set[str] = set()
    if markets_config.aave_v3:
        configured_protocols.add("aave_v3")
    if markets_config.spark:
        configured_protocols.add("spark")
    if markets_config.morpho:
        configured_protocols.add("morpho")
    if markets_config.euler_v2:
        configured_protocols.add("euler_v2")
    if markets_config.dolomite:
        configured_protocols.add("dolomite")
    if markets_config.kamino:
        configured_protocols.add("kamino")
    if markets_config.pendle:
        configured_protocols.add("pendle")
    if markets_config.zest:
        configured_protocols.add("zest")
    if markets_config.traderjoe_lp:
        configured_protocols.add("traderjoe_lp")
    if markets_config.stakedao:
        configured_protocols.add("stakedao")
    if markets_config.etherex:
        configured_protocols.add("etherex")
    return configured_chains, configured_protocols


def _resolve_snapshot_as_of(session: Session, requested_as_of: datetime | None) -> datetime:
    if requested_as_of is None:
        latest = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
        if latest is None:
            raise RuntimeError("position_snapshots table is empty")
        return latest

    matched = session.scalar(
        select(func.max(PositionSnapshot.as_of_ts_utc)).where(
            PositionSnapshot.as_of_ts_utc <= requested_as_of
        )
    )
    if matched is None:
        raise RuntimeError(
            f"no position snapshots found at or before {requested_as_of.isoformat()}"
        )
    return matched


def _strategy_wallets_from_db(session: Session) -> tuple[list[str], list[str]]:
    rows = session.execute(
        select(Wallet.address)
        .where(Wallet.wallet_type == "strategy")
        .order_by(Wallet.address.asc())
    ).all()
    all_strategy = [canonical_address(address) for (address,) in rows]
    evm_only = [wallet for wallet in all_strategy if is_evm_address(wallet)]
    return all_strategy, evm_only


def _symbol_from_metadata(metadata_json: object, *keys: str) -> str | None:
    if not isinstance(metadata_json, dict):
        return None
    for key in keys:
        value = metadata_json.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _db_leg_token_symbol(
    *,
    protocol_code: str,
    leg_type: str,
    base_symbol: str | None,
    collateral_symbol: str | None,
    metadata_json: object,
    supplied_usd: Decimal,
    raw_supplied_usd: Decimal,
    collateral_usd: Decimal | None,
    borrowed_usd: Decimal,
) -> str | None:
    if protocol_code == "dolomite":
        # Dolomite market rows may not always resolve a base token dimension; fall back to metadata.
        return _symbol_from_metadata(metadata_json, "symbol") or base_symbol or collateral_symbol

    if protocol_code == "morpho":
        if leg_type == "supply":
            if borrowed_usd <= 0:
                return (
                    _symbol_from_metadata(metadata_json, "loan_token")
                    or base_symbol
                    or _symbol_from_metadata(metadata_json, "collateral_token")
                    or collateral_symbol
                )
            return (
                _symbol_from_metadata(metadata_json, "collateral_token")
                or collateral_symbol
                or base_symbol
            )
        return (
            _symbol_from_metadata(metadata_json, "loan_token") or base_symbol or collateral_symbol
        )

    if protocol_code == "kamino":
        if leg_type == "supply":
            return (
                _symbol_from_metadata(metadata_json, "supply_token_symbol")
                or collateral_symbol
                or base_symbol
            )
        return (
            _symbol_from_metadata(metadata_json, "borrow_token_symbol")
            or base_symbol
            or collateral_symbol
        )

    if protocol_code == "euler_v2":
        if leg_type == "supply":
            # Consumer-market synthetic rows encode supply token as collateral.
            return (
                _symbol_from_metadata(metadata_json, "collateral_token_symbol")
                or collateral_symbol
                or _symbol_from_metadata(metadata_json, "asset_symbol")
                or base_symbol
            )
        return (
            _symbol_from_metadata(metadata_json, "borrow_token_symbol")
            or base_symbol
            or collateral_symbol
        )

    if protocol_code == "traderjoe_lp":
        if leg_type == "supply":
            return (
                _symbol_from_metadata(metadata_json, "token_y_symbol")
                or base_symbol
                or _symbol_from_metadata(metadata_json, "token_x_symbol")
                or collateral_symbol
            )
        return base_symbol or collateral_symbol

    if protocol_code == "pendle":
        if leg_type == "supply":
            if collateral_usd is not None and collateral_usd > 0 and raw_supplied_usd <= 0:
                return (
                    _symbol_from_metadata(metadata_json, "yt_token_symbol")
                    or collateral_symbol
                    or base_symbol
                )
            return (
                _symbol_from_metadata(metadata_json, "pt_token_symbol")
                or base_symbol
                or collateral_symbol
            )
        return base_symbol or collateral_symbol

    if leg_type == "supply":
        return _symbol_from_metadata(metadata_json, "symbol") or base_symbol or collateral_symbol
    return (
        base_symbol
        or _symbol_from_metadata(metadata_json, "borrow_token_symbol")
        or collateral_symbol
    )


def _load_db_legs(
    *,
    session: Session,
    as_of_ts_utc: datetime,
    min_leg_usd: Decimal,
) -> dict[LegKey, Decimal]:
    base_token = aliased(Token)
    collateral_token = aliased(Token)
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
            Wallet.address,
            Chain.chain_code,
            ProtocolModel.protocol_code,
            economic_supply_usd.label("supplied_usd"),
            PositionSnapshot.supplied_usd.label("raw_supplied_usd"),
            PositionSnapshot.collateral_usd,
            PositionSnapshot.borrowed_usd,
            Market.metadata_json,
            base_token.symbol,
            collateral_token.symbol,
        )
        .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
        .outerjoin(base_token, base_token.token_id == Market.base_asset_token_id)
        .outerjoin(collateral_token, collateral_token.token_id == Market.collateral_token_id)
        .where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
        .where(Wallet.wallet_type == "strategy")
    ).all()

    aggregated: dict[LegKey, Decimal] = defaultdict(lambda: Decimal("0"))
    for (
        wallet_address,
        chain_code,
        protocol_code,
        supplied_usd,
        raw_supplied_usd,
        collateral_usd,
        borrowed_usd,
        metadata_json,
        base_symbol,
        collateral_symbol,
    ) in rows:
        wallet = canonical_address(wallet_address)
        chain = chain_code.lower()
        protocol = protocol_code.lower()

        supplied = abs(Decimal(supplied_usd))
        borrowed = abs(Decimal(borrowed_usd))
        if supplied >= min_leg_usd:
            symbol = _db_leg_token_symbol(
                protocol_code=protocol,
                leg_type="supply",
                base_symbol=base_symbol,
                collateral_symbol=collateral_symbol,
                metadata_json=metadata_json,
                supplied_usd=supplied,
                raw_supplied_usd=abs(Decimal(raw_supplied_usd)),
                collateral_usd=(
                    abs(Decimal(collateral_usd))
                    if collateral_usd is not None
                    else None
                ),
                borrowed_usd=borrowed,
            )
            if symbol:
                key = LegKey(
                    wallet_address=wallet,
                    chain_code=chain,
                    protocol_code=protocol,
                    leg_type="supply",
                    token_symbol=normalize_token_symbol(symbol),
                )
                aggregated[key] += supplied

        if borrowed >= min_leg_usd:
            symbol = _db_leg_token_symbol(
                protocol_code=protocol,
                leg_type="borrow",
                base_symbol=base_symbol,
                collateral_symbol=collateral_symbol,
                metadata_json=metadata_json,
                supplied_usd=supplied,
                raw_supplied_usd=abs(Decimal(raw_supplied_usd)),
                collateral_usd=(
                    abs(Decimal(collateral_usd))
                    if collateral_usd is not None
                    else None
                ),
                borrowed_usd=borrowed,
            )
            if symbol:
                key = LegKey(
                    wallet_address=wallet,
                    chain_code=chain,
                    protocol_code=protocol,
                    leg_type="borrow",
                    token_symbol=normalize_token_symbol(symbol),
                )
                aggregated[key] += borrowed

    return dict(aggregated)


def _flatten_debank_payload_legs(
    *,
    wallet_address: str,
    payload: list[dict[str, object]],
    configured_chains: set[str],
    configured_protocols: set[str],
    min_leg_usd: Decimal,
) -> tuple[dict[LegKey, Decimal], dict[LegKey, bool]]:
    aggregated: dict[LegKey, Decimal] = defaultdict(lambda: Decimal("0"))
    in_scope: dict[LegKey, bool] = {}

    for protocol_payload in payload:
        chain_id = _to_string(protocol_payload.get("chain"))
        if not chain_id:
            continue
        chain_code = normalize_chain_code(chain_id)

        protocol_id = _to_string(protocol_payload.get("id")) or "unknown"
        protocol_code = normalize_protocol_code(protocol_id)
        if protocol_code in DEBANK_EXCLUDED_REWARD_PROTOCOLS:
            continue

        portfolio_items = protocol_payload.get("portfolio_item_list")
        if not isinstance(portfolio_items, list):
            continue

        is_config_surface = (
            chain_code in configured_chains and protocol_code in configured_protocols
        )

        for item in portfolio_items:
            if not isinstance(item, dict):
                continue
            detail = item.get("detail")
            if not isinstance(detail, dict):
                continue
            for detail_key, detail_value in detail.items():
                if not detail_key.endswith("_token_list") or not isinstance(detail_value, list):
                    continue
                leg_type = _leg_type_from_detail_key(detail_key)
                if leg_type is None:
                    continue
                for token in detail_value:
                    if not isinstance(token, dict):
                        continue
                    symbol = _to_string(token.get("optimized_symbol")) or _to_string(
                        token.get("symbol")
                    )
                    if not symbol:
                        continue

                    usd_value = _to_decimal(token.get("usd_value"))
                    if usd_value is None:
                        amount = _to_decimal(token.get("amount"))
                        price = _to_decimal(token.get("price"))
                        if amount is not None and price is not None:
                            usd_value = amount * price
                    if usd_value is None:
                        continue

                    usd_abs = abs(usd_value)
                    if usd_abs < min_leg_usd:
                        continue

                    key = LegKey(
                        wallet_address=wallet_address,
                        chain_code=chain_code,
                        protocol_code=protocol_code,
                        leg_type=leg_type,
                        token_symbol=normalize_token_symbol(symbol),
                    )
                    aggregated[key] += usd_abs
                    in_scope[key] = is_config_surface

    return dict(aggregated), in_scope


def _preflight_status(
    *,
    session: Session,
    as_of_ts_utc: datetime,
    configured_protocols: set[str],
) -> PreflightStatus:
    protocol_codes_in_dimension = {
        protocol_code
        for (protocol_code,) in session.execute(select(ProtocolModel.protocol_code)).all()
    }
    missing_protocol_dimensions = sorted(configured_protocols - protocol_codes_in_dimension)

    rows = session.execute(
        select(
            ProtocolModel.protocol_code,
            func.count(PositionSnapshot.snapshot_id),
        )
        .select_from(PositionSnapshot)
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
        .where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
        .group_by(ProtocolModel.protocol_code)
    ).all()
    snapshot_counts_by_protocol = {protocol_code: int(count) for protocol_code, count in rows}

    zero_snapshot_protocols = sorted(
        protocol_code
        for protocol_code in configured_protocols
        if snapshot_counts_by_protocol.get(protocol_code, 0) == 0
    )

    return PreflightStatus(
        missing_protocol_dimensions=missing_protocol_dimensions,
        zero_snapshot_protocols=zero_snapshot_protocols,
        snapshot_counts_by_protocol=snapshot_counts_by_protocol,
    )


def _bucket_key(key: LegKey) -> tuple[str, str, str, str]:
    return (
        key.wallet_address,
        key.chain_code,
        key.protocol_code,
        key.leg_type,
    )


def _non_config_bucket_key(key: LegKey) -> tuple[str, str, str]:
    return (
        key.wallet_address,
        key.chain_code,
        key.leg_type,
    )


def _token_equivalents(symbol: str) -> set[str]:
    for group in TOKEN_EQUIVALENCE_GROUPS:
        if symbol in group:
            return set(group)
    return {symbol}


def _canonicalize_debank_token_keys_to_db(
    *,
    debank_aggregated: dict[LegKey, Decimal],
    debank_in_scope: dict[LegKey, bool],
    db_aggregated: dict[LegKey, Decimal],
    max_relative_delta: Decimal = TOKEN_CANONICALIZATION_MAX_RELATIVE_DELTA,
) -> tuple[dict[LegKey, Decimal], dict[LegKey, bool]]:
    """Remap DeBank token symbols to DB token symbols by bucketed notional proximity.

    DB/RPC symbols are treated as canonical. DeBank symbol keys are remapped only when:
    - token symbol already matches DB exactly, or
    - nearest unmatched DB token in same bucket is within `max_relative_delta`.
    """

    if max_relative_delta < 0:
        raise ValueError("max_relative_delta must be non-negative")

    db_by_bucket: dict[tuple[str, str, str, str], list[tuple[LegKey, Decimal]]] = defaultdict(list)
    for db_key, db_usd in db_aggregated.items():
        db_by_bucket[_bucket_key(db_key)].append((db_key, db_usd))

    debank_by_bucket: dict[tuple[str, str, str, str], list[tuple[LegKey, Decimal]]] = defaultdict(
        list
    )
    for debank_key, debank_usd in debank_aggregated.items():
        debank_by_bucket[_bucket_key(debank_key)].append((debank_key, debank_usd))

    canonicalized: dict[LegKey, Decimal] = defaultdict(lambda: Decimal("0"))
    canonicalized_in_scope: dict[LegKey, bool] = {}

    for bucket, debank_items in debank_by_bucket.items():
        db_items = db_by_bucket.get(bucket, [])
        db_exact_by_symbol: dict[str, tuple[LegKey, Decimal]] = {
            db_key.token_symbol: (db_key, db_usd) for db_key, db_usd in db_items
        }
        used_db_symbols: set[str] = set()
        deferred: list[tuple[LegKey, Decimal]] = []

        # Pass 1: exact symbol matches to preserve deterministic canonical keys.
        for debank_key, debank_usd in sorted(debank_items, key=lambda item: item[1], reverse=True):
            exact = db_exact_by_symbol.get(debank_key.token_symbol)
            if exact is None:
                deferred.append((debank_key, debank_usd))
                continue

            canonical_key, _db_usd = exact
            used_db_symbols.add(canonical_key.token_symbol)
            canonicalized[canonical_key] += debank_usd
            canonicalized_in_scope[canonical_key] = canonicalized_in_scope.get(
                canonical_key, False
            ) or debank_in_scope.get(debank_key, False)

        # Pass 2: remap symbol-equivalent tokens before amount-proximity fallback.
        remaining: list[tuple[LegKey, Decimal]] = []
        for debank_key, debank_usd in sorted(deferred, key=lambda item: item[1], reverse=True):
            equivalent_symbols = _token_equivalents(debank_key.token_symbol)
            equivalent_candidates = [
                (db_key, db_usd)
                for db_key, db_usd in db_items
                if db_key.token_symbol in equivalent_symbols
                and db_key.token_symbol not in used_db_symbols
            ]
            if not equivalent_candidates:
                remaining.append((debank_key, debank_usd))
                continue

            best_key: LegKey | None = None
            best_delta: Decimal | None = None
            for candidate_key, candidate_usd in equivalent_candidates:
                delta = abs(candidate_usd - debank_usd)
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best_key = candidate_key
            if best_key is None or best_delta is None:
                remaining.append((debank_key, debank_usd))
                continue

            denominator = debank_usd if debank_usd > 0 else Decimal("1")
            relative_delta = best_delta / denominator
            allowed_relative_delta = (
                max_relative_delta
                if best_key.token_symbol == debank_key.token_symbol
                else TOKEN_EQUIVALENCE_MAX_RELATIVE_DELTA
            )
            if relative_delta > allowed_relative_delta:
                remaining.append((debank_key, debank_usd))
                continue

            used_db_symbols.add(best_key.token_symbol)
            canonicalized[best_key] += debank_usd
            canonicalized_in_scope[best_key] = canonicalized_in_scope.get(
                best_key, False
            ) or debank_in_scope.get(debank_key, False)

        # Pass 3: remap unmatched DeBank symbols to nearest unmatched DB symbol in bucket.
        for debank_key, debank_usd in sorted(remaining, key=lambda item: item[1], reverse=True):
            candidates = [
                (db_key, db_usd)
                for db_key, db_usd in db_items
                if db_key.token_symbol not in used_db_symbols
            ]
            if not candidates:
                canonicalized[debank_key] += debank_usd
                canonicalized_in_scope[debank_key] = canonicalized_in_scope.get(
                    debank_key, False
                ) or debank_in_scope.get(debank_key, False)
                continue

            best_key_fallback: LegKey | None = None
            best_delta_fallback: Decimal | None = None
            for candidate_key, candidate_usd in candidates:
                delta = abs(candidate_usd - debank_usd)
                if best_delta_fallback is None or delta < best_delta_fallback:
                    best_delta_fallback = delta
                    best_key_fallback = candidate_key

            if best_key_fallback is None or best_delta_fallback is None:
                canonicalized[debank_key] += debank_usd
                canonicalized_in_scope[debank_key] = canonicalized_in_scope.get(
                    debank_key, False
                ) or debank_in_scope.get(debank_key, False)
                continue

            denominator = debank_usd if debank_usd > 0 else Decimal("1")
            relative_delta = best_delta_fallback / denominator
            if relative_delta <= max_relative_delta:
                used_db_symbols.add(best_key_fallback.token_symbol)
                canonicalized[best_key_fallback] += debank_usd
                canonicalized_in_scope[best_key_fallback] = canonicalized_in_scope.get(
                    best_key_fallback, False
                ) or debank_in_scope.get(debank_key, False)
            else:
                canonicalized[debank_key] += debank_usd
                canonicalized_in_scope[debank_key] = canonicalized_in_scope.get(
                    debank_key, False
                ) or debank_in_scope.get(debank_key, False)

    return dict(canonicalized), canonicalized_in_scope


def _canonicalize_non_config_cross_protocol(
    *,
    debank_canonicalized: dict[LegKey, Decimal],
    debank_in_scope: dict[LegKey, bool],
    db_aggregated: dict[LegKey, Decimal],
    max_relative_delta: Decimal = NON_CONFIG_TOKEN_CANONICALIZATION_MAX_RELATIVE_DELTA,
) -> tuple[dict[LegKey, Decimal], dict[LegKey, bool]]:
    """Remap unmatched non-config legs across protocols with strict token equivalence."""

    if max_relative_delta < 0:
        raise ValueError("max_relative_delta must be non-negative")

    db_by_bucket: dict[tuple[str, str, str], list[tuple[LegKey, Decimal]]] = defaultdict(list)
    for db_key, db_usd in db_aggregated.items():
        db_by_bucket[_non_config_bucket_key(db_key)].append((db_key, db_usd))

    remapped: dict[LegKey, Decimal] = defaultdict(lambda: Decimal("0"))
    remapped_in_scope: dict[LegKey, bool] = {}

    used_db_keys: set[LegKey] = {key for key in debank_canonicalized if key in db_aggregated}

    for debank_key, debank_usd in sorted(
        debank_canonicalized.items(),
        key=lambda item: item[1],
        reverse=True,
    ):
        in_scope = debank_in_scope.get(debank_key, False)
        if in_scope or debank_key in db_aggregated:
            remapped[debank_key] += debank_usd
            remapped_in_scope[debank_key] = remapped_in_scope.get(debank_key, False) or in_scope
            continue

        candidates = [
            (db_key, db_usd)
            for db_key, db_usd in db_by_bucket.get(_non_config_bucket_key(debank_key), [])
            if db_key not in used_db_keys
            and db_key.token_symbol in _token_equivalents(debank_key.token_symbol)
        ]
        if not candidates:
            remapped[debank_key] += debank_usd
            remapped_in_scope[debank_key] = remapped_in_scope.get(debank_key, False) or in_scope
            continue

        best_key: LegKey | None = None
        best_delta: Decimal | None = None
        for candidate_key, candidate_usd in candidates:
            delta = abs(candidate_usd - debank_usd)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_key = candidate_key

        if best_key is None or best_delta is None:
            remapped[debank_key] += debank_usd
            remapped_in_scope[debank_key] = remapped_in_scope.get(debank_key, False) or in_scope
            continue

        denominator = debank_usd if debank_usd > 0 else Decimal("1")
        relative_delta = best_delta / denominator
        if relative_delta > max_relative_delta:
            remapped[debank_key] += debank_usd
            remapped_in_scope[debank_key] = remapped_in_scope.get(debank_key, False) or in_scope
            continue

        used_db_keys.add(best_key)
        remapped[best_key] += debank_usd
        remapped_in_scope[best_key] = remapped_in_scope.get(best_key, False) or in_scope

    return dict(remapped), remapped_in_scope


def _percent(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return (numerator / denominator) * Decimal("100")


def _is_manually_resolved_debank_leg(key: LegKey) -> bool:
    return (
        key.wallet_address,
        key.chain_code,
        key.protocol_code,
        key.leg_type,
        key.token_symbol,
    ) in MANUALLY_RESOLVED_DEBANK_LEGS


def _build_totals(rows: list[LegMatchRow]) -> CoverageTotals:
    total_legs = len(rows)
    matched_legs = sum(1 for row in rows if row.matched)
    debank_total_usd = sum((row.debank_usd for row in rows), Decimal("0"))
    matched_usd = sum((row.debank_usd for row in rows if row.matched), Decimal("0"))
    return CoverageTotals(
        total_legs=total_legs,
        matched_legs=matched_legs,
        coverage_pct=_percent(Decimal(matched_legs), Decimal(total_legs)),
        debank_total_usd=debank_total_usd,
        matched_usd=matched_usd,
        usd_coverage_pct=_percent(matched_usd, debank_total_usd),
    )


def _protocol_rows(rows: list[LegMatchRow]) -> list[ProtocolCoverageRow]:
    grouped: dict[str, list[LegMatchRow]] = defaultdict(list)
    for row in rows:
        grouped[row.key.protocol_code].append(row)

    result: list[ProtocolCoverageRow] = []
    for protocol_code, protocol_rows in grouped.items():
        totals = _build_totals(protocol_rows)
        result.append(
            ProtocolCoverageRow(
                protocol_code=protocol_code,
                total_legs=totals.total_legs,
                matched_legs=totals.matched_legs,
                coverage_pct=totals.coverage_pct,
                debank_total_usd=totals.debank_total_usd,
                matched_usd=totals.matched_usd,
                usd_coverage_pct=totals.usd_coverage_pct,
            )
        )

    result.sort(key=lambda row: (-row.debank_total_usd, row.protocol_code))
    return result


def run_debank_coverage_audit(
    *,
    session: Session,
    client: DebankCoverageClient,
    markets_config: MarketsConfig,
    as_of_ts_utc: datetime | None,
    min_leg_usd: Decimal,
    match_tolerance_usd: Decimal,
    max_concurrency: int = 6,
    max_wallets: int | None = None,
) -> DebankCoverageAuditResult:
    """Compare DeBank wallet legs against DB snapshot legs and compute coverage."""

    if min_leg_usd < 0:
        raise ValueError("min_leg_usd must be non-negative")
    if match_tolerance_usd < 0:
        raise ValueError("match_tolerance_usd must be non-negative")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
    if max_wallets is not None and max_wallets < 1:
        raise ValueError("max_wallets must be >= 1 when provided")

    all_wallets, evm_wallets = _strategy_wallets_from_db(session)
    if max_wallets is not None:
        evm_wallets = evm_wallets[:max_wallets]

    resolved_as_of = _resolve_snapshot_as_of(session, as_of_ts_utc)
    configured_chains, configured_protocols = _configured_surface(markets_config)

    preflight = _preflight_status(
        session=session,
        as_of_ts_utc=resolved_as_of,
        configured_protocols=configured_protocols,
    )

    debank_aggregated: dict[LegKey, Decimal] = defaultdict(lambda: Decimal("0"))
    debank_in_scope: dict[LegKey, bool] = {}
    wallet_errors: list[WalletFetchError] = []

    def _scan_wallet(wallet_address: str) -> tuple[str, list[dict[str, object]], str | None]:
        try:
            payload = client.get_user_complex_protocols(wallet_address)
        except Exception as exc:  # pragma: no cover - network failures
            return wallet_address, [], str(exc)
        return wallet_address, payload, None

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [pool.submit(_scan_wallet, wallet) for wallet in evm_wallets]
        for future in as_completed(futures):
            wallet_address, payload, fetch_error = future.result()
            if fetch_error is not None:
                wallet_errors.append(
                    WalletFetchError(wallet_address=wallet_address, error_message=fetch_error)
                )
                continue

            wallet_aggregated, wallet_in_scope = _flatten_debank_payload_legs(
                wallet_address=wallet_address,
                payload=payload,
                configured_chains=configured_chains,
                configured_protocols=configured_protocols,
                min_leg_usd=min_leg_usd,
            )
            for key, value in wallet_aggregated.items():
                debank_aggregated[key] += value
                debank_in_scope[key] = wallet_in_scope.get(key, False)

    db_aggregated = _load_db_legs(
        session=session,
        as_of_ts_utc=resolved_as_of,
        min_leg_usd=min_leg_usd,
    )
    debank_canonicalized, debank_canonicalized_in_scope = _canonicalize_debank_token_keys_to_db(
        debank_aggregated=debank_aggregated,
        debank_in_scope=debank_in_scope,
        db_aggregated=db_aggregated,
    )
    debank_canonicalized, debank_canonicalized_in_scope = _canonicalize_non_config_cross_protocol(
        debank_canonicalized=debank_canonicalized,
        debank_in_scope=debank_canonicalized_in_scope,
        db_aggregated=db_aggregated,
    )

    matches: list[LegMatchRow] = []
    for key, debank_usd in debank_canonicalized.items():
        if _is_manually_resolved_debank_leg(key):
            continue

        db_usd = db_aggregated.get(key)
        if db_usd is None:
            matches.append(
                LegMatchRow(
                    key=key,
                    debank_usd=debank_usd,
                    db_usd=None,
                    matched=False,
                    within_tolerance=None,
                    delta_usd=None,
                    in_config_surface=debank_canonicalized_in_scope.get(key, False),
                )
            )
            continue

        delta = abs(db_usd - debank_usd)
        matches.append(
            LegMatchRow(
                key=key,
                debank_usd=debank_usd,
                db_usd=db_usd,
                matched=True,
                within_tolerance=delta <= match_tolerance_usd,
                delta_usd=delta,
                in_config_surface=debank_canonicalized_in_scope.get(key, False),
            )
        )

    matches.sort(
        key=lambda row: (
            row.key.wallet_address,
            row.key.chain_code,
            row.key.protocol_code,
            row.key.leg_type,
            row.key.token_symbol,
        )
    )

    totals_all = _build_totals(matches)
    configured_surface_matches = [row for row in matches if row.in_config_surface]
    totals_configured_surface = _build_totals(configured_surface_matches)

    protocol_rows = _protocol_rows(matches)
    unmatched_rows = sorted(matches, key=lambda row: row.debank_usd, reverse=True)
    unmatched_rows = [row for row in unmatched_rows if not row.matched]

    db_only_leg_count = sum(1 for key in db_aggregated if key not in debank_canonicalized)

    wallet_errors.sort(key=lambda row: row.wallet_address)
    return DebankCoverageAuditResult(
        as_of_ts_utc=resolved_as_of,
        wallets_total=len(all_wallets),
        wallets_scanned=len(evm_wallets),
        non_evm_wallets_skipped=max(len(all_wallets) - len(evm_wallets), 0),
        wallet_errors=wallet_errors,
        preflight=preflight,
        totals_all=totals_all,
        totals_configured_surface=totals_configured_surface,
        protocol_rows=protocol_rows,
        unmatched_rows=unmatched_rows,
        db_only_leg_count=db_only_leg_count,
    )


def serialize_audit_result(
    result: DebankCoverageAuditResult,
    *,
    unmatched_limit: int,
) -> dict[str, object]:
    """Serialize audit result into JSON-compatible primitives."""

    if unmatched_limit < 0:
        raise ValueError("unmatched_limit must be non-negative")

    def _decimal_to_str(value: Decimal) -> str:
        return format(value, "f")

    return {
        "as_of_ts_utc": result.as_of_ts_utc.isoformat(),
        "wallets_total": result.wallets_total,
        "wallets_scanned": result.wallets_scanned,
        "non_evm_wallets_skipped": result.non_evm_wallets_skipped,
        "wallet_errors": [
            {"wallet_address": row.wallet_address, "error_message": row.error_message}
            for row in result.wallet_errors
        ],
        "preflight": {
            "missing_protocol_dimensions": result.preflight.missing_protocol_dimensions,
            "zero_snapshot_protocols": result.preflight.zero_snapshot_protocols,
            "snapshot_counts_by_protocol": result.preflight.snapshot_counts_by_protocol,
        },
        "totals_all": {
            "total_legs": result.totals_all.total_legs,
            "matched_legs": result.totals_all.matched_legs,
            "coverage_pct": _decimal_to_str(result.totals_all.coverage_pct),
            "debank_total_usd": _decimal_to_str(result.totals_all.debank_total_usd),
            "matched_usd": _decimal_to_str(result.totals_all.matched_usd),
            "usd_coverage_pct": _decimal_to_str(result.totals_all.usd_coverage_pct),
        },
        "totals_configured_surface": {
            "total_legs": result.totals_configured_surface.total_legs,
            "matched_legs": result.totals_configured_surface.matched_legs,
            "coverage_pct": _decimal_to_str(result.totals_configured_surface.coverage_pct),
            "debank_total_usd": _decimal_to_str(result.totals_configured_surface.debank_total_usd),
            "matched_usd": _decimal_to_str(result.totals_configured_surface.matched_usd),
            "usd_coverage_pct": _decimal_to_str(result.totals_configured_surface.usd_coverage_pct),
        },
        "protocol_rows": [
            {
                "protocol_code": row.protocol_code,
                "total_legs": row.total_legs,
                "matched_legs": row.matched_legs,
                "coverage_pct": _decimal_to_str(row.coverage_pct),
                "debank_total_usd": _decimal_to_str(row.debank_total_usd),
                "matched_usd": _decimal_to_str(row.matched_usd),
                "usd_coverage_pct": _decimal_to_str(row.usd_coverage_pct),
            }
            for row in result.protocol_rows
        ],
        "unmatched_rows": [
            {
                "wallet_address": row.key.wallet_address,
                "chain_code": row.key.chain_code,
                "protocol_code": row.key.protocol_code,
                "leg_type": row.key.leg_type,
                "token_symbol": row.key.token_symbol,
                "debank_usd": _decimal_to_str(row.debank_usd),
                "db_usd": _decimal_to_str(row.db_usd) if row.db_usd is not None else None,
                "within_tolerance": row.within_tolerance,
                "delta_usd": _decimal_to_str(row.delta_usd) if row.delta_usd is not None else None,
                "in_config_surface": row.in_config_surface,
            }
            for row in result.unmatched_rows[:unmatched_limit]
        ],
        "db_only_leg_count": result.db_only_leg_count,
    }
