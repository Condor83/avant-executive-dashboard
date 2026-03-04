"""Risk scoring for market kink/liquidity/borrow-shock and position spread compression."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.config import RiskThresholdsConfig
from core.db.models import Chain, Market, MarketSnapshot, PositionSnapshot, Protocol

ZERO = Decimal("0")
SOURCE_PRIORITY = {"rpc": 0, "defillama": 1, "debank": 2}


@dataclass(frozen=True)
class PreviousBorrowSnapshot:
    """Previous market snapshot required for borrow-rate delta scoring."""

    as_of_ts_utc: datetime
    borrow_apy: Decimal


@dataclass(frozen=True)
class MarketRiskRow:
    """Derived market-level risk metrics at a shared as-of timestamp."""

    as_of_ts_utc: datetime
    market_id: int
    protocol_code: str
    chain_code: str
    market_address: str
    utilization: Decimal
    kink_target_utilization: Decimal
    kink_score: Decimal
    borrow_apy: Decimal
    borrow_apy_delta: Decimal | None
    available_liquidity_usd: Decimal
    total_supply_usd: Decimal
    available_liquidity_ratio: Decimal


@dataclass(frozen=True)
class PositionRiskRow:
    """Derived position-level spread-compression metric at a shared as-of timestamp."""

    as_of_ts_utc: datetime
    position_key: str
    wallet_id: int
    market_id: int
    supply_apy: Decimal
    reward_apy: Decimal
    borrow_apy: Decimal
    net_spread_apy: Decimal


@dataclass(frozen=True)
class RiskComputationResult:
    """Risk scoring output for downstream alert generation and watchlists."""

    as_of_ts_utc: datetime
    market_rows: list[MarketRiskRow]
    position_rows: list[PositionRiskRow]


@dataclass(frozen=True)
class _MarketSnapshotRow:
    """Canonical market snapshot payload with dimensions for risk scoring."""

    market_id: int
    protocol_code: str
    chain_code: str
    market_address: str
    utilization: Decimal
    borrow_apy: Decimal
    available_liquidity_usd: Decimal | None
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    irm_params_json: dict[str, Any] | None


def compute_kink_risk_score(*, utilization: Decimal, kink_target_utilization: Decimal) -> Decimal:
    """Normalize utilization against kink target so values near/above 1.0 are risky."""

    if kink_target_utilization <= ZERO:
        return utilization
    return utilization / kink_target_utilization


def compute_available_liquidity_ratio(
    *,
    available_liquidity_usd: Decimal,
    total_supply_usd: Decimal,
) -> Decimal:
    """Compute available-liquidity ratio with safe zero-supply handling."""

    if total_supply_usd <= ZERO:
        return ZERO
    return available_liquidity_usd / total_supply_usd


def compute_net_spread_apy(
    *,
    supply_apy: Decimal,
    reward_apy: Decimal,
    borrow_apy: Decimal,
) -> Decimal:
    """Compute position net spread APY used for spread-compression alerts."""

    return supply_apy + reward_apy - borrow_apy


def top_markets_by_kink_risk(
    result: RiskComputationResult,
    *,
    limit: int = 20,
) -> list[MarketRiskRow]:
    """Return highest-risk markets ordered by kink score and utilization."""

    return sorted(
        result.market_rows,
        key=lambda row: (row.kink_score, row.utilization, row.market_id),
        reverse=True,
    )[:limit]


def top_positions_by_worst_net_spread(
    result: RiskComputationResult,
    *,
    limit: int = 20,
) -> list[PositionRiskRow]:
    """Return positions with the tightest/worst net spread."""

    return sorted(
        result.position_rows,
        key=lambda row: (row.net_spread_apy, row.position_key),
    )[:limit]


def _normalize_ratio_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None

    if parsed <= ZERO:
        return None
    if parsed > Decimal("1") and parsed <= Decimal("100"):
        return parsed / Decimal("100")
    return parsed


def extract_kink_target_from_irm(irm_params_json: Any) -> Decimal | None:
    """Best-effort extraction of protocol kink/optimal utilization from IRM metadata."""

    if not isinstance(irm_params_json, dict):
        return None

    candidate_keys = (
        "kink",
        "kink_utilization",
        "kink_utilization_ratio",
        "optimal_usage_ratio",
        "optimal_utilization",
        "utilization_optimal",
        "u_opt",
    )

    for key in candidate_keys:
        value = irm_params_json.get(key)
        parsed = _normalize_ratio_decimal(value)
        if parsed is not None:
            return parsed

    return None


class RiskEngine:
    """Compute market/position risk signals from canonical snapshots."""

    def __init__(self, session: Session, *, thresholds: RiskThresholdsConfig) -> None:
        self.session = session
        self.thresholds = thresholds

    def compute_for_date(self, *, business_date: date) -> RiskComputationResult:
        """Score risk at the latest common snapshot timestamp within a Denver business day."""

        start_utc, end_utc = denver_business_bounds_utc(business_date)
        as_of_ts_utc = self._resolve_common_snapshot_ts(start_utc=start_utc, end_utc=end_utc)
        if as_of_ts_utc is None:
            raise ValueError(
                "no common market/position snapshots found for business_date="
                f"{business_date.isoformat()}"
            )
        return self._compute(as_of_ts_utc=as_of_ts_utc)

    def compute_as_of(self, *, as_of_ts_utc: datetime) -> RiskComputationResult:
        """Score risk at the latest common snapshot timestamp at or before requested as-of."""

        normalized_as_of = as_of_ts_utc
        if normalized_as_of.tzinfo is None:
            normalized_as_of = normalized_as_of.replace(tzinfo=UTC)
        else:
            normalized_as_of = normalized_as_of.astimezone(UTC)

        as_of_ts = self._resolve_common_snapshot_ts(
            start_utc=None,
            end_utc=normalized_as_of,
        )
        if as_of_ts is None:
            raise ValueError(
                "no common market/position snapshots found at or before "
                f"{normalized_as_of.isoformat()}"
            )
        return self._compute(as_of_ts_utc=as_of_ts)

    def _resolve_common_snapshot_ts(
        self,
        *,
        start_utc: datetime | None,
        end_utc: datetime,
    ) -> datetime | None:
        market_query = select(MarketSnapshot.as_of_ts_utc.label("ts")).where(
            MarketSnapshot.as_of_ts_utc <= end_utc
        )
        position_query = select(PositionSnapshot.as_of_ts_utc.label("ts")).where(
            PositionSnapshot.as_of_ts_utc <= end_utc
        )

        if start_utc is not None:
            market_query = market_query.where(
                MarketSnapshot.as_of_ts_utc >= start_utc,
                MarketSnapshot.as_of_ts_utc < end_utc,
            )
            position_query = position_query.where(
                PositionSnapshot.as_of_ts_utc >= start_utc,
                PositionSnapshot.as_of_ts_utc < end_utc,
            )

        market_ts_subq = market_query.group_by(MarketSnapshot.as_of_ts_utc).subquery()
        position_ts_subq = position_query.group_by(PositionSnapshot.as_of_ts_utc).subquery()

        return self.session.scalar(
            select(func.max(market_ts_subq.c.ts)).select_from(
                market_ts_subq.join(position_ts_subq, position_ts_subq.c.ts == market_ts_subq.c.ts)
            )
        )

    def _compute(self, *, as_of_ts_utc: datetime) -> RiskComputationResult:
        market_rows = self._load_market_risk_rows(as_of_ts_utc=as_of_ts_utc)
        position_rows = self._load_position_risk_rows(as_of_ts_utc=as_of_ts_utc)
        return RiskComputationResult(
            as_of_ts_utc=as_of_ts_utc,
            market_rows=market_rows,
            position_rows=position_rows,
        )

    def _load_market_risk_rows(self, *, as_of_ts_utc: datetime) -> list[MarketRiskRow]:
        raw_rows = self.session.execute(
            select(
                MarketSnapshot.market_id,
                Protocol.protocol_code,
                Chain.chain_code,
                Market.market_address,
                MarketSnapshot.utilization,
                MarketSnapshot.borrow_apy,
                MarketSnapshot.available_liquidity_usd,
                MarketSnapshot.total_supply_usd,
                MarketSnapshot.total_borrow_usd,
                MarketSnapshot.irm_params_json,
                MarketSnapshot.source,
            )
            .join(Market, Market.market_id == MarketSnapshot.market_id)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(MarketSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()

        deduped_by_market: dict[int, tuple[int, _MarketSnapshotRow]] = {}
        for row in raw_rows:
            source_priority = SOURCE_PRIORITY.get(row[10], 99)
            market_row = _MarketSnapshotRow(
                market_id=row[0],
                protocol_code=row[1],
                chain_code=row[2],
                market_address=row[3],
                utilization=row[4],
                borrow_apy=row[5],
                available_liquidity_usd=row[6],
                total_supply_usd=row[7],
                total_borrow_usd=row[8],
                irm_params_json=row[9] if isinstance(row[9], dict) else None,
            )
            existing = deduped_by_market.get(market_row.market_id)
            if existing is None or source_priority < existing[0]:
                deduped_by_market[market_row.market_id] = (source_priority, market_row)

        rows: list[MarketRiskRow] = []
        max_gap = timedelta(hours=self.thresholds.borrow_spike.max_lookback_hours)

        for market_id in sorted(deduped_by_market):
            market_row = deduped_by_market[market_id][1]
            irm_kink_target = extract_kink_target_from_irm(market_row.irm_params_json)
            kink_target = irm_kink_target or self._kink_target_for_protocol(
                market_row.protocol_code
            )
            kink_score = compute_kink_risk_score(
                utilization=market_row.utilization,
                kink_target_utilization=kink_target,
            )

            previous = self._load_previous_borrow_snapshot(
                market_id=market_row.market_id,
                as_of_ts_utc=as_of_ts_utc,
            )
            borrow_apy_delta: Decimal | None = None
            if previous is not None and (as_of_ts_utc - previous.as_of_ts_utc) <= max_gap:
                borrow_apy_delta = market_row.borrow_apy - previous.borrow_apy

            available_liquidity_usd = (
                market_row.available_liquidity_usd
                if market_row.available_liquidity_usd is not None
                else max(market_row.total_supply_usd - market_row.total_borrow_usd, ZERO)
            )
            available_liquidity_ratio = compute_available_liquidity_ratio(
                available_liquidity_usd=available_liquidity_usd,
                total_supply_usd=market_row.total_supply_usd,
            )

            rows.append(
                MarketRiskRow(
                    as_of_ts_utc=as_of_ts_utc,
                    market_id=market_row.market_id,
                    protocol_code=market_row.protocol_code,
                    chain_code=market_row.chain_code,
                    market_address=market_row.market_address,
                    utilization=market_row.utilization,
                    kink_target_utilization=kink_target,
                    kink_score=kink_score,
                    borrow_apy=market_row.borrow_apy,
                    borrow_apy_delta=borrow_apy_delta,
                    available_liquidity_usd=available_liquidity_usd,
                    total_supply_usd=market_row.total_supply_usd,
                    available_liquidity_ratio=available_liquidity_ratio,
                )
            )

        return rows

    def _kink_target_for_protocol(self, protocol_code: str) -> Decimal:
        overrides = self.thresholds.kink.protocol_target_overrides
        return overrides.get(protocol_code, self.thresholds.kink.default_target_utilization)

    def _load_previous_borrow_snapshot(
        self,
        *,
        market_id: int,
        as_of_ts_utc: datetime,
    ) -> PreviousBorrowSnapshot | None:
        rows = self.session.execute(
            select(
                MarketSnapshot.as_of_ts_utc,
                MarketSnapshot.borrow_apy,
                MarketSnapshot.source,
            )
            .where(
                MarketSnapshot.market_id == market_id,
                MarketSnapshot.as_of_ts_utc < as_of_ts_utc,
            )
            .order_by(MarketSnapshot.as_of_ts_utc.desc())
        ).all()
        if not rows:
            return None

        first_ts = rows[0][0]
        best_row = rows[0]
        best_priority = SOURCE_PRIORITY.get(best_row[2], 99)

        for row in rows[1:]:
            row_ts = row[0]
            if row_ts != first_ts:
                break
            priority = SOURCE_PRIORITY.get(row[2], 99)
            if priority < best_priority:
                best_row = row
                best_priority = priority

        return PreviousBorrowSnapshot(as_of_ts_utc=best_row[0], borrow_apy=best_row[1])

    def _load_position_risk_rows(self, *, as_of_ts_utc: datetime) -> list[PositionRiskRow]:
        rows = self.session.execute(
            select(
                PositionSnapshot.position_key,
                PositionSnapshot.wallet_id,
                PositionSnapshot.market_id,
                PositionSnapshot.supply_apy,
                PositionSnapshot.reward_apy,
                PositionSnapshot.borrow_apy,
            ).where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()

        output: list[PositionRiskRow] = []
        for row in rows:
            net_spread_apy = compute_net_spread_apy(
                supply_apy=row[3],
                reward_apy=row[4],
                borrow_apy=row[5],
            )
            output.append(
                PositionRiskRow(
                    as_of_ts_utc=as_of_ts_utc,
                    position_key=row[0],
                    wallet_id=row[1],
                    market_id=row[2],
                    supply_apy=row[3],
                    reward_apy=row[4],
                    borrow_apy=row[5],
                    net_spread_apy=net_spread_apy,
                )
            )

        return sorted(output, key=lambda item: item.position_key)
