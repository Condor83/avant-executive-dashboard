"""Daily yield computation from position snapshots."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.fee_engine import FeeBreakdown, apply_fee_waterfall
from core.db.models import DataQuality, Market, PositionSnapshot, WalletProductMap, YieldDaily
from core.settings import get_settings

METHOD_APY_PRORATED_SOD_EOD = "apy_prorated_sod_eod"
DENVER_TZ = ZoneInfo("America/Denver")
DAYS_PER_YEAR = Decimal("365")
ZERO = Decimal("0")
FULL_CONFIDENCE = Decimal("1")
PARTIAL_CONFIDENCE = Decimal("0.5")


@dataclass(frozen=True)
class BoundarySelection:
    """Chosen SOD/EOD snapshot timestamps for a business day."""

    sod_ts_utc: datetime | None
    eod_ts_utc: datetime | None
    used_sod_fallback: bool
    used_eod_fallback: bool
    used_latest_snapshot_fallback: bool


@dataclass(frozen=True)
class BoundaryCheckResult:
    """Business-day boundary readiness and selected snapshot timestamps."""

    business_date: date
    sod_exact_ts_utc: datetime
    eod_exact_ts_utc: datetime
    sod_ts_utc: datetime | None
    eod_ts_utc: datetime | None
    exact_sod_present: bool
    exact_eod_present: bool
    used_sod_fallback: bool
    used_eod_fallback: bool

    @property
    def ready_for_signoff(self) -> bool:
        """True when both exact Denver boundaries are present."""

        return self.exact_sod_present and self.exact_eod_present


@dataclass(frozen=True)
class SnapshotPoint:
    """Snapshot facts required for daily yield math."""

    wallet_id: int
    market_id: int
    supplied_usd: Decimal
    borrowed_usd: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    reward_apy: Decimal


@dataclass(frozen=True)
class YieldDailyRow:
    """Yield row persisted into the derived table."""

    business_date: date
    wallet_id: int | None
    product_id: int | None
    protocol_id: int | None
    market_id: int | None
    position_key: str | None
    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal
    avg_equity_usd: Decimal | None
    gross_roe: Decimal | None
    post_strategy_fee_roe: Decimal | None
    net_roe: Decimal | None
    avant_gop_roe: Decimal | None
    method: str
    confidence_score: Decimal | None

    def as_insert_dict(self) -> dict[str, object]:
        """Convert to dictionary for SQLAlchemy bulk insert."""

        return {
            "business_date": self.business_date,
            "wallet_id": self.wallet_id,
            "product_id": self.product_id,
            "protocol_id": self.protocol_id,
            "market_id": self.market_id,
            "position_key": self.position_key,
            "gross_yield_usd": self.gross_yield_usd,
            "strategy_fee_usd": self.strategy_fee_usd,
            "avant_gop_usd": self.avant_gop_usd,
            "net_yield_usd": self.net_yield_usd,
            "avg_equity_usd": self.avg_equity_usd,
            "gross_roe": self.gross_roe,
            "post_strategy_fee_roe": self.post_strategy_fee_roe,
            "net_roe": self.net_roe,
            "avant_gop_roe": self.avant_gop_roe,
            "method": self.method,
            "confidence_score": self.confidence_score,
        }


@dataclass(frozen=True)
class YieldComputationSummary:
    """Daily computation result summary for CLI output."""

    business_date: date
    sod_ts_utc: datetime | None
    eod_ts_utc: datetime | None
    position_rows_written: int
    rollup_rows_written: int
    issues_written: int


@dataclass(frozen=True)
class RoeBreakdown:
    """ROE variants derived from daily yield and average equity."""

    avg_equity_usd: Decimal
    gross_roe: Decimal | None
    post_strategy_fee_roe: Decimal | None
    net_roe: Decimal | None
    avant_gop_roe: Decimal | None


def denver_business_bounds_utc(business_date: date) -> tuple[datetime, datetime]:
    """Return UTC bounds for a Denver business date."""

    start_local = datetime.combine(business_date, time.min, tzinfo=DENVER_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def denver_business_date_for_timestamp(ts_utc: datetime) -> date:
    """Map a UTC timestamp to the Denver business date."""

    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=UTC)
    return ts_utc.astimezone(DENVER_TZ).date()


def select_business_day_boundaries(session: Session, *, business_date: date) -> BoundaryCheckResult:
    """Resolve exact and fallback boundaries for a Denver business date."""

    start_utc, end_utc = denver_business_bounds_utc(business_date)
    exact_start = session.scalar(
        select(PositionSnapshot.as_of_ts_utc).where(PositionSnapshot.as_of_ts_utc == start_utc)
    )
    exact_end = session.scalar(
        select(PositionSnapshot.as_of_ts_utc).where(PositionSnapshot.as_of_ts_utc == end_utc)
    )
    day_min, day_max = session.execute(
        select(
            func.min(PositionSnapshot.as_of_ts_utc),
            func.max(PositionSnapshot.as_of_ts_utc),
        ).where(
            PositionSnapshot.as_of_ts_utc >= start_utc,
            PositionSnapshot.as_of_ts_utc < end_utc,
        )
    ).one()

    sod_ts = exact_start or day_min
    eod_ts = exact_end or day_max
    return BoundaryCheckResult(
        business_date=business_date,
        sod_exact_ts_utc=start_utc,
        eod_exact_ts_utc=end_utc,
        sod_ts_utc=sod_ts,
        eod_ts_utc=eod_ts,
        exact_sod_present=exact_start is not None,
        exact_eod_present=exact_end is not None,
        used_sod_fallback=exact_start is None and sod_ts is not None,
        used_eod_fallback=exact_end is None and eod_ts is not None,
    )


def compute_daily_gross_yield(
    *,
    supply_usd_sod: Decimal,
    supply_usd_eod: Decimal,
    borrow_usd_sod: Decimal,
    borrow_usd_eod: Decimal,
    supply_apy_sod: Decimal,
    supply_apy_eod: Decimal,
    reward_apy_sod: Decimal,
    reward_apy_eod: Decimal,
    borrow_apy_sod: Decimal,
    borrow_apy_eod: Decimal,
) -> Decimal:
    """Compute gross daily yield using SOD/EOD APY and balance observations."""

    avg_supply_usd = (supply_usd_sod + supply_usd_eod) / Decimal("2")
    avg_borrow_usd = (borrow_usd_sod + borrow_usd_eod) / Decimal("2")
    avg_supply_apy = (supply_apy_sod + supply_apy_eod) / Decimal("2")
    avg_reward_apy = (reward_apy_sod + reward_apy_eod) / Decimal("2")
    avg_borrow_apy = (borrow_apy_sod + borrow_apy_eod) / Decimal("2")

    daily_supply_interest = avg_supply_usd * avg_supply_apy / DAYS_PER_YEAR
    daily_rewards = avg_supply_usd * avg_reward_apy / DAYS_PER_YEAR
    daily_borrow_cost = avg_borrow_usd * avg_borrow_apy / DAYS_PER_YEAR
    return daily_supply_interest + daily_rewards - daily_borrow_cost


def compute_average_equity_usd(
    *,
    supply_usd_sod: Decimal,
    supply_usd_eod: Decimal,
    borrow_usd_sod: Decimal,
    borrow_usd_eod: Decimal,
) -> Decimal:
    """Compute average equity from SOD/EOD supplied and borrowed balances."""

    equity_sod = supply_usd_sod - borrow_usd_sod
    equity_eod = supply_usd_eod - borrow_usd_eod
    return (equity_sod + equity_eod) / Decimal("2")


def compute_roe_breakdown(
    *,
    gross_yield_usd: Decimal,
    strategy_fee_usd: Decimal,
    net_yield_usd: Decimal,
    avant_gop_usd: Decimal,
    avg_equity_usd: Decimal,
) -> RoeBreakdown:
    """Compute daily ROE variants from yield and average equity denominator."""

    if avg_equity_usd <= ZERO:
        return RoeBreakdown(
            avg_equity_usd=avg_equity_usd,
            gross_roe=None,
            post_strategy_fee_roe=None,
            net_roe=None,
            avant_gop_roe=None,
        )

    post_strategy_fee_yield = gross_yield_usd - strategy_fee_usd
    return RoeBreakdown(
        avg_equity_usd=avg_equity_usd,
        gross_roe=gross_yield_usd / avg_equity_usd,
        post_strategy_fee_roe=post_strategy_fee_yield / avg_equity_usd,
        net_roe=net_yield_usd / avg_equity_usd,
        avant_gop_roe=avant_gop_usd / avg_equity_usd,
    )


def build_rollup_rows(
    *,
    business_date: date,
    method: str,
    position_rows: list[YieldDailyRow],
) -> list[YieldDailyRow]:
    """Build wallet/product/protocol/total rollups from position rows."""

    wallet_totals: defaultdict[int, list[Decimal]] = defaultdict(
        lambda: [ZERO, ZERO, ZERO, ZERO, ZERO]
    )
    product_totals: defaultdict[int, list[Decimal]] = defaultdict(
        lambda: [ZERO, ZERO, ZERO, ZERO, ZERO]
    )
    protocol_totals: defaultdict[int, list[Decimal]] = defaultdict(
        lambda: [ZERO, ZERO, ZERO, ZERO, ZERO]
    )
    total = [ZERO, ZERO, ZERO, ZERO, ZERO]

    def _build_rollup_row(
        *,
        wallet_id: int | None,
        product_id: int | None,
        protocol_id: int | None,
        values: list[Decimal],
    ) -> YieldDailyRow:
        roe = compute_roe_breakdown(
            gross_yield_usd=values[0],
            strategy_fee_usd=values[1],
            net_yield_usd=values[3],
            avant_gop_usd=values[2],
            avg_equity_usd=values[4],
        )
        return YieldDailyRow(
            business_date=business_date,
            wallet_id=wallet_id,
            product_id=product_id,
            protocol_id=protocol_id,
            market_id=None,
            position_key=None,
            gross_yield_usd=values[0],
            strategy_fee_usd=values[1],
            avant_gop_usd=values[2],
            net_yield_usd=values[3],
            avg_equity_usd=roe.avg_equity_usd,
            gross_roe=roe.gross_roe,
            post_strategy_fee_roe=roe.post_strategy_fee_roe,
            net_roe=roe.net_roe,
            avant_gop_roe=roe.avant_gop_roe,
            method=method,
            confidence_score=None,
        )

    for row in position_rows:
        total[0] += row.gross_yield_usd
        total[1] += row.strategy_fee_usd
        total[2] += row.avant_gop_usd
        total[3] += row.net_yield_usd
        total[4] += row.avg_equity_usd or ZERO

        if row.wallet_id is not None:
            bucket = wallet_totals[row.wallet_id]
            bucket[0] += row.gross_yield_usd
            bucket[1] += row.strategy_fee_usd
            bucket[2] += row.avant_gop_usd
            bucket[3] += row.net_yield_usd
            bucket[4] += row.avg_equity_usd or ZERO

        if row.product_id is not None:
            bucket = product_totals[row.product_id]
            bucket[0] += row.gross_yield_usd
            bucket[1] += row.strategy_fee_usd
            bucket[2] += row.avant_gop_usd
            bucket[3] += row.net_yield_usd
            bucket[4] += row.avg_equity_usd or ZERO

        if row.protocol_id is not None:
            bucket = protocol_totals[row.protocol_id]
            bucket[0] += row.gross_yield_usd
            bucket[1] += row.strategy_fee_usd
            bucket[2] += row.avant_gop_usd
            bucket[3] += row.net_yield_usd
            bucket[4] += row.avg_equity_usd or ZERO

    rows: list[YieldDailyRow] = []

    for wallet_id, values in sorted(wallet_totals.items()):
        rows.append(
            _build_rollup_row(wallet_id=wallet_id, product_id=None, protocol_id=None, values=values)
        )

    for product_id, values in sorted(product_totals.items()):
        rows.append(
            _build_rollup_row(
                wallet_id=None, product_id=product_id, protocol_id=None, values=values
            )
        )

    for protocol_id, values in sorted(protocol_totals.items()):
        rows.append(
            _build_rollup_row(
                wallet_id=None, product_id=None, protocol_id=protocol_id, values=values
            )
        )

    rows.append(_build_rollup_row(wallet_id=None, product_id=None, protocol_id=None, values=total))
    return rows


class YieldEngine:
    """Compute and persist daily yield rows from canonical snapshots."""

    def __init__(
        self,
        session: Session,
        *,
        boundary_policy: str = "auto",
        now_utc_provider: Callable[[], datetime] | None = None,
    ) -> None:
        if boundary_policy not in ("auto", "in_day", "latest_snapshot"):
            raise ValueError(
                "boundary_policy must be one of: auto, in_day, latest_snapshot"
            )
        self.session = session
        self.boundary_policy = boundary_policy
        self.now_utc_provider = now_utc_provider or (lambda: datetime.now(UTC))

    def compute_daily(self, *, business_date: date) -> YieldComputationSummary:
        """Compute daily yield rows for the provided Denver business date."""

        start_utc, end_utc = denver_business_bounds_utc(business_date)
        boundary = self._select_boundaries(start_utc=start_utc, end_utc=end_utc)
        issues: list[DataQuality] = []

        if boundary.sod_ts_utc is None or boundary.eod_ts_utc is None:
            issues.append(
                self._build_issue(
                    as_of_ts_utc=start_utc,
                    error_type="daily_boundaries_missing",
                    error_message=(
                        "unable to resolve SOD/EOD snapshot boundaries inside Denver business day"
                    ),
                    payload_json={"business_date": business_date.isoformat()},
                )
            )
            self._replace_daily_rows(business_date=business_date, rows=[])
            issues_written = self._write_issues(issues)
            return YieldComputationSummary(
                business_date=business_date,
                sod_ts_utc=boundary.sod_ts_utc,
                eod_ts_utc=boundary.eod_ts_utc,
                position_rows_written=0,
                rollup_rows_written=0,
                issues_written=issues_written,
            )

        if boundary.used_latest_snapshot_fallback:
            issues.append(
                self._build_issue(
                    as_of_ts_utc=boundary.eod_ts_utc,
                    error_type="daily_latest_snapshot_fallback_used",
                    error_message=(
                        "exact day boundaries missing; used latest available snapshot "
                        "for both SOD and EOD"
                    ),
                    payload_json={
                        "business_date": business_date.isoformat(),
                        "boundary_policy": self.boundary_policy,
                    },
                )
            )
        else:
            if boundary.used_sod_fallback:
                issues.append(
                    self._build_issue(
                        as_of_ts_utc=boundary.sod_ts_utc,
                        error_type="daily_sod_fallback_used",
                        error_message=(
                            "SOD exact midnight snapshot missing; used earliest in-day snapshot"
                        ),
                        payload_json={"business_date": business_date.isoformat()},
                    )
                )

            if boundary.used_eod_fallback:
                issues.append(
                    self._build_issue(
                        as_of_ts_utc=boundary.eod_ts_utc,
                        error_type="daily_eod_fallback_used",
                        error_message=(
                            "EOD exact midnight snapshot missing; used latest in-day snapshot"
                        ),
                        payload_json={"business_date": business_date.isoformat()},
                    )
                )

        sod_rows = self._load_snapshot_map(boundary.sod_ts_utc)
        eod_rows = self._load_snapshot_map(boundary.eod_ts_utc)
        position_rows = self._build_position_rows(
            business_date=business_date,
            sod_rows=sod_rows,
            eod_rows=eod_rows,
            has_distinct_boundaries=boundary.sod_ts_utc != boundary.eod_ts_utc,
            issues=issues,
        )
        rollup_rows = build_rollup_rows(
            business_date=business_date,
            method=METHOD_APY_PRORATED_SOD_EOD,
            position_rows=position_rows,
        )

        self._replace_daily_rows(business_date=business_date, rows=position_rows + rollup_rows)
        issues_written = self._write_issues(issues)

        return YieldComputationSummary(
            business_date=business_date,
            sod_ts_utc=boundary.sod_ts_utc,
            eod_ts_utc=boundary.eod_ts_utc,
            position_rows_written=len(position_rows),
            rollup_rows_written=len(rollup_rows),
            issues_written=issues_written,
        )

    def _select_boundaries(self, *, start_utc: datetime, end_utc: datetime) -> BoundarySelection:
        business_date = start_utc.astimezone(DENVER_TZ).date()
        check = select_business_day_boundaries(self.session, business_date=business_date)
        sod_ts = check.sod_ts_utc
        eod_ts = check.eod_ts_utc
        use_latest_snapshot = self._should_use_latest_snapshot_fallback(
            business_date=business_date,
            boundary_check=check,
        )
        if use_latest_snapshot:
            latest_ts = self.session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
            if latest_ts is not None:
                sod_ts = latest_ts
                eod_ts = latest_ts
        return BoundarySelection(
            sod_ts_utc=sod_ts,
            eod_ts_utc=eod_ts,
            used_sod_fallback=check.used_sod_fallback,
            used_eod_fallback=check.used_eod_fallback,
            used_latest_snapshot_fallback=use_latest_snapshot and sod_ts is not None,
        )

    def _should_use_latest_snapshot_fallback(
        self,
        *,
        business_date: date,
        boundary_check: BoundaryCheckResult,
    ) -> bool:
        if boundary_check.ready_for_signoff:
            return False
        if self.boundary_policy == "latest_snapshot":
            return True
        if self.boundary_policy == "in_day":
            return False

        app_env = get_settings().app_env.strip().lower()
        if app_env == "prod":
            return False
        now_utc = self.now_utc_provider()
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=UTC)
        current_denver_date = now_utc.astimezone(DENVER_TZ).date()
        return business_date < current_denver_date

    def _load_snapshot_map(self, as_of_ts_utc: datetime) -> dict[str, SnapshotPoint]:
        rows = self.session.execute(
            select(
                PositionSnapshot.position_key,
                PositionSnapshot.wallet_id,
                PositionSnapshot.market_id,
                PositionSnapshot.supplied_usd,
                PositionSnapshot.borrowed_usd,
                PositionSnapshot.supply_apy,
                PositionSnapshot.borrow_apy,
                PositionSnapshot.reward_apy,
            ).where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()

        return {
            position_key: SnapshotPoint(
                wallet_id=wallet_id,
                market_id=market_id,
                supplied_usd=supplied_usd,
                borrowed_usd=borrowed_usd,
                supply_apy=supply_apy,
                borrow_apy=borrow_apy,
                reward_apy=reward_apy,
            )
            for (
                position_key,
                wallet_id,
                market_id,
                supplied_usd,
                borrowed_usd,
                supply_apy,
                borrow_apy,
                reward_apy,
            ) in rows
        }

    def _build_position_rows(
        self,
        *,
        business_date: date,
        sod_rows: dict[str, SnapshotPoint],
        eod_rows: dict[str, SnapshotPoint],
        has_distinct_boundaries: bool,
        issues: list[DataQuality],
    ) -> list[YieldDailyRow]:
        business_start_utc, _ = denver_business_bounds_utc(business_date)
        wallet_to_product: dict[int, int] = {
            wallet_id: product_id
            for wallet_id, product_id in self.session.execute(
                select(WalletProductMap.wallet_id, WalletProductMap.product_id)
            ).all()
        }
        market_to_protocol: dict[int, int] = {
            market_id: protocol_id
            for market_id, protocol_id in self.session.execute(
                select(Market.market_id, Market.protocol_id)
            ).all()
        }

        missing_product_wallets: set[int] = set()
        missing_protocol_markets: set[int] = set()

        rows: list[YieldDailyRow] = []
        for position_key in sorted(set(sod_rows) | set(eod_rows)):
            sod = sod_rows.get(position_key)
            eod = eod_rows.get(position_key)
            base_point = sod or eod
            if base_point is None:
                continue

            wallet_id = base_point.wallet_id
            market_id = base_point.market_id
            product_id = wallet_to_product.get(wallet_id)
            protocol_id = market_to_protocol.get(market_id)

            if product_id is None and wallet_id not in missing_product_wallets:
                missing_product_wallets.add(wallet_id)
                issues.append(
                    self._build_issue(
                        as_of_ts_utc=business_start_utc,
                        error_type="wallet_product_missing",
                        error_message="wallet is missing wallet_product_map entry",
                        payload_json={
                            "business_date": business_date.isoformat(),
                            "wallet_id": wallet_id,
                        },
                    )
                )

            if protocol_id is None and market_id not in missing_protocol_markets:
                missing_protocol_markets.add(market_id)
                issues.append(
                    self._build_issue(
                        as_of_ts_utc=business_start_utc,
                        error_type="market_protocol_missing",
                        error_message="market is missing protocol mapping",
                        payload_json={
                            "business_date": business_date.isoformat(),
                            "market_id": market_id,
                        },
                    )
                )

            gross_yield = compute_daily_gross_yield(
                supply_usd_sod=sod.supplied_usd if sod else ZERO,
                supply_usd_eod=eod.supplied_usd if eod else ZERO,
                borrow_usd_sod=sod.borrowed_usd if sod else ZERO,
                borrow_usd_eod=eod.borrowed_usd if eod else ZERO,
                supply_apy_sod=sod.supply_apy if sod else ZERO,
                supply_apy_eod=eod.supply_apy if eod else ZERO,
                reward_apy_sod=sod.reward_apy if sod else ZERO,
                reward_apy_eod=eod.reward_apy if eod else ZERO,
                borrow_apy_sod=sod.borrow_apy if sod else ZERO,
                borrow_apy_eod=eod.borrow_apy if eod else ZERO,
            )
            fees: FeeBreakdown = apply_fee_waterfall(gross_yield)
            avg_equity_usd = compute_average_equity_usd(
                supply_usd_sod=sod.supplied_usd if sod else ZERO,
                supply_usd_eod=eod.supplied_usd if eod else ZERO,
                borrow_usd_sod=sod.borrowed_usd if sod else ZERO,
                borrow_usd_eod=eod.borrowed_usd if eod else ZERO,
            )
            roe = compute_roe_breakdown(
                gross_yield_usd=fees.gross_yield_usd,
                strategy_fee_usd=fees.strategy_fee_usd,
                net_yield_usd=fees.net_yield_usd,
                avant_gop_usd=fees.avant_gop_usd,
                avg_equity_usd=avg_equity_usd,
            )

            rows.append(
                YieldDailyRow(
                    business_date=business_date,
                    wallet_id=wallet_id,
                    product_id=product_id,
                    protocol_id=protocol_id,
                    market_id=market_id,
                    position_key=position_key,
                    gross_yield_usd=fees.gross_yield_usd,
                    strategy_fee_usd=fees.strategy_fee_usd,
                    avant_gop_usd=fees.avant_gop_usd,
                    net_yield_usd=fees.net_yield_usd,
                    avg_equity_usd=roe.avg_equity_usd,
                    gross_roe=roe.gross_roe,
                    post_strategy_fee_roe=roe.post_strategy_fee_roe,
                    net_roe=roe.net_roe,
                    avant_gop_roe=roe.avant_gop_roe,
                    method=METHOD_APY_PRORATED_SOD_EOD,
                    confidence_score=(
                        FULL_CONFIDENCE
                        if sod and eod and has_distinct_boundaries
                        else PARTIAL_CONFIDENCE
                    ),
                )
            )
        return rows

    def _replace_daily_rows(self, *, business_date: date, rows: list[YieldDailyRow]) -> None:
        self.session.execute(
            delete(YieldDaily).where(
                YieldDaily.business_date == business_date,
                YieldDaily.method == METHOD_APY_PRORATED_SOD_EOD,
            )
        )
        if not rows:
            return

        self.session.execute(insert(YieldDaily).values([row.as_insert_dict() for row in rows]))

    def _build_issue(
        self,
        *,
        as_of_ts_utc: datetime,
        error_type: str,
        error_message: str,
        payload_json: dict[str, object] | None = None,
    ) -> DataQuality:
        return DataQuality(
            as_of_ts_utc=as_of_ts_utc,
            stage="compute_daily",
            protocol_code=None,
            chain_code=None,
            wallet_address=None,
            market_ref=None,
            error_type=error_type,
            error_message=error_message,
            payload_json=payload_json,
        )

    def _write_issues(self, issues: list[DataQuality]) -> int:
        if not issues:
            return 0
        self.session.add_all(issues)
        return len(issues)
