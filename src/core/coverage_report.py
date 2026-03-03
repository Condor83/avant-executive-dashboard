"""Coverage report helpers for adapter ingestion runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import MarketsConfig
from core.db.models import DataQuality, Market, MarketSnapshot, PositionSnapshot
from core.db.models import Protocol as ProtocolModel

ADAPTER_PROTOCOLS = ("morpho", "euler_v2", "dolomite")


@dataclass(frozen=True)
class ExpectedCoverage:
    """Expected coverage surface for a protocol from config."""

    expected_wallet_market_pairs: int
    expected_markets: int


def expected_coverage_from_config(markets_config: MarketsConfig) -> dict[str, ExpectedCoverage]:
    """Return expected protocol coverage from configured wallets/markets."""

    morpho_pairs = sum(
        len(chain.wallets) * len(chain.markets) for chain in markets_config.morpho.values()
    )
    morpho_markets = sum(len(chain.markets) for chain in markets_config.morpho.values())

    euler_pairs = sum(
        len(chain.wallets) * len(chain.vaults) for chain in markets_config.euler_v2.values()
    )
    euler_markets = sum(len(chain.vaults) for chain in markets_config.euler_v2.values())

    dolomite_pairs = sum(
        len(chain.wallets) * len(chain.markets) for chain in markets_config.dolomite.values()
    )
    dolomite_markets = sum(len(chain.markets) for chain in markets_config.dolomite.values())

    return {
        "morpho": ExpectedCoverage(
            expected_wallet_market_pairs=morpho_pairs,
            expected_markets=morpho_markets,
        ),
        "euler_v2": ExpectedCoverage(
            expected_wallet_market_pairs=euler_pairs,
            expected_markets=euler_markets,
        ),
        "dolomite": ExpectedCoverage(
            expected_wallet_market_pairs=dolomite_pairs,
            expected_markets=dolomite_markets,
        ),
    }


def _count_position_rows(session: Session, *, as_of_ts_utc: datetime, protocol_code: str) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(PositionSnapshot)
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
        .where(
            PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
            ProtocolModel.protocol_code == protocol_code,
        )
    )
    return int(count or 0)


def _count_market_rows(session: Session, *, as_of_ts_utc: datetime, protocol_code: str) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(MarketSnapshot)
        .join(Market, Market.market_id == MarketSnapshot.market_id)
        .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
        .where(
            MarketSnapshot.as_of_ts_utc == as_of_ts_utc,
            ProtocolModel.protocol_code == protocol_code,
        )
    )
    return int(count or 0)


def _failure_rows(
    session: Session,
    *,
    as_of_ts_utc: datetime,
    protocol_code: str,
    stage: str,
) -> list[tuple[str, str | None, str | None, int]]:
    rows = session.execute(
        select(
            DataQuality.error_type,
            DataQuality.chain_code,
            DataQuality.wallet_address,
            func.count().label("cnt"),
        )
        .where(
            DataQuality.as_of_ts_utc == as_of_ts_utc,
            DataQuality.protocol_code == protocol_code,
            DataQuality.stage == stage,
        )
        .group_by(DataQuality.error_type, DataQuality.chain_code, DataQuality.wallet_address)
        .order_by(func.count().desc(), DataQuality.error_type)
    ).all()
    return [
        (error_type, chain_code, wallet_address, int(cnt))
        for error_type, chain_code, wallet_address, cnt in rows
    ]


def _missing_price_markets(
    session: Session,
    *,
    as_of_ts_utc: datetime,
    protocol_code: str,
) -> list[tuple[str | None, str | None, int]]:
    rows = session.execute(
        select(
            DataQuality.chain_code,
            DataQuality.market_ref,
            func.count().label("cnt"),
        )
        .where(
            DataQuality.as_of_ts_utc == as_of_ts_utc,
            DataQuality.protocol_code == protocol_code,
            DataQuality.error_type == "price_missing",
        )
        .group_by(DataQuality.chain_code, DataQuality.market_ref)
        .order_by(func.count().desc(), DataQuality.chain_code, DataQuality.market_ref)
    ).all()
    return [(chain_code, market_ref, int(cnt)) for chain_code, market_ref, cnt in rows]


def _pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "n/a"
    return f"{(100.0 * numerator / denominator):.1f}%"


def build_coverage_report(
    *,
    session: Session,
    markets_config: MarketsConfig,
    as_of_ts_utc: datetime,
) -> str:
    """Build text coverage report for configured adapter protocols."""

    expected = expected_coverage_from_config(markets_config)

    lines: list[str] = []
    lines.append(f"coverage report as_of={as_of_ts_utc.isoformat()}")

    for protocol_code in ADAPTER_PROTOCOLS:
        surface = expected[protocol_code]
        position_rows = _count_position_rows(
            session,
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=protocol_code,
        )
        market_rows = _count_market_rows(
            session,
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=protocol_code,
        )

        lines.append(
            "\n"
            f"[{protocol_code}] "
            f"positions={position_rows}/{surface.expected_wallet_market_pairs} "
            f"({_pct(position_rows, surface.expected_wallet_market_pairs)}) "
            f"markets={market_rows}/{surface.expected_markets} "
            f"({_pct(market_rows, surface.expected_markets)})"
        )

        snapshot_failures = _failure_rows(
            session,
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=protocol_code,
            stage="sync_snapshot",
        )
        market_failures = _failure_rows(
            session,
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=protocol_code,
            stage="sync_markets",
        )
        missing_price = _missing_price_markets(
            session,
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=protocol_code,
        )

        if snapshot_failures:
            lines.append("  sync_snapshot failures:")
            for error_type, chain_code, wallet_address, cnt in snapshot_failures[:20]:
                lines.append(
                    "  "
                    f"- {error_type} chain={chain_code or 'unknown'} "
                    f"wallet={wallet_address or 'n/a'} count={cnt}"
                )

        if market_failures:
            lines.append("  sync_markets failures:")
            for error_type, chain_code, wallet_address, cnt in market_failures[:20]:
                lines.append(
                    "  "
                    f"- {error_type} chain={chain_code or 'unknown'} "
                    f"wallet={wallet_address or 'n/a'} count={cnt}"
                )

        if missing_price:
            lines.append("  markets missing prices:")
            for chain_code, market_ref, cnt in missing_price[:20]:
                lines.append(
                    "  "
                    + (
                        f"- chain={chain_code or 'unknown'} "
                        f"market={market_ref or 'unknown'} count={cnt}"
                    )
                )

        if not snapshot_failures and not market_failures and not missing_price:
            lines.append("  no failures recorded")

    return "\n".join(lines)
