"""Pendle adapter tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from adapters.pendle import PendleAdapter, PendleMarketMetadata, PendleWalletPosition
from core.config import MarketsConfig


class _StubPendleClient:
    def __init__(
        self,
        *,
        markets: list[PendleMarketMetadata],
        wallet_positions: dict[tuple[int, str], list[PendleWalletPosition]] | None = None,
    ) -> None:
        self.markets = markets
        self.wallet_positions = wallet_positions or {}

    def get_markets(self, *, chain_id: int) -> list[PendleMarketMetadata]:
        return [market for market in self.markets if market.chain_id == chain_id]

    def get_wallet_positions(
        self, *, chain_id: int, wallet_address: str
    ) -> list[PendleWalletPosition]:
        return list(self.wallet_positions.get((chain_id, wallet_address.lower()), []))


def _markets_config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "spark": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "pendle": {
                "ethereum": {
                    "wallets": ["0x1111111111111111111111111111111111111111"],
                    "markets": [
                        {
                            "market_address": "0x2222222222222222222222222222222222222222",
                            "name": "avUSD Pendle 14MAY2026",
                            "expiry": "2026-05-14",
                            "pt_token": {
                                "symbol": "PT-avUSD-14MAY2026",
                                "address": "0x3333333333333333333333333333333333333333",
                                "decimals": 18,
                            },
                            "yt_token": {
                                "symbol": "YT-avUSD-14MAY2026",
                                "address": "0x4444444444444444444444444444444444444444",
                                "decimals": 18,
                            },
                            "underlying_token": {
                                "symbol": "avUSD",
                                "address": "0x5555555555555555555555555555555555555555",
                                "decimals": 18,
                            },
                            "sy_token_address": "0x6666666666666666666666666666666666666666",
                        }
                    ],
                }
            },
            "zest": {},
            "wallet_balances": {},
            "traderjoe_lp": {},
            "stakedao": {},
            "etherex": {},
        }
    )


def test_pendle_adapter_emits_separate_pt_and_yt_rows() -> None:
    adapter = PendleAdapter(
        markets_config=_markets_config(),
        client=_StubPendleClient(
            markets=[
                PendleMarketMetadata(
                    chain_id=1,
                    market_address="0x2222222222222222222222222222222222222222",
                    name="avUSD Pendle 14MAY2026",
                    expiry=datetime(2026, 5, 14, tzinfo=UTC),
                    pt_token_address="0x3333333333333333333333333333333333333333",
                    yt_token_address="0x4444444444444444444444444444444444444444",
                    sy_token_address="0x6666666666666666666666666666666666666666",
                    underlying_token_address="0x5555555555555555555555555555555555555555",
                    liquidity_usd=Decimal("2500000"),
                    total_tvl_usd=Decimal("7000000"),
                    total_pt=Decimal("500000"),
                    total_sy=Decimal("1800000"),
                    underlying_apy=Decimal("0.04"),
                    implied_apy=Decimal("0.07"),
                    pendle_apy=Decimal("0.02"),
                    swap_fee_apy=Decimal("0.01"),
                    aggregated_apy=Decimal("0.03"),
                )
            ],
            wallet_positions={
                (1, "0x1111111111111111111111111111111111111111"): [
                    PendleWalletPosition(
                        chain_id=1,
                        market_address="0x2222222222222222222222222222222222222222",
                        pt_balance=Decimal("1500000000000000000"),
                        pt_valuation_usd=Decimal("1470"),
                        yt_balance=Decimal("3000000000000000000"),
                        yt_valuation_usd=Decimal("210"),
                    )
                ]
            },
        ),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 17, 4, 16, tzinfo=UTC),
        prices_by_token={},
    )

    assert issues == []
    assert len(positions) == 2

    pt_row = next(position for position in positions if position.position_key.endswith(":pt"))
    assert pt_row.supplied_amount == Decimal("1.5")
    assert pt_row.supplied_usd == Decimal("1470")
    assert pt_row.collateral_amount is None
    assert pt_row.borrowed_usd == Decimal("0")
    assert pt_row.supply_apy == Decimal("0")

    yt_row = next(position for position in positions if position.position_key.endswith(":yt"))
    assert yt_row.supplied_amount == Decimal("0")
    assert yt_row.collateral_amount == Decimal("3")
    assert yt_row.collateral_usd == Decimal("210")
    assert yt_row.equity_usd == Decimal("210")
    assert yt_row.supply_apy == Decimal("-0.03")


def test_pendle_adapter_collects_market_snapshots() -> None:
    adapter = PendleAdapter(
        markets_config=_markets_config(),
        client=_StubPendleClient(
            markets=[
                PendleMarketMetadata(
                    chain_id=1,
                    market_address="0x2222222222222222222222222222222222222222",
                    name="avUSD Pendle 14MAY2026",
                    expiry=datetime.combine(date(2026, 5, 14), datetime.min.time(), tzinfo=UTC),
                    pt_token_address="0x3333333333333333333333333333333333333333",
                    yt_token_address="0x4444444444444444444444444444444444444444",
                    sy_token_address="0x6666666666666666666666666666666666666666",
                    underlying_token_address="0x5555555555555555555555555555555555555555",
                    liquidity_usd=Decimal("2372877.56"),
                    total_tvl_usd=Decimal("6723387.28"),
                    total_pt=Decimal("488676.15919477417"),
                    total_sy=Decimal("1899012.0702663884"),
                    underlying_apy=Decimal("0.00"),
                    implied_apy=Decimal("0.08138"),
                    pendle_apy=Decimal("0.02564"),
                    swap_fee_apy=Decimal("0.00726"),
                    aggregated_apy=Decimal("0.05002"),
                )
            ],
        ),
    )

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 17, 4, 16, tzinfo=UTC),
        prices_by_token={},
    )

    assert issues == []
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.market_ref == "0x2222222222222222222222222222222222222222"
    assert snapshot.total_supply_usd == Decimal("6723387.28")
    assert snapshot.available_liquidity_usd == Decimal("2372877.56")
    assert snapshot.total_borrow_usd == Decimal("0")
    assert snapshot.utilization == Decimal("0")
    assert snapshot.supply_apy == Decimal("0.08138")
    assert snapshot.borrow_apy == Decimal("0")
    assert snapshot.irm_params_json is not None
    assert snapshot.irm_params_json["expiry"] == "2026-05-14"
