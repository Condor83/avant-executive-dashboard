"""Morpho golden wallet integration tests with deterministic RPC fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from adapters.morpho.adapter import (
    MorphoAdapter,
    MorphoMarketParams,
    MorphoMarketState,
    MorphoPosition,
)
from core.config import MarketsConfig, load_markets_config


class FakeMorphoClient:
    def __init__(
        self,
        *,
        market_states: dict[str, MorphoMarketState],
        market_params: dict[str, MorphoMarketParams],
        positions: dict[tuple[str, str], MorphoPosition],
        borrow_rate_raw: int,
        collateral_decimals: dict[str, int],
    ) -> None:
        self.market_states = market_states
        self.market_params = market_params
        self.positions = positions
        self.borrow_rate_raw = borrow_rate_raw
        self.collateral_decimals = collateral_decimals

    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "ethereum"
        return 22_000_000

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        assert chain_code == "ethereum"
        assert morpho_address
        return self.positions[(market_id, wallet_address)]

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        assert chain_code == "ethereum"
        assert morpho_address
        return self.market_states[market_id]

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        assert chain_code == "ethereum"
        assert morpho_address
        return self.market_params[market_id]

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        assert chain_code == "ethereum"
        assert market_params
        assert market_state
        return self.borrow_rate_raw

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        assert chain_code == "ethereum"
        return self.collateral_decimals[token_address]


def _minimal_morpho_config() -> tuple[MarketsConfig, list[str], list[str], list[str | None]]:
    full = load_markets_config(Path("config/markets.yaml"))
    chain = full.morpho["ethereum"]

    wallets = [wallet.lower() for wallet in chain.wallets[:2]]
    markets = chain.markets[:2]
    market_ids = [market.id.lower() for market in markets]
    pool_ids = [market.defillama_pool_id for market in markets]

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {
                "ethereum": {
                    "morpho": chain.morpho,
                    "wallets": wallets,
                    "markets": [market.model_dump() for market in markets],
                    "vaults": [],
                }
            },
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    return cfg, wallets, market_ids, pool_ids


def test_morpho_golden_wallets_returns_positions_and_market_snapshots() -> None:
    cfg, wallets, market_ids, pool_ids = _minimal_morpho_config()

    usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    syrup = "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
    pyusd = "0x6c3ea9036406852006290770bedfcaba0e23a0e8"

    market_states = {
        market_ids[0]: MorphoMarketState(
            total_supply_assets=2_000_000_000,
            total_supply_shares=2_000_000_000,
            total_borrow_assets=400_000_000,
            total_borrow_shares=400_000_000,
            last_update=1,
            fee=0,
        ),
        market_ids[1]: MorphoMarketState(
            total_supply_assets=1_500_000_000,
            total_supply_shares=1_500_000_000,
            total_borrow_assets=300_000_000,
            total_borrow_shares=300_000_000,
            last_update=1,
            fee=0,
        ),
    }
    market_params = {
        market_ids[0]: MorphoMarketParams(
            loan_token=usdc,
            collateral_token=syrup,
            oracle="0x0000000000000000000000000000000000000001",
            irm="0x0000000000000000000000000000000000000002",
            lltv=915000000000000000,
        ),
        market_ids[1]: MorphoMarketParams(
            loan_token=pyusd,
            collateral_token=syrup,
            oracle="0x0000000000000000000000000000000000000003",
            irm="0x0000000000000000000000000000000000000004",
            lltv=860000000000000000,
        ),
    }
    positions = {
        (market_ids[0], wallets[0]): MorphoPosition(
            supply_shares=500_000_000,
            borrow_shares=80_000_000,
            collateral=200_000_000,
        ),
        (market_ids[1], wallets[0]): MorphoPosition(
            supply_shares=250_000_000,
            borrow_shares=0,
            collateral=0,
        ),
        (market_ids[0], wallets[1]): MorphoPosition(
            supply_shares=0,
            borrow_shares=40_000_000,
            collateral=300_000_000,
        ),
        (market_ids[1], wallets[1]): MorphoPosition(
            supply_shares=100_000_000,
            borrow_shares=20_000_000,
            collateral=50_000_000,
        ),
    }

    adapter = MorphoAdapter(
        markets_config=cfg,
        rpc_client=FakeMorphoClient(
            market_states=market_states,
            market_params=market_params,
            positions=positions,
            borrow_rate_raw=1_584_979_923,
            collateral_decimals={syrup: 6},
        ),
    )

    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    prices = {
        ("ethereum", usdc): Decimal("1"),
        ("ethereum", pyusd): Decimal("1"),
        ("ethereum", syrup): Decimal("1"),
    }

    position_rows, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    market_rows, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(position_rows) == 4
    assert not position_issues
    assert len(market_rows) == 2
    assert not market_issues

    assert {row.wallet_address for row in position_rows} == set(wallets)
    assert {row.market_ref for row in position_rows} == set(market_ids)

    for row in position_rows:
        assert row.supply_apy >= Decimal("0")
        assert row.borrow_apy >= Decimal("0")
        assert row.reward_apy == Decimal("0")

    # If defillama_pool_id is configured, adapter must attach it for downstream fallback.
    by_market = {row.market_ref: row for row in market_rows}
    for market_id, pool_id in zip(market_ids, pool_ids, strict=True):
        assert by_market[market_id].irm_params_json is not None
        assert by_market[market_id].irm_params_json["defillama_pool_id"] == pool_id
