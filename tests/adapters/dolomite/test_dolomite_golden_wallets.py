"""Dolomite golden wallet integration tests with deterministic RPC fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from adapters.dolomite.adapter import (
    DolomiteAdapter,
    DolomiteMarketIndex,
    DolomiteMarketPar,
    DolomiteSignedWei,
)
from core.config import MarketsConfig, load_markets_config


class FakeDolomiteClient:
    def __init__(
        self,
        *,
        token_addresses: dict[int, str],
        price_raw: dict[int, int],
        interest_rate_raw: dict[int, int],
        market_index: dict[int, DolomiteMarketIndex],
        total_par: dict[int, DolomiteMarketPar],
        account_wei: dict[tuple[str, int, int], DolomiteSignedWei],
    ) -> None:
        self.token_addresses = token_addresses
        self.price_raw = price_raw
        self.interest_rate_raw = interest_rate_raw
        self.market_index = market_index
        self.total_par = total_par
        self.account_wei = account_wei

    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "bera"
        return 4_500_000

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        assert chain_code == "bera"
        assert margin_address
        return self.token_addresses[market_id]

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        assert chain_code == "bera"
        assert margin_address
        return max(self.token_addresses) + 1

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        assert chain_code == "bera"
        return self.price_raw[market_id]

    def get_market_interest_rate(self, chain_code: str, margin_address: str, market_id: int) -> int:
        assert chain_code == "bera"
        return self.interest_rate_raw[market_id]

    def get_market_current_index(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketIndex:
        assert chain_code == "bera"
        return self.market_index[market_id]

    def get_market_total_par(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketPar:
        assert chain_code == "bera"
        return self.total_par[market_id]

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        assert chain_code == "bera"
        return self.account_wei.get(
            (wallet_address, account_number, market_id),
            DolomiteSignedWei(is_positive=False, value=0),
        )

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        assert chain_code == "bera"
        assert token_address
        return 18


def _minimal_dolomite_config() -> tuple[MarketsConfig, list[str], list[int]]:
    full = load_markets_config(Path("config/markets.yaml"))
    chain = full.dolomite["bera"]

    wallets = [wallet.lower() for wallet in chain.wallets]
    markets = chain.markets[:2]
    market_ids = [market.id for market in markets]

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "bera": {
                    "margin": chain.margin,
                    "wallets": wallets,
                    "markets": [market.model_dump() for market in markets],
                    "account_numbers": [0, 1],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    return cfg, wallets, market_ids


def test_dolomite_golden_wallets_returns_positions_and_market_snapshots() -> None:
    cfg, wallets, market_ids = _minimal_dolomite_config()

    wallet = wallets[0]
    client = FakeDolomiteClient(
        token_addresses={
            market_ids[0]: "0x549943e04f40284185054145c6e4e9568c1d3241",
            market_ids[1]: "0x00000000000000000000000000000000000000b0",
        },
        price_raw={
            market_ids[0]: 10**30,  # $1 for 6-decimal asset
            market_ids[1]: 5 * 10**17,  # $0.5 for 18-decimal asset
        },
        interest_rate_raw={
            market_ids[0]: 2_088_661_969,
            market_ids[1]: 1_500_000_000,
        },
        market_index={
            market_ids[0]: DolomiteMarketIndex(
                borrow=1_050_000_000_000_000_000,
                supply=1_030_000_000_000_000_000,
                last_update=1,
            ),
            market_ids[1]: DolomiteMarketIndex(
                borrow=1_020_000_000_000_000_000,
                supply=1_010_000_000_000_000_000,
                last_update=1,
            ),
        },
        total_par={
            market_ids[0]: DolomiteMarketPar(borrow=700_000_000, supply=1_200_000_000),
            market_ids[1]: DolomiteMarketPar(
                borrow=80_000_000_000_000_000_000,
                supply=200_000_000_000_000_000_000,
            ),
        },
        account_wei={
            (wallet, 0, market_ids[0]): DolomiteSignedWei(is_positive=True, value=350_000_000),
            (wallet, 1, market_ids[1]): DolomiteSignedWei(
                is_positive=False,
                value=12_000_000_000_000_000_000,
            ),
        },
    )

    adapter = DolomiteAdapter(markets_config=cfg, rpc_client=client)
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    position_rows, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token={}
    )
    market_rows, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token={})

    assert len(position_rows) == 2
    assert not position_issues
    assert len(market_rows) == 2
    assert not market_issues

    assert {row.wallet_address for row in position_rows} == {wallet}
    assert {row.market_ref for row in position_rows} == {str(market_id) for market_id in market_ids}
    assert sum(":0:" in row.position_key for row in position_rows) == 1
    assert sum(":1:" in row.position_key for row in position_rows) == 1

    for row in position_rows:
        assert row.supply_apy >= Decimal("0")
        assert row.borrow_apy >= Decimal("0")
        assert row.reward_apy == Decimal("0")
