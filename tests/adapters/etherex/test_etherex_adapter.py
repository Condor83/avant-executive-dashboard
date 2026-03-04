"""Etherex adapter unit tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.etherex.adapter import EtherexAdapter, EtherexPosition
from core.config import MarketsConfig

POOL = "0x1659819f2b07a492e5efc20ecf33617e655daefb"
MANAGER = "0xb56542bf1822c3fea210d920c8ab0fcaabcd1798"
TOKEN0 = "0x37c44fc08e403efc0946c0623cb1164a52ce1576"  # avUSD
TOKEN1 = "0x5c247948fd58bb02b6c4678d9940f5e6b9af1127"  # savUSD
WALLET = "0xFC6e0C2026da9404727d0A2231835dCB44f5155F"


class _StubEtherexClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "linea"
        return 123456

    def get_pool_token0(self, chain_code: str, pool_address: str) -> str:
        assert chain_code == "linea"
        assert pool_address == POOL
        return TOKEN0

    def get_pool_token1(self, chain_code: str, pool_address: str) -> str:
        assert chain_code == "linea"
        assert pool_address == POOL
        return TOKEN1

    def get_pool_fee(self, chain_code: str, pool_address: str) -> int:
        assert chain_code == "linea"
        assert pool_address == POOL
        return 250

    def get_pool_slot0(self, chain_code: str, pool_address: str) -> tuple[int, int]:
        assert chain_code == "linea"
        assert pool_address == POOL
        return 73905608084582636946439715380, -1391

    def get_balance_of(self, chain_code: str, manager_address: str, owner: str) -> int:
        assert chain_code == "linea"
        assert manager_address == MANAGER
        assert owner == WALLET.lower()
        return 2

    def get_token_of_owner_by_index(
        self,
        chain_code: str,
        manager_address: str,
        owner: str,
        index: int,
    ) -> int:
        assert chain_code == "linea"
        assert manager_address == MANAGER
        assert owner == WALLET.lower()
        if index == 0:
            return 26116
        if index == 1:
            return 26118
        raise AssertionError(f"unexpected owner index: {index}")

    def get_position(self, chain_code: str, manager_address: str, token_id: int) -> EtherexPosition:
        assert chain_code == "linea"
        assert manager_address == MANAGER
        if token_id == 26116:
            return EtherexPosition(
                token0_address=TOKEN0,
                token1_address=TOKEN1,
                fee=5,
                tick_lower=-1385,
                tick_upper=-1380,
                liquidity=37330396167471144357248490,
                tokens_owed_0=0,
                tokens_owed_1=0,
            )
        if token_id == 26118:
            return EtherexPosition(
                token0_address=TOKEN0,
                token1_address=TOKEN1,
                fee=5,
                tick_lower=-1385,
                tick_upper=-1380,
                liquidity=3695709220579643291367600517,
                tokens_owed_0=0,
                tokens_owed_1=0,
            )
        raise AssertionError(f"unexpected token id: {token_id}")


class _PoolMismatchClient(_StubEtherexClient):
    def get_pool_token1(self, chain_code: str, pool_address: str) -> str:
        return "0x0000000000000000000000000000000000000001"


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "spark": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
            "traderjoe_lp": {},
            "stakedao": {},
            "etherex": {
                "linea": {
                    "wallets": [WALLET],
                    "pools": [
                        {
                            "pool_address": POOL,
                            "position_manager_address": MANAGER,
                            "token0_address": TOKEN0,
                            "token0_symbol": "avUSD",
                            "token0_decimals": 18,
                            "token1_address": TOKEN1,
                            "token1_symbol": "savUSD",
                            "token1_decimals": 18,
                            "fee": 5,
                            "include_in_yield": False,
                            "capital_bucket": "market_stability_ops",
                        }
                    ],
                }
            },
        }
    )


def test_collect_positions_decodes_linea_etherex_nfts() -> None:
    adapter = EtherexAdapter(markets_config=_config(), rpc_client=_StubEtherexClient())
    as_of = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=as_of,
        prices_by_token={
            ("avalanche", "symbol:AVUSD"): Decimal("1"),
            ("avalanche", "symbol:SAVUSD"): Decimal("1"),
        },
    )

    assert not issues
    assert len(positions) == 2

    total_supplied_amount = sum((position.supplied_amount for position in positions), Decimal("0"))
    total_supplied_usd = sum((position.supplied_usd for position in positions), Decimal("0"))

    assert abs(total_supplied_amount - Decimal("1000000")) < Decimal("0.000001")
    assert abs(total_supplied_usd - Decimal("1000000")) < Decimal("0.000001")
    assert all(position.wallet_address == WALLET.lower() for position in positions)
    assert all(position.market_ref == POOL for position in positions)
    assert all(position.borrowed_usd == Decimal("0") for position in positions)


def test_collect_positions_emits_pool_mismatch_issue() -> None:
    adapter = EtherexAdapter(markets_config=_config(), rpc_client=_PoolMismatchClient())
    as_of = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=as_of,
        prices_by_token={
            ("linea", TOKEN0): Decimal("1"),
            ("linea", TOKEN1): Decimal("1"),
        },
    )

    assert not positions
    assert any(issue.error_type == "etherex_pool_mismatch" for issue in issues)
