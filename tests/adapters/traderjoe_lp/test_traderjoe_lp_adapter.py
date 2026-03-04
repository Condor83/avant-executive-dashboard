"""Trader Joe LP adapter unit tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.traderjoe_lp.adapter import TraderJoeLpAdapter
from core.config import MarketsConfig

POOL = "0xb768091a8e6ffcdc215767937bd9fb039cb06577"
TOKEN_X = "0x06d47f3fb376649c3a9dafe069b3d6e35572219e"
TOKEN_Y = "0x24de8771bc5ddb3362db529fc3358f2df3a0e346"
WALLET = "0xFC6e0C2026da9404727d0A2231835dCB44f5155F"


class _FakeTraderJoeClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "avalanche"
        return 123456

    def get_token_x(self, chain_code: str, pool_address: str) -> str:
        assert chain_code == "avalanche"
        assert pool_address == POOL
        return TOKEN_X

    def get_token_y(self, chain_code: str, pool_address: str) -> str:
        assert chain_code == "avalanche"
        assert pool_address == POOL
        return TOKEN_Y

    def get_bin(self, chain_code: str, pool_address: str, bin_id: int) -> tuple[int, int]:
        assert chain_code == "avalanche"
        assert pool_address == POOL
        if bin_id == 1:
            return 2 * 10**18, 8 * 10**18
        if bin_id == 2:
            return 1 * 10**18, 1 * 10**18
        raise AssertionError(f"unexpected bin id: {bin_id}")

    def get_bin_total_supply(self, chain_code: str, pool_address: str, bin_id: int) -> int:
        assert chain_code == "avalanche"
        assert pool_address == POOL
        if bin_id == 1:
            return 10 * 10**18
        if bin_id == 2:
            return 1 * 10**18
        raise AssertionError(f"unexpected bin id: {bin_id}")

    def get_bin_balance(
        self,
        chain_code: str,
        pool_address: str,
        wallet_address: str,
        bin_id: int,
    ) -> int:
        assert chain_code == "avalanche"
        assert pool_address == POOL
        assert wallet_address == WALLET.lower()
        if bin_id == 1:
            return 5 * 10**18
        if bin_id == 2:
            return 0
        raise AssertionError(f"unexpected bin id: {bin_id}")


class _TokenMismatchClient(_FakeTraderJoeClient):
    def get_token_y(self, chain_code: str, pool_address: str) -> str:
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
            "traderjoe_lp": {
                "avalanche": {
                    "wallets": [WALLET],
                    "pools": [
                        {
                            "pool_address": POOL,
                            "pool_type": "joe_v2_lb",
                            "token_x_address": TOKEN_X,
                            "token_x_symbol": "savUSD",
                            "token_x_decimals": 18,
                            "token_y_address": TOKEN_Y,
                            "token_y_symbol": "avUSD",
                            "token_y_decimals": 18,
                            "bin_ids": [1, 2],
                            "include_in_yield": False,
                            "capital_bucket": "market_stability_ops",
                        }
                    ],
                }
            },
        }
    )


def test_collect_positions_normalizes_bin_exposure_to_usd() -> None:
    adapter = TraderJoeLpAdapter(markets_config=_config(), rpc_client=_FakeTraderJoeClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=as_of,
        prices_by_token={
            ("avalanche", TOKEN_X): Decimal("1"),
            ("avalanche", TOKEN_Y): Decimal("1"),
        },
    )

    assert not issues
    assert len(positions) == 1

    position = positions[0]
    assert position.protocol_code == "traderjoe_lp"
    assert position.chain_code == "avalanche"
    assert position.wallet_address == WALLET.lower()
    assert position.market_ref == POOL
    assert position.position_key == f"traderjoe_lp:avalanche:{WALLET.lower()}:{POOL}"
    assert position.supplied_amount == Decimal("4")
    assert position.supplied_usd == Decimal("5")
    assert position.borrowed_usd == Decimal("0")
    assert position.equity_usd == Decimal("5")


def test_collect_positions_emits_token_mismatch_issue() -> None:
    adapter = TraderJoeLpAdapter(markets_config=_config(), rpc_client=_TokenMismatchClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=as_of,
        prices_by_token={
            ("avalanche", TOKEN_X): Decimal("1"),
            ("avalanche", TOKEN_Y): Decimal("1"),
        },
    )

    assert len(positions) == 1
    assert any(issue.error_type == "traderjoe_token_mismatch" for issue in issues)
