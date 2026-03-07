"""Dolomite golden wallet integration tests with deterministic RPC fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from adapters.dolomite.adapter import (
    DolomiteAdapter,
    DolomiteMarketIndex,
    DolomiteMarketPar,
    DolomiteSignedWei,
)
from core.config import MarketsConfig, load_markets_config
from core.yields import AvantYieldOracle, DefiLlamaYieldOracle


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
        assert chain_code
        return 4_500_000

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        assert chain_code
        assert margin_address
        return self.token_addresses[market_id]

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        assert chain_code
        assert margin_address
        return max(self.token_addresses) + 1

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        assert chain_code
        return self.price_raw[market_id]

    def get_market_interest_rate(self, chain_code: str, margin_address: str, market_id: int) -> int:
        assert chain_code
        return self.interest_rate_raw[market_id]

    def get_market_current_index(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketIndex:
        assert chain_code
        return self.market_index[market_id]

    def get_market_total_par(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketPar:
        assert chain_code
        return self.total_par[market_id]

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        assert chain_code
        return self.account_wei.get(
            (wallet_address, account_number, market_id),
            DolomiteSignedWei(is_positive=False, value=0),
        )

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        assert chain_code
        assert token_address
        return 18


class _StubAvantYieldOracle:
    def __init__(self, apy: Decimal | None, *, error: Exception | None = None) -> None:
        self.apy = apy
        self.error = error
        self.calls: list[str] = []

    def close(self) -> None:
        return None

    def get_token_apy(self, symbol: str) -> Decimal:
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        if self.apy is None:
            raise RuntimeError("avant unavailable")
        return self.apy


class _StubYieldOracle:
    def __init__(self, apy: Decimal | None, *, error: Exception | None = None) -> None:
        self.apy = apy
        self.error = error
        self.calls: list[str] = []

    def close(self) -> None:
        return None

    def get_pool_apy(self, pool_id: str) -> Decimal:
        self.calls.append(pool_id)
        if self.error is not None:
            raise self.error
        if self.apy is None:
            raise RuntimeError("llama unavailable")
        return self.apy


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


def test_dolomite_prefers_shared_price_map_for_position_and_market_valuation() -> None:
    cfg, wallets, market_ids = _minimal_dolomite_config()
    wallet = wallets[0]
    market_id = market_ids[0]
    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "bera": {
                    "margin": cfg.dolomite["bera"].margin,
                    "wallets": [wallet],
                    "markets": [cfg.dolomite["bera"].markets[0].model_dump()],
                    "account_numbers": [0],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    market = cfg.dolomite["bera"].markets[0]
    assert market.token_address is not None

    client = FakeDolomiteClient(
        token_addresses={market_id: market.token_address},
        price_raw={market_id: 10**30},  # Dolomite internal oracle says $1.00
        interest_rate_raw={market_id: 2_088_661_969},
        market_index={
            market_id: DolomiteMarketIndex(
                borrow=1_050_000_000_000_000_000,
                supply=1_030_000_000_000_000_000,
                last_update=1,
            )
        },
        total_par={market_id: DolomiteMarketPar(borrow=700_000_000, supply=1_200_000_000)},
        account_wei={
            (wallet, 0, market_id): DolomiteSignedWei(is_positive=True, value=350_000_000),
        },
    )
    adapter = DolomiteAdapter(markets_config=cfg, rpc_client=client)
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    shared_price = Decimal("1.153")
    price_map = {("bera", market.token_address.lower()): shared_price}

    position_rows, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of,
        prices_by_token=price_map,
    )
    market_rows, market_issues = adapter.collect_markets(
        as_of_ts_utc=as_of,
        prices_by_token=price_map,
    )

    assert not position_issues
    assert not market_issues
    assert len(position_rows) == 1
    assert len(market_rows) == 1
    assert position_rows[0].supplied_usd == Decimal("350") * shared_price
    assert market_rows[0].total_supply_usd == Decimal("1236") * shared_price
    assert market_rows[0].total_borrow_usd == Decimal("735") * shared_price


def test_dolomite_uses_avant_api_for_avant_native_supplied_token() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "bera": {
                    "margin": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
                    "wallets": [wallet],
                    "markets": [
                        {
                            "id": 47,
                            "symbol": "savUSD",
                            "token_address": "0xa744fe3688291ac3a4a7ec917678783ad9946a1e",
                            "decimals": 18,
                        }
                    ],
                    "account_numbers": [0],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    client = FakeDolomiteClient(
        token_addresses={47: "0xa744fe3688291ac3a4a7ec917678783ad9946a1e"},
        price_raw={47: 10**18},
        interest_rate_raw={47: 0},
        market_index={
            47: DolomiteMarketIndex(
                borrow=1_000_000_000_000_000_000,
                supply=1_000_000_000_000_000_000,
                last_update=1,
            )
        },
        total_par={47: DolomiteMarketPar(borrow=0, supply=10**18)},
        account_wei={(wallet, 0, 47): DolomiteSignedWei(is_positive=True, value=10**18)},
    )
    avant_oracle = _StubAvantYieldOracle(Decimal("0.0745"))
    adapter = DolomiteAdapter(
        markets_config=cfg,
        rpc_client=client,
        avant_yield_oracle=cast(AvantYieldOracle, avant_oracle),
    )

    rows, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert not issues
    assert len(rows) == 1
    assert rows[0].supply_apy == Decimal("0.0745")
    assert avant_oracle.calls == ["savUSD"]


def test_dolomite_avant_api_failure_falls_back_to_protocol_supply_apy() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "bera": {
                    "margin": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
                    "wallets": [wallet],
                    "markets": [
                        {
                            "id": 47,
                            "symbol": "savUSD",
                            "token_address": "0xa744fe3688291ac3a4a7ec917678783ad9946a1e",
                            "decimals": 18,
                        }
                    ],
                    "account_numbers": [0],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    client = FakeDolomiteClient(
        token_addresses={47: "0xa744fe3688291ac3a4a7ec917678783ad9946a1e"},
        price_raw={47: 10**18},
        interest_rate_raw={47: 2_000_000_000},
        market_index={
            47: DolomiteMarketIndex(
                borrow=1_000_000_000_000_000_000,
                supply=1_000_000_000_000_000_000,
                last_update=1,
            )
        },
        total_par={47: DolomiteMarketPar(borrow=600_000_000, supply=1_300_000_000)},
        account_wei={(wallet, 0, 47): DolomiteSignedWei(is_positive=True, value=10**18)},
    )
    avant_oracle = _StubAvantYieldOracle(None, error=RuntimeError("avant unavailable"))
    adapter = DolomiteAdapter(
        markets_config=cfg,
        rpc_client=client,
        avant_yield_oracle=cast(AvantYieldOracle, avant_oracle),
    )

    rows, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert len(rows) == 1
    assert rows[0].supply_apy > Decimal("0")
    assert len(issues) == 1
    assert issues[0].error_type == "dolomite_underlying_apy_fetch_failed"
    assert issues[0].payload_json is not None
    assert issues[0].payload_json["symbol"] == "savUSD"


def test_dolomite_uses_defillama_pool_for_configured_supplied_token() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "ethereum": {
                    "margin": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
                    "wallets": [wallet],
                    "markets": [
                        {
                            "id": 6,
                            "symbol": "weETH",
                            "token_address": "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee",
                            "defillama_pool_id": "46bd2bdf-6d92-4066-b482-e885ee172264",
                            "decimals": 18,
                        }
                    ],
                    "account_numbers": [0],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    client = FakeDolomiteClient(
        token_addresses={6: "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"},
        price_raw={6: 2 * 10**18},
        interest_rate_raw={6: 0},
        market_index={
            6: DolomiteMarketIndex(
                borrow=1_000_000_000_000_000_000,
                supply=1_000_000_000_000_000_000,
                last_update=1,
            )
        },
        total_par={6: DolomiteMarketPar(borrow=0, supply=10**18)},
        account_wei={(wallet, 0, 6): DolomiteSignedWei(is_positive=True, value=10**18)},
    )
    yield_oracle = _StubYieldOracle(Decimal("0.0249"))
    adapter = DolomiteAdapter(
        markets_config=cfg,
        rpc_client=client,
        yield_oracle=cast(DefiLlamaYieldOracle, yield_oracle),
    )

    rows, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert not issues
    assert len(rows) == 1
    assert rows[0].supply_apy == Decimal("0.0249")
    assert yield_oracle.calls == ["46bd2bdf-6d92-4066-b482-e885ee172264"]
