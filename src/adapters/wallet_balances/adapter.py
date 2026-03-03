"""Wallet balances adapter for EVM chains in `wallet_balances` config."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import MarketsConfig, canonical_address
from core.types import DataQualityIssue, PositionSnapshotInput


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
    """Convert raw on-chain integer amounts into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _strip_0x_hex(value: str) -> str:
    value = value.lower()
    return value[2:] if value.startswith("0x") else value


def _encode_balance_of_call(wallet_address: str) -> str:
    """Encode ERC20 balanceOf(address) calldata for eth_call."""

    selector = "70a08231"
    address_hex = _strip_0x_hex(wallet_address).rjust(64, "0")
    return f"0x{selector}{address_hex}"


def _is_native_token(token_address: str) -> bool:
    normalized = token_address.strip().lower()
    return normalized in {
        "native",
        "eth",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    }


class BalanceClient(Protocol):
    """Protocol for EVM balance reads used by the adapter."""

    def get_erc20_balance(self, chain_code: str, wallet_address: str, token_address: str) -> int:
        """Return raw ERC20 balance for wallet/token on a chain."""

    def get_native_balance(self, chain_code: str, wallet_address: str) -> int:
        """Return raw native balance for wallet on a chain."""


@dataclass(frozen=True)
class WalletTokenTarget:
    """Wallet/token balance target from config."""

    chain_code: str
    wallet_address: str
    token_symbol: str
    token_address: str
    decimals: int


class EvmRpcBalanceClient:
    """Minimal JSON-RPC client for EVM balance queries."""

    def __init__(self, rpc_urls: dict[str, str], timeout_seconds: float = 15.0) -> None:
        self.rpc_urls = {key: value for key, value in rpc_urls.items() if value}
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _rpc(self, chain_code: str, method: str, params: list[object]) -> str:
        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing RPC URL for chain '{chain_code}'")

        response = self._client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            },
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result")
        if not isinstance(result, str):
            raise RuntimeError(f"unexpected RPC response for {method}: {payload}")

        return result

    def get_erc20_balance(self, chain_code: str, wallet_address: str, token_address: str) -> int:
        """Read ERC20 `balanceOf` for a wallet."""

        call_data = _encode_balance_of_call(wallet_address)
        raw_hex = self._rpc(
            chain_code,
            "eth_call",
            [{"to": token_address, "data": call_data}, "latest"],
        )
        return int(raw_hex, 16)

    def get_native_balance(self, chain_code: str, wallet_address: str) -> int:
        """Read native token balance for a wallet."""

        raw_hex = self._rpc(chain_code, "eth_getBalance", [wallet_address, "latest"])
        return int(raw_hex, 16)


class WalletBalancesAdapter:
    """Builds canonical position snapshots for configured wallet balances."""

    protocol_code = "wallet_balances"

    def __init__(self, markets_config: MarketsConfig, balance_client: BalanceClient) -> None:
        self.markets_config = markets_config
        self.balance_client = balance_client

    def _targets(self) -> list[WalletTokenTarget]:
        targets: list[WalletTokenTarget] = []
        for chain_code, chain_config in self.markets_config.wallet_balances.items():
            for wallet_address in chain_config.wallets:
                for token in chain_config.tokens:
                    targets.append(
                        WalletTokenTarget(
                            chain_code=chain_code,
                            wallet_address=canonical_address(wallet_address),
                            token_symbol=token.symbol,
                            token_address=canonical_address(token.address),
                            decimals=token.decimals,
                        )
                    )
        return targets

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        """Return wallet balance snapshots and failures."""

        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for target in self._targets():
            try:
                if _is_native_token(target.token_address):
                    raw_balance = self.balance_client.get_native_balance(
                        target.chain_code,
                        target.wallet_address,
                    )
                else:
                    raw_balance = self.balance_client.get_erc20_balance(
                        target.chain_code, target.wallet_address, target.token_address
                    )
                supplied_amount = normalize_raw_amount(raw_balance, target.decimals)
            except Exception as exc:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="wallet_balance_read_failed",
                        error_message=str(exc),
                        protocol_code=self.protocol_code,
                        chain_code=target.chain_code,
                        wallet_address=target.wallet_address,
                        market_ref=target.token_address,
                        payload_json={"symbol": target.token_symbol},
                    )
                )
                continue

            token_key = (target.chain_code, target.token_address)
            price_usd = prices_by_token.get(token_key)
            if price_usd is None:
                price_usd = Decimal("0")
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="price_missing",
                        error_message="no price available for wallet balance token",
                        protocol_code=self.protocol_code,
                        chain_code=target.chain_code,
                        wallet_address=target.wallet_address,
                        market_ref=target.token_address,
                        payload_json={"symbol": target.token_symbol},
                    )
                )

            supplied_usd = supplied_amount * price_usd
            position_key = ":".join(
                [
                    "wallet_balances",
                    target.chain_code,
                    target.wallet_address,
                    target.token_address,
                ]
            )
            positions.append(
                PositionSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code=self.protocol_code,
                    chain_code=target.chain_code,
                    wallet_address=target.wallet_address,
                    market_ref=target.token_address,
                    position_key=position_key,
                    supplied_amount=supplied_amount,
                    supplied_usd=supplied_usd,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=supplied_usd,
                    source="rpc",
                )
            )

        return positions, issues
