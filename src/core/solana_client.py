"""Minimal Solana JSON-RPC client and parsing helpers."""

from __future__ import annotations

import base64
from dataclasses import dataclass

import httpx


def decode_base64_account_data(raw_data: list[str] | str) -> bytes:
    """Decode account data payload returned by Solana RPC."""

    if isinstance(raw_data, str):
        return base64.b64decode(raw_data)
    if isinstance(raw_data, list) and raw_data:
        return base64.b64decode(raw_data[0])
    raise ValueError(f"unsupported Solana account data payload: {raw_data}")


def parse_u64_le(data: bytes, offset: int) -> int:
    """Parse little-endian u64 from account bytes at a fixed offset."""

    end = offset + 8
    if offset < 0 or end > len(data):
        raise ValueError("u64 parse offset is out of bounds")
    return int.from_bytes(data[offset:end], byteorder="little", signed=False)


@dataclass(frozen=True)
class SolanaAccountInfo:
    """Decoded Solana account data bundle."""

    pubkey: str
    owner: str
    lamports: int
    data: bytes
    slot: int | None


class SolanaRpcClient:
    """Thin JSON-RPC client for Solana read calls."""

    def __init__(self, rpc_urls: dict[str, str], timeout_seconds: float = 15.0) -> None:
        self.rpc_urls = {key: value for key, value in rpc_urls.items() if value}
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _rpc(self, chain_code: str, method: str, params: list[object]) -> dict[str, object]:
        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing Solana RPC URL for chain '{chain_code}'")

        response = self._client.post(
            rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        if "result" not in payload:
            raise RuntimeError(f"unexpected Solana RPC response for {method}: {payload}")
        result = payload["result"]
        if not isinstance(result, dict):
            raise RuntimeError(f"unexpected Solana RPC result type for {method}: {result}")
        return result

    def get_slot(self, chain_code: str) -> int:
        """Return latest Solana slot."""

        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing Solana RPC URL for chain '{chain_code}'")
        response = self._client.post(
            rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result")
        if not isinstance(result, int):
            raise RuntimeError(f"unexpected Solana RPC result type for getSlot: {result}")
        return result

    def get_account_info(self, chain_code: str, pubkey: str) -> SolanaAccountInfo:
        """Return decoded account info for a Solana pubkey."""

        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing Solana RPC URL for chain '{chain_code}'")

        response = self._client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [pubkey, {"encoding": "base64"}],
            },
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))

        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"unexpected Solana account info response: {payload}")

        context = result.get("context", {})
        slot = context.get("slot") if isinstance(context, dict) else None
        slot_value = int(slot) if isinstance(slot, int) else None

        value = result.get("value")
        if not isinstance(value, dict):
            raise RuntimeError(f"account not found for pubkey {pubkey}")

        raw_data = value.get("data")
        owner = value.get("owner")
        lamports = value.get("lamports")
        if (
            not isinstance(owner, str)
            or not isinstance(lamports, int)
            or not isinstance(raw_data, list | str)
        ):
            raise RuntimeError(f"unexpected Solana account value payload: {value}")

        return SolanaAccountInfo(
            pubkey=pubkey,
            owner=owner,
            lamports=lamports,
            data=decode_base64_account_data(raw_data),
            slot=slot_value,
        )
