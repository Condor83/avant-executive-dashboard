"""Stacks API helpers for read-only adapter calls."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import httpx

C32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
HEX_ALPHABET = "0123456789abcdef"
READ_ONLY_DEFAULT_SENDER = "SP000000000000000000002Q6VF78"
QueryPrimitive = str | int | float | bool | None
QueryValue = QueryPrimitive | Sequence[QueryPrimitive]


def parse_rate_to_unit(value: object) -> Decimal:
    """Normalize APR/APY representations to 0.0-1.0 decimal units."""

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid rate value: {value}") from None
    if parsed < 0:
        raise ValueError(f"rate cannot be negative: {parsed}")
    if parsed > Decimal("1"):
        return parsed / Decimal("100")
    return parsed


def _c32_normalize(value: str) -> str:
    return value.upper().replace("O", "0").replace("L", "1").replace("I", "1")


def _c32_decode_to_hex(c32input: str, *, min_length: int | None = None) -> str:
    normalized = _c32_normalize(c32input)
    if not normalized:
        return ""
    if any(char not in C32_ALPHABET for char in normalized):
        raise ValueError(f"not a c32-encoded string: {c32input}")

    leading_zero_digits = len(normalized) - len(normalized.lstrip("0"))

    result_hex_digits: list[str] = []
    carry = 0
    carry_bits = 0
    for index in range(len(normalized) - 1, -1, -1):
        if carry_bits == 4:
            result_hex_digits.insert(0, HEX_ALPHABET[carry])
            carry_bits = 0
            carry = 0

        current_code = C32_ALPHABET.index(normalized[index]) << carry_bits
        current_value = current_code + carry
        result_hex_digits.insert(0, HEX_ALPHABET[current_value % 16])
        carry_bits += 1
        carry = current_value >> 4

    result_hex_digits.insert(0, HEX_ALPHABET[carry])
    if len(result_hex_digits) % 2 == 1:
        result_hex_digits.insert(0, "0")

    leading_zero_hex_digits = 0
    for digit in result_hex_digits:
        if digit != "0":
            break
        leading_zero_hex_digits += 1

    trim_count = leading_zero_hex_digits - (leading_zero_hex_digits % 2)
    trimmed = result_hex_digits[trim_count:]
    hex_str = "".join(trimmed)
    hex_str = ("00" * leading_zero_digits) + hex_str

    if min_length is not None:
        target_length = min_length * 2
        if len(hex_str) < target_length:
            hex_str = ("00" * ((target_length - len(hex_str)) // 2)) + hex_str

    return hex_str


def decode_stacks_address(address: str) -> tuple[int, bytes]:
    """Decode a Stacks c32check address to (version, hash160 bytes)."""

    if len(address) <= 5:
        raise ValueError(f"invalid Stacks address length: {address}")
    if not address.startswith("S"):
        raise ValueError(f"Stacks address must start with 'S': {address}")

    encoded = _c32_normalize(address[1:])
    version_char = encoded[0]
    if version_char not in C32_ALPHABET:
        raise ValueError(f"invalid Stacks address version char: {address}")

    version = C32_ALPHABET.index(version_char)
    data_hex = _c32_decode_to_hex(encoded[1:])
    if len(data_hex) < 8:
        raise ValueError(f"invalid Stacks address payload: {address}")

    payload_hex = data_hex[:-8]
    checksum_hex = data_hex[-8:]
    version_hex = f"{version:02x}"
    checksum_input = bytes.fromhex(version_hex + payload_hex)
    expected_checksum = hashlib.sha256(hashlib.sha256(checksum_input).digest()).digest()[:4].hex()
    if expected_checksum != checksum_hex:
        raise ValueError(f"invalid Stacks address checksum: {address}")

    payload = bytes.fromhex(payload_hex)
    if len(payload) != 20:
        raise ValueError(f"Stacks address hash160 length is {len(payload)}; expected 20")
    return version, payload


def serialize_principal(address: str) -> str:
    """Serialize a standard principal as Clarity value hex."""

    version, hash160 = decode_stacks_address(address)
    encoded = bytes([0x05, version]) + hash160
    return f"0x{encoded.hex()}"


def serialize_contract_principal(contract_identifier: str) -> str:
    """Serialize a contract principal (<address>.<contract>) as Clarity value hex."""

    if "." not in contract_identifier:
        raise ValueError(f"invalid contract identifier: {contract_identifier}")

    address, contract_name = contract_identifier.split(".", 1)
    name_bytes = contract_name.encode("ascii")
    if len(name_bytes) > 255:
        raise ValueError(f"contract name too long: {contract_identifier}")

    version, hash160 = decode_stacks_address(address)
    encoded = bytes([0x06, version]) + hash160 + bytes([len(name_bytes)]) + name_bytes
    return f"0x{encoded.hex()}"


def _decode_clarity_value_bytes(data: bytes, offset: int = 0) -> tuple[object, int]:
    if offset >= len(data):
        raise ValueError("unexpected end of Clarity value")

    value_type = data[offset]
    offset += 1

    if value_type == 0x00:  # int
        return int.from_bytes(data[offset : offset + 16], "big", signed=True), offset + 16
    if value_type == 0x01:  # uint
        return int.from_bytes(data[offset : offset + 16], "big", signed=False), offset + 16
    if value_type == 0x02:  # buffer
        size = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        return data[offset : offset + size], offset + size
    if value_type == 0x03:  # bool true
        return True, offset
    if value_type == 0x04:  # bool false
        return False, offset
    if value_type == 0x05:  # standard principal
        version = data[offset]
        hash160 = data[offset + 1 : offset + 21]
        return {"address_version": version, "hash160": hash160.hex()}, offset + 21
    if value_type == 0x06:  # contract principal
        version = data[offset]
        hash160 = data[offset + 1 : offset + 21]
        offset += 21
        name_len = data[offset]
        offset += 1
        contract_name = data[offset : offset + name_len].decode("ascii")
        offset += name_len
        return {
            "address_version": version,
            "hash160": hash160.hex(),
            "contract_name": contract_name,
        }, offset
    if value_type == 0x07:  # response ok
        inner, offset = _decode_clarity_value_bytes(data, offset)
        return {"ok": inner}, offset
    if value_type == 0x08:  # response err
        inner, offset = _decode_clarity_value_bytes(data, offset)
        return {"err": inner}, offset
    if value_type == 0x09:  # optional none
        return None, offset
    if value_type == 0x0A:  # optional some
        return _decode_clarity_value_bytes(data, offset)
    if value_type == 0x0B:  # list
        length = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        list_items: list[object] = []
        for _ in range(length):
            item, offset = _decode_clarity_value_bytes(data, offset)
            list_items.append(item)
        return list_items, offset
    if value_type == 0x0C:  # tuple
        length = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        tuple_items: dict[str, object] = {}
        for _ in range(length):
            key_len = data[offset]
            offset += 1
            key = data[offset : offset + key_len].decode("ascii")
            offset += key_len
            item, offset = _decode_clarity_value_bytes(data, offset)
            tuple_items[key] = item
        return tuple_items, offset
    if value_type == 0x0D:  # string-ascii
        size = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        return data[offset : offset + size].decode("ascii"), offset + size
    if value_type == 0x0E:  # string-utf8
        size = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        return data[offset : offset + size].decode("utf-8"), offset + size

    raise ValueError(f"unsupported Clarity type tag: {value_type}")


def decode_clarity_value(value_hex: str) -> object:
    """Decode a Clarity value hex string (e.g. `0x0c...`) into Python objects."""

    normalized = value_hex[2:] if value_hex.startswith("0x") else value_hex
    payload = bytes.fromhex(normalized)
    value, consumed = _decode_clarity_value_bytes(payload, 0)
    if consumed != len(payload):
        raise ValueError("extra bytes after Clarity value")
    return value


@dataclass(frozen=True)
class StacksFtBalance:
    """Fungible token balance for a principal/asset identifier."""

    principal: str
    asset_identifier: str
    balance_raw: int


class StacksClient:
    """HTTP client for Stacks API reads."""

    def __init__(self, base_url: str, timeout_seconds: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _get(self, path: str, params: Mapping[str, QueryValue] | None = None) -> object:
        response = self._client.get(f"{self.base_url}{path}", params=params)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, payload_json: dict[str, object]) -> object:
        response = self._client.post(f"{self.base_url}{path}", json=payload_json)
        response.raise_for_status()
        return response.json()

    def get_block_height(self) -> int:
        """Return latest Stacks burn block height."""

        payload = self._get("/extended/v1/block")
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                height = first.get("height")
                if isinstance(height, int):
                    return height

        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list) and results:
                first = results[0]
                if isinstance(first, dict):
                    height = first.get("height")
                    if isinstance(height, int):
                        return height

        raise RuntimeError(f"unexpected Stacks block payload: {payload}")

    def get_ft_balance(self, principal: str, asset_identifier: str) -> StacksFtBalance:
        """Return fungible token balance for principal/asset."""

        payload = self._get(f"/extended/v1/address/{principal}/balances")
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Stacks balances payload: {payload}")

        ft_balances = payload.get("fungible_tokens")
        if not isinstance(ft_balances, dict):
            raise RuntimeError(f"unexpected Stacks fungible token payload: {payload}")

        token_payload = ft_balances.get(asset_identifier)
        if not isinstance(token_payload, dict):
            return StacksFtBalance(
                principal=principal,
                asset_identifier=asset_identifier,
                balance_raw=0,
            )

        balance_raw_value = token_payload.get("balance")
        if balance_raw_value is None:
            return StacksFtBalance(
                principal=principal,
                asset_identifier=asset_identifier,
                balance_raw=0,
            )

        return StacksFtBalance(
            principal=principal,
            asset_identifier=asset_identifier,
            balance_raw=int(str(balance_raw_value)),
        )

    @staticmethod
    def serialize_principal(address: str) -> str:
        return serialize_principal(address)

    @staticmethod
    def serialize_contract_principal(contract_identifier: str) -> str:
        return serialize_contract_principal(contract_identifier)

    def call_read_only(
        self,
        *,
        contract_address: str,
        contract_name: str,
        function_name: str,
        arguments: list[str],
        sender: str = READ_ONLY_DEFAULT_SENDER,
    ) -> object:
        """Execute a Clarity read-only function and decode its result."""

        payload = self._post(
            f"/v2/contracts/call-read/{contract_address}/{contract_name}/{function_name}",
            {
                "sender": sender,
                "arguments": arguments,
            },
        )
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected read-only payload: {payload}")

        if payload.get("okay") is not True:
            cause = payload.get("cause", "unknown read-only failure")
            raise RuntimeError(str(cause))

        result_hex = payload.get("result")
        if not isinstance(result_hex, str):
            raise RuntimeError(f"missing read-only result: {payload}")

        decoded = decode_clarity_value(result_hex)
        if isinstance(decoded, dict) and set(decoded.keys()) == {"ok"}:
            return decoded["ok"]
        if isinstance(decoded, dict) and set(decoded.keys()) == {"err"}:
            raise RuntimeError(f"clarity read-only error: {decoded['err']}")
        return decoded
