"""RFC 8785 for the AC-001 integer-only manifest domain; no float conversions."""

from __future__ import annotations

import hashlib
import json
from typing import TypeAlias

from bqip.contracts.values import ContractError

JsonValue: TypeAlias = None | bool | int | str | list["JsonValue"] | dict[str, "JsonValue"]
MAX_SAFE_INTEGER = (1 << 53) - 1
EXCLUDED = frozenset(("manifest_hash", "comments"))


def _pairs(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _reject_float(text: str) -> None:
    raise ContractError(f"NON_INTEGER_JSON_NUMBER: {text}")


def parse_manifest(text: str) -> dict[str, JsonValue]:
    value: JsonValue = json.loads(text, object_pairs_hook=_pairs, parse_float=_reject_float,
                                 parse_constant=_reject_float)
    if not isinstance(value, dict):
        raise ContractError("MANIFEST_OBJECT_REQUIRED")
    # Validate the complete document before excluding annotations.
    canonical_bytes(value)
    return value


def canonical_bytes(value: JsonValue) -> bytes:
    if value is None:
        return b"null"
    if type(value) is bool:
        return b"true" if value else b"false"
    if type(value) is int:
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise ContractError("UNSAFE_JSON_INTEGER_USE_STRING")
        return str(value).encode("ascii")
    if isinstance(value, str):
        try:
            return json.dumps(value, ensure_ascii=False).encode("utf-8", errors="strict")
        except UnicodeError as error:
            raise ContractError("INVALID_UNICODE") from error
    if isinstance(value, list):
        return b"[" + b",".join(canonical_bytes(item) for item in value) + b"]"
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ContractError("JSON_KEY_MUST_BE_STRING")
        try:
            keys = sorted(value, key=lambda key: key.encode("utf-16-be", errors="strict"))
        except UnicodeError as error:
            raise ContractError("INVALID_UNICODE") from error
        return b"{" + b",".join(canonical_bytes(key) + b":" + canonical_bytes(value[key])
                                for key in keys) + b"}"
    raise ContractError("UNSUPPORTED_JSON_VALUE")


def canonical_manifest(manifest: dict[str, JsonValue]) -> bytes:
    canonical_bytes(manifest)
    return canonical_bytes({key: value for key, value in manifest.items() if key not in EXCLUDED})


def manifest_hash(manifest: dict[str, JsonValue]) -> bytes:
    return hashlib.sha256(canonical_manifest(manifest)).digest()
