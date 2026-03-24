from __future__ import annotations

from typing import Any

try:
    import orjson

    def from_json(data: str | bytes) -> Any:
        return orjson.loads(data)

    def to_json_bytes(value: Any, *, default: Any = None) -> bytes:
        return orjson.dumps(value, default=default, option=orjson.OPT_NON_STR_KEYS)

    def to_json_str(value: Any, *, default: Any = None) -> str:
        return to_json_bytes(value, default=default).decode()

except ImportError:
    import json

    def from_json(data: str | bytes) -> Any:
        return json.loads(data)

    def to_json_bytes(value: Any, *, default: Any = None) -> bytes:
        return json.dumps(
            value,
            default=default,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()

    def to_json_str(value: Any, *, default: Any = None) -> str:
        return json.dumps(
            value,
            default=default,
            ensure_ascii=False,
            separators=(",", ":"),
        )
