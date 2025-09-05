from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

registry = {}
default_encoding_name: str | None = None


def register_encoding(
    name: str,
    *,
    default: bool = False,
) -> Callable[[type[BaseEncoding]], type[BaseEncoding]]:
    def decorator(cls: type[BaseEncoding]) -> type[BaseEncoding]:
        global default_encoding_name  # noqa: PLW0603
        registry[name] = cls
        if default:
            default_encoding_name = name
        return cls

    return decorator


class BaseEncoding:
    def encode(self, value: Any, failsafe: bool = False) -> str | bytes:
        """
        Encode a value to a string or bytes.

        :param failsafe: When set, hint that the encoder should try hard not to fail,
                         even if it requires loss of fidelity.
        """
        raise NotImplementedError("Encoding not implemented")

    def decode(self, value: str | bytes) -> Any:
        raise NotImplementedError("Decoding not implemented")


@register_encoding("json", default=True)
class JSONEncoding(BaseEncoding):
    """
    Default (JSON) encoding for kwargs and results.
    """

    # These can be effortlessly overridden in subclasses
    dump_kwargs: dict[str, Any] = {
        "ensure_ascii": False,
        "separators": (",", ":"),
    }
    load_kwargs: dict[str, Any] = {}
    failsafe_default = str

    def encode(self, value: Any, failsafe: bool = False) -> str | bytes:
        kwargs = self.dump_kwargs.copy()
        if failsafe:
            kwargs["default"] = self.failsafe_default
        return json.dumps(
            value,
            **kwargs,
        )

    def decode(self, value: str | bytes) -> Any:
        if isinstance(value, bytes):
            value = value.decode()
        return json.loads(value, **self.load_kwargs)
