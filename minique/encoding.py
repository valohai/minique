from __future__ import annotations

from collections.abc import Callable
from typing import Any

from minique.json import from_json, to_json_bytes

registry: dict[str, type[BaseEncoding]] = {}
default_encoding_name: str | None = None


def register_encoding(
    name: str, *, default: bool = False
) -> Callable[[type[BaseEncoding]], type[BaseEncoding]]:
    def decorator(cls: type[BaseEncoding]) -> type[BaseEncoding]:
        global default_encoding_name
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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attr in ("dump_kwargs", "load_kwargs", "failsafe_default"):
            if attr in cls.__dict__:
                raise TypeError(
                    f"{cls.__name__} sets removed attribute {attr!r}; "
                    f"override encode()/decode() directly instead",
                )

    def encode(self, value: Any, failsafe: bool = False) -> str | bytes:
        return to_json_bytes(value, default=str if failsafe else None)

    def decode(self, value: str | bytes) -> Any:
        return from_json(value)
