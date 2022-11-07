import json
from typing import Any, Callable, Dict, Optional, Type, Union

registry = {}
default_encoding_name: Optional[str] = None


def register_encoding(
    name: str, *, default: bool = False
) -> Callable[[Type["BaseEncoding"]], Type["BaseEncoding"]]:
    def decorator(cls: Type["BaseEncoding"]) -> Type["BaseEncoding"]:
        global default_encoding_name
        registry[name] = cls
        if default:
            default_encoding_name = name
        return cls

    return decorator


class BaseEncoding:
    def encode(self, value: Any, failsafe: bool = False) -> Union[str, bytes]:
        """
        Encode a value to a string or bytes.

        :param failsafe: When set, hint that the encoder should try hard not to fail,
                         even if it requires loss of fidelity.
        """
        raise NotImplementedError("Encoding not implemented")

    def decode(self, value: Union[str, bytes]) -> Any:
        raise NotImplementedError("Decoding not implemented")


@register_encoding("json", default=True)
class JSONEncoding(BaseEncoding):
    """
    Default (JSON) encoding for kwargs and results.
    """

    # These can be effortlessly overridden in subclasses
    dump_kwargs = {
        "ensure_ascii": False,
        "separators": (",", ":"),
    }
    load_kwargs = {}  # type: ignore
    failsafe_default = str

    def encode(self, value: Any, failsafe: bool = False) -> Union[str, bytes]:
        kwargs = self.dump_kwargs.copy()  # type: Dict[str, Any]
        if failsafe:
            kwargs["default"] = self.failsafe_default
        return json.dumps(
            value,
            **kwargs,
        )

    def decode(self, value: Union[str, bytes]) -> Any:
        if isinstance(value, bytes):
            value = value.decode()
        return json.loads(value, **self.load_kwargs)
