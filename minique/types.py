from types import TracebackType
from typing import Any, Dict, Tuple, Type, Union

ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]
ContextDict = Dict[str, Any]
