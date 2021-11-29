from types import TracebackType
from typing import Tuple, Type, Dict, Any, Union

ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]
ContextDict = Dict[str, Any]
