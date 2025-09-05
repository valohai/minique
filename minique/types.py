from __future__ import annotations

from types import TracebackType
from typing import Any, TypeAlias

ExcInfo: TypeAlias = (
    tuple[type[BaseException], BaseException, TracebackType] | tuple[None, None, None]
)
ContextDict: TypeAlias = dict[str, Any]
