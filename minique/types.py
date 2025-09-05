from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeAlias

ExcInfo: TypeAlias = (
    tuple[type[BaseException], BaseException, TracebackType] | tuple[None, None, None]
)
ContextDict: TypeAlias = dict[str, Any]

if TYPE_CHECKING:
    import redis

    RedisClient: TypeAlias = redis.Redis[bytes]

    # TODO: the types for `valkey` are suboptimal (since they are so in the original
    #       pre-fork `redis` package), so adding `| valkey.Valkey` to the above type alias
    #       practically breaks everything. The stub types from `types-redis` are OK, but
    #       naturally won't apply to `valkey`.
    #       See https://github.com/valkey-io/valkey-py/issues/164.
