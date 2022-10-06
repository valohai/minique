from typing import Iterable, Optional

from redis import Redis


def read_list(
    redis_conn: "Redis[bytes]",
    key: str,
    *,
    chunk_size: int = 4096,
    last_n: Optional[int] = None
) -> Iterable[bytes]:
    """
    Read a possibly large Redis list in chunks.

    Avoids OOMs on the Redis side.

    :param redis_conn: Redis connection
    :param key: Key
    :param chunk_size: How many lines to read per request.
    :param last_n: Attempt to only read the last N lines.
    :return:
    """
    # LLEN returns zero for a non-existent key,
    # and there would be nothing to iterate from an empty
    # list anyway, so we can do one query less.
    list_len = redis_conn.llen(key)
    if list_len == 0:
        return

    if chunk_size <= 0:
        chunk_size = 4096

    if last_n and last_n > 0:
        offset = max(0, list_len - last_n)
    else:
        offset = 0

    while offset < list_len:
        # Regarding that - 1 there, see this from https://redis.io/commands/lrange:
        # > Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10
        # > will return 11 elements, that is, the rightmost item is included.
        chunk = redis_conn.lrange(key, offset, offset + chunk_size - 1) or []
        if not chunk:
            break
        yield from chunk
        offset += chunk_size
