from typing import Iterable, Optional

from redis import Redis


def read_list(
    redis_conn: Redis,
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
    if not redis_conn.exists(key):
        return

    if chunk_size <= 0:
        chunk_size = 4096

    if last_n and last_n > 0:
        offset = redis_conn.llen(key) - last_n
    else:
        offset = 0

    while offset < redis_conn.llen(key):
        # Regarding that - 1 there, see this from https://redis.io/commands/lrange:
        # > Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10
        # > will return 11 elements, that is, the rightmost item is included.
        chunk = (redis_conn.lrange(key, offset, offset + chunk_size - 1) or [])
        if not chunk:
            break
        yield from chunk
        offset += chunk_size
