from typing import Iterable, Optional, Callable

from redis import Redis


def read_list(
    redis_conn: "Redis[bytes]",
    key: str,
    *,
    chunk_size: int = 4096,
    filter_fn: Optional[Callable[[bytes], bool]] = None,
    last_n: Optional[int] = None,
) -> Iterable[bytes]:
    """
    Read a possibly large Redis list in chunks.

    Avoids OOMs on the Redis side.

    :param redis_conn: Redis connection
    :param key: Key
    :param chunk_size: How many lines to read per request.
    :param filter_fn: Only include lines that pass this filter
    :param last_n: How many last lines to return, filtering is applied before limiting.
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

    results = []

    # Regarding that - 1 there, see this from https://redis.io/commands/lrange:
    # > Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10
    # > will return 11 elements, that is, the rightmost item is included.
    end_index = list_len - 1
    remaining_needed = last_n if last_n and last_n > 0 else list_len

    # read until we have enough items, or we run out of the list
    while end_index >= 0 and remaining_needed > 0:
        start_index = max(0, end_index - chunk_size + 1)

        chunk = redis_conn.lrange(key, start_index, end_index)
        if not chunk:
            break

        for item in reversed(chunk):
            if not filter_fn or filter_fn(item):
                results.append(item)
                remaining_needed -= 1
                if remaining_needed == 0:
                    break

        # move the reading window further back
        end_index = start_index - 1

    yield from reversed(results)
