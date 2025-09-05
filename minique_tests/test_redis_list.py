from __future__ import annotations

from typing import TYPE_CHECKING

from minique.utils.redis_list import read_list

if TYPE_CHECKING:
    from minique.types import RedisClient


def test_read_list(redis: RedisClient, random_queue_name: str):
    data = [str(x).encode() for x in range(500)]
    redis.rpush(random_queue_name, *data)
    assert list(read_list(redis, random_queue_name, chunk_size=7)) == data
    redis.delete(random_queue_name)


def test_read_list_last_n(redis: RedisClient, random_queue_name: str):
    data = [str(x).encode() for x in range(6)]
    redis.rpush(random_queue_name, *data)

    last_two = read_list(redis, random_queue_name, last_n=2)
    assert list(last_two) == [b"4", b"5"]

    last_four = read_list(redis, random_queue_name, last_n=4)
    assert list(last_four) == [b"2", b"3", b"4", b"5"]

    last_everything = read_list(redis, random_queue_name, last_n=999_999)
    assert list(last_everything) == [b"0", b"1", b"2", b"3", b"4", b"5"]

    redis.delete(random_queue_name)


def test_read_list_filter(redis: RedisClient, random_queue_name: str):
    data = [str(x).encode() for x in range(101)]
    redis.rpush(random_queue_name, *data)

    last_four_without_nines = read_list(
        redis,
        random_queue_name,
        chunk_size=2,
        last_n=4,
        filter_fn=lambda line: b"9" not in line,
    )
    assert list(last_four_without_nines) == [b"86", b"87", b"88", b"100"]

    last_four_starting_with_one = read_list(
        redis,
        random_queue_name,
        chunk_size=5,
        last_n=4,
        filter_fn=lambda line: line.startswith(b"1"),
    )
    assert list(last_four_starting_with_one) == [b"17", b"18", b"19", b"100"]

    redis.delete(random_queue_name)
