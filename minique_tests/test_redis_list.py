from redis import Redis

from minique.utils.redis_list import read_list


def test_redis_list(redis: Redis, random_queue_name: str):
    data = [str(x).encode() for x in range(500)]
    redis.rpush(random_queue_name, *data)
    assert list(read_list(redis, random_queue_name, chunk_size=7)) == data
    redis.delete(random_queue_name)
