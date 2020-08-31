from typing import TYPE_CHECKING

from redis import Redis

from minique.consts import QUEUE_KEY_PREFIX
from minique.utils import cached_property

if TYPE_CHECKING:
    from minique.models.job import Job


class Queue:
    def __init__(self, redis: Redis, name: str) -> None:
        self.redis = redis
        self.name = str(name)

    @cached_property
    def redis_key(self):
        return '%s%s' % (QUEUE_KEY_PREFIX, self.name)

    @property
    def length(self) -> int:
        return self.redis.llen(self.redis_key)

    def clear(self):
        """
        Entirely clear this queue. Do not call this unless you're willing to risk orphaned jobs.
        """
        return self.redis.delete(self.redis_key)

    def enqueue_initial(self, job: 'Job', payload: dict):
        assert payload['queue'] == self.name
        with self.redis.pipeline() as p:
            p.hmset(job.redis_key, payload)
            if payload['job_ttl'] > 0:
                p.expire(job.redis_key, payload['job_ttl'])
            p.rpush(self.redis_key, job.id)
            p.execute()
