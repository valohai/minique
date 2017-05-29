from minique.consts import QUEUE_KEY_PREFIX
from minique.utils import cached_property


class Queue:
    def __init__(self, redis, name):
        self.redis = redis
        self.name = name

    @cached_property
    def redis_key(self):
        return '%s%s' % (QUEUE_KEY_PREFIX, self.name)

    @property
    def length(self):
        return self.redis.llen(self.redis_key)
