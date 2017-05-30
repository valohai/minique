import json

from redis import StrictRedis

from minique.consts import JOB_KEY_PREFIX, RESULT_KEY_PREFIX
from minique.enums import JobStatus
from minique.excs import NoSuchJob


class Job:
    def __init__(self, redis: StrictRedis, id):
        self.redis = redis
        assert isinstance(id, str)
        self.id = id

    def ensure_exists(self):
        if not self.exists:
            raise NoSuchJob('Job {id} does not exist'.format(id=self.id))

    @property
    def redis_key(self):
        return '%s%s' % (JOB_KEY_PREFIX, self.id)

    @property
    def result_redis_key(self):
        return '%s%s' % (RESULT_KEY_PREFIX, self.id)

    @property
    def acquisition_info(self):
        return json.loads(self.redis.hget(self.redis_key, 'acquired').decode())

    @property
    def has_finished(self):
        return self.redis.exists(self.result_redis_key)

    @property
    def has_started(self):
        return self.redis.hexists(self.redis_key, 'acquired')

    @property
    def result(self):
        # TODO: exception handling
        return json.loads(self.redis.get(self.result_redis_key).decode())

    @property
    def status(self):
        return JobStatus(self.redis.hget(self.redis_key, 'status').decode())

    @property
    def exists(self):
        return self.redis.exists(self.redis_key)

    @property
    def result_ttl(self):
        return int(self.redis.hget(self.redis_key, 'result_ttl'))

    @property
    def duration(self):
        return float(self.redis.hget(self.redis_key, 'duration'))

    @property
    def kwargs(self):
        # TODO: exception handling
        return json.loads(self.redis.hget(self.redis_key, 'kwargs').decode())

    @property
    def callable_name(self):
        return self.redis.hget(self.redis_key, 'callable').decode()

    def __str__(self):
        return '<job %s>' % self.id

    def __eq__(self, other):
        return isinstance(other, Job) and (self.id == other.id)
