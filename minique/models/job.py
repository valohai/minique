from redis import StrictRedis

from minique.consts import JOB_KEY_PREFIX, RESULT_KEY_PREFIX
from minique.enums import JobStatus
from minique.excs import NoSuchJob
from minique.utils import get_json_or_none


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
        return get_json_or_none(self.redis.hget(self.redis_key, 'acquired'))

    @property
    def has_finished(self):
        return self.redis.exists(self.result_redis_key)

    @property
    def has_started(self):
        return self.redis.hexists(self.redis_key, 'acquired')

    @property
    def result(self):
        return get_json_or_none(self.redis.get(self.result_redis_key))

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
    def queue_name(self):
        return self.redis.hget(self.redis_key, 'queue').decode()

    @property
    def kwargs(self):
        return get_json_or_none(self.redis.hget(self.redis_key, 'kwargs'))

    @property
    def callable_name(self):
        return self.redis.hget(self.redis_key, 'callable').decode()

    def get_queue(self):
        from minique.models.queue import Queue
        return Queue(redis=self.redis, name=self.queue_name)

    def __str__(self):
        return '<job %s>' % self.id

    def __eq__(self, other):
        return isinstance(other, Job) and (self.id == other.id)
