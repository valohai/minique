from typing import Any, Optional, Tuple

from redis import Redis

from minique._compat import TYPE_CHECKING
from minique.consts import JOB_KEY_PREFIX, RESULT_KEY_PREFIX
from minique.enums import JobStatus
from minique.excs import NoSuchJob, AlreadyAcquired, AlreadyResulted, InvalidStatus
from minique.utils import get_json_or_none

if TYPE_CHECKING:
    from minique.models.queue import Queue


class Job:
    def __init__(self, redis: Redis, id: str) -> None:
        self.redis = redis
        self.id = str(id)

    def ensure_exists(self) -> None:
        if not self.exists:
            raise NoSuchJob('Job {id} does not exist'.format(id=self.id))

    def ensure_enqueued(self) -> Tuple[bool, int]:
        """
        Ensure the job is indeed in its queue. See the docs for Queue.ensure_enqueued() for more information.
        """
        if self.has_started:
            raise AlreadyAcquired('Job {id} has already been started, will not enqueue'.format(id=self.id))
        if self.has_finished:
            raise AlreadyResulted('Job {id} has already been finished, will not enqueue'.format(id=self.id))
        status = self.status
        if status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            raise InvalidStatus('Job {id} has status {status}, will not enqueue'.format(id=self.id, status=status))
        return self.get_queue().ensure_enqueued(self)

    @property
    def redis_key(self) -> str:
        return '%s%s' % (JOB_KEY_PREFIX, self.id)

    @property
    def result_redis_key(self) -> str:
        return '%s%s' % (RESULT_KEY_PREFIX, self.id)

    @property
    def acquisition_info(self) -> dict:
        return get_json_or_none(self.redis.hget(self.redis_key, 'acquired'))

    @property
    def has_finished(self) -> int:
        return self.redis.exists(self.result_redis_key)

    @property
    def has_started(self) -> bool:
        return self.redis.hexists(self.redis_key, 'acquired')

    @property
    def result(self) -> Optional[Any]:
        return get_json_or_none(self.redis.get(self.result_redis_key))

    @property
    def status(self) -> JobStatus:
        return JobStatus(self.redis.hget(self.redis_key, 'status').decode())

    @property
    def exists(self) -> int:
        return self.redis.exists(self.redis_key)

    @property
    def result_ttl(self) -> int:
        return int(self.redis.hget(self.redis_key, 'result_ttl'))

    @property
    def duration(self) -> float:
        return float(self.redis.hget(self.redis_key, 'duration'))

    @property
    def queue_name(self) -> str:
        return self.redis.hget(self.redis_key, 'queue').decode()

    @property
    def kwargs(self) -> dict:
        return get_json_or_none(self.redis.hget(self.redis_key, 'kwargs'))

    @property
    def callable_name(self) -> str:
        return self.redis.hget(self.redis_key, 'callable').decode()

    def get_queue(self) -> 'Queue':
        from minique.models.queue import Queue
        return Queue(redis=self.redis, name=self.queue_name)

    def __str__(self):
        return '<job %s>' % self.id

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Job) and (self.id == other.id)
