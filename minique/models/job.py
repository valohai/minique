import json
import time
from typing import Any, Callable, Optional, Tuple

from redis import Redis

from minique import encoding
from minique.compat import TYPE_CHECKING
from minique.consts import JOB_KEY_PREFIX, RESULT_KEY_PREFIX
from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, AlreadyResulted, InvalidStatus, NoSuchJob

if TYPE_CHECKING:
    from minique.models.queue import Queue


class Job:
    # Used for testing:
    replacement_callable = None  # type: Optional[Callable]
    replacement_kwargs = None  # type: Optional[dict]

    def __init__(self, redis: Redis, id: str) -> None:
        self.redis = redis
        self.id = str(id)

    def ensure_exists(self) -> None:
        if not self.exists:
            raise NoSuchJob("Job {id} does not exist".format(id=self.id))

    def ensure_enqueued(self) -> Tuple[bool, int]:
        """
        Ensure the job is indeed in its queue. See the docs for Queue.ensure_enqueued() for more information.
        """
        if self.has_started:
            raise AlreadyAcquired(
                "Job {id} has already been started, will not enqueue".format(id=self.id)
            )
        if self.has_finished:
            raise AlreadyResulted(
                "Job {id} has already been finished, will not enqueue".format(
                    id=self.id
                )
            )
        status = self.status
        if status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            raise InvalidStatus(
                "Job {id} has status {status}, will not enqueue".format(
                    id=self.id, status=status
                )
            )
        return self.get_queue().ensure_enqueued(self)

    @property
    def redis_key(self) -> str:
        return "%s%s" % (JOB_KEY_PREFIX, self.id)

    @property
    def result_redis_key(self) -> str:
        return "%s%s" % (RESULT_KEY_PREFIX, self.id)

    @property
    def acquisition_info(self) -> Optional[dict]:
        # Acquisition info is always stored as JSON, not with the `encoding`
        acquisition_json = self.redis.hget(self.redis_key, "acquired")
        if acquisition_json:
            return json.loads(acquisition_json.decode())
        return None

    @property
    def has_finished(self) -> int:
        return self.redis.exists(self.result_redis_key)

    @property
    def has_started(self) -> bool:
        return self.redis.hexists(self.redis_key, "acquired")

    @property
    def encoded_result(self) -> Optional[bytes]:
        return self.redis.get(self.result_redis_key)

    @property
    def result(self) -> Optional[Any]:
        result_data = self.encoded_result
        if result_data is not None:
            return self.get_encoding().decode(result_data)
        return None

    @property
    def encoded_meta(self) -> Optional[bytes]:
        return self.redis.hget(self.redis_key, "meta")

    @property
    def meta(self) -> Optional[Any]:
        """
        Get any possible in-band progress metadata for this job.
        """
        meta_data = self.encoded_meta
        if meta_data is not None:
            return self.get_encoding().decode(meta_data)
        return None

    @property
    def status(self) -> JobStatus:
        return JobStatus(self.redis.hget(self.redis_key, "status").decode())  # type: ignore

    @property
    def heartbeat(self) -> Optional[float]:
        """
        Get heartbeat of this job after job's refresh_heartbeat has been called
        """
        heartbeat = self.redis.hget(self.redis_key, "heartbeat")
        if heartbeat is None:
            return None
        return float(heartbeat)  # type: ignore

    @property
    def exists(self) -> int:
        return self.redis.exists(self.redis_key)

    @property
    def result_ttl(self) -> int:
        return int(self.redis.hget(self.redis_key, "result_ttl"))  # type: ignore

    @property
    def duration(self) -> float:
        return float(self.redis.hget(self.redis_key, "duration"))  # type: ignore

    @property
    def queue_name(self) -> str:
        return self.redis.hget(self.redis_key, "queue").decode()  # type: ignore

    @property
    def kwargs(self) -> dict:
        kwargs_data = self.redis.hget(self.redis_key, "kwargs")
        if not kwargs_data:
            return {}
        return self.get_encoding().decode(kwargs_data)

    @property
    def callable_name(self) -> str:
        return self.redis.hget(self.redis_key, "callable").decode()  # type: ignore

    @property
    def encoding_name(self) -> str:
        encoding_name = self.redis.hget(self.redis_key, "encoding_name")
        if encoding_name:
            return encoding_name.decode()
        # If there is no encoding name, assume this job is from an earlier version,
        # and use the default encoding.
        return encoding.default_encoding_name or "no default encoding set"

    def get_encoding(self) -> encoding.BaseEncoding:
        return encoding.registry[self.encoding_name]()

    def get_queue(self) -> "Queue":
        from minique.models.queue import Queue

        return Queue(redis=self.redis, name=self.queue_name)

    def set_meta(self, meta: Any):
        """
        Set the "in-band" progress metadata for this job.
        Keep it short, though, for performance's sake...
        """
        self.redis.hset(self.redis_key, "meta", self.get_encoding().encode(meta))

    def refresh_heartbeat(self):
        """
        Set time.time() into heartbeat.
        """
        self.redis.hset(self.redis_key, "heartbeat", time.time())

    def __str__(self):
        return "<job %s>" % self.id

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Job) and (self.id == other.id)
