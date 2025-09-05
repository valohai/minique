from __future__ import annotations

import json
import time
from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any

from minique import encoding
from minique.consts import JOB_KEY_PREFIX, RESULT_KEY_PREFIX
from minique.enums import JobStatus
from minique.excs import (
    AlreadyAcquired,
    AlreadyResulted,
    InvalidStatus,
    MissingJobData,
    NoSuchJob,
)

if TYPE_CHECKING:
    from redis import Redis

    from minique.models.priority_queue import PriorityQueue
    from minique.models.queue import Queue


class Job:  # noqa: PLW1641 (this class has no __hash__ by design)
    # Used for testing:
    replacement_callable: Callable[..., Any] | None = None
    replacement_kwargs: dict[str, Any] | None = None

    def __init__(self, redis: Redis[bytes], id: str) -> None:
        self.redis = redis
        self.id = str(id)

    def ensure_exists(self) -> None:
        if not self.exists:
            raise NoSuchJob(f"Job {self.id} does not exist")

    def ensure_enqueued(self) -> tuple[bool, int]:
        """
        Ensure the job is indeed in its queue. See the docs for Queue.ensure_enqueued() for more information.
        """
        if self.has_started:
            raise AlreadyAcquired(
                f"Job {self.id} has already been started, will not enqueue",
            )
        if self.has_finished:
            raise AlreadyResulted(
                f"Job {self.id} has already been finished, will not enqueue",
            )
        status = self.status
        if status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            raise InvalidStatus(f"Job {self.id} has status {status}, will not enqueue")

        return self.get_queue().ensure_enqueued(self)

    def dequeue(self) -> bool:
        """
        Remove the job from the queue without changing its status.
        """
        return self.get_queue().dequeue_job(self.id)

    def cleanup(self) -> bool:
        if self.has_priority:
            queue = self.get_queue()
            if hasattr(queue, "clean_job"):  # avoids import
                queue.clean_job(self)
                return True

        return False

    @property
    def redis_key(self) -> str:
        return f"{JOB_KEY_PREFIX}{self.id}"

    @property
    def result_redis_key(self) -> str:
        return f"{RESULT_KEY_PREFIX}{self.id}"

    @property
    def acquisition_info(self) -> dict[str, Any] | None:
        # Acquisition info is always stored as JSON, not with the `encoding`
        acquisition_json = self.redis.hget(self.redis_key, "acquired")
        if acquisition_json:
            return json.loads(acquisition_json.decode())  # type: ignore[no-any-return]
        return None

    @property
    def has_finished(self) -> int:
        has_result = self.redis.exists(self.result_redis_key)
        if has_result:
            self.cleanup()
        return has_result

    @property
    def has_started(self) -> bool:
        return self.redis.hexists(self.redis_key, "acquired")

    @property
    def encoded_result(self) -> bytes | None:
        return self.redis.get(self.result_redis_key)

    @property
    def result(self) -> Any | None:
        result_data = self.encoded_result
        if result_data is not None:
            self.cleanup()
            return self.get_encoding().decode(result_data)
        return None

    @property
    def encoded_meta(self) -> bytes | None:
        return self.redis.hget(self.redis_key, "meta")

    @property
    def meta(self) -> Any | None:
        """
        Get any possible in-band progress metadata for this job.
        """
        meta_data = self.encoded_meta
        if meta_data is not None:
            return self.get_encoding().decode(meta_data)
        return None

    @property
    def status(self) -> JobStatus:
        status = self.redis.hget(self.redis_key, "status")
        if status is None:
            raise MissingJobData(f"Job {self.id} has no status")
        return JobStatus(status.decode())

    @property
    def heartbeat(self) -> float | None:
        """
        Get heartbeat of this job after job's refresh_heartbeat has been called
        """
        heartbeat = self.redis.hget(self.redis_key, "heartbeat")
        if heartbeat is None:
            return None
        return float(heartbeat)

    @property
    def exists(self) -> int:
        return self.redis.exists(self.redis_key)

    @property
    def result_ttl(self) -> int:
        result_ttl = self.redis.hget(self.redis_key, "result_ttl")
        if result_ttl is None:
            raise MissingJobData(f"Job {self.id} has no result_ttl")
        return int(result_ttl)

    @property
    def duration(self) -> float:
        duration = self.redis.hget(self.redis_key, "duration")
        if duration is None:
            raise MissingJobData(f"Job {self.id} has no duration")
        return float(duration)

    @cached_property
    def has_priority(self) -> bool:
        return self.redis.hget(self.redis_key, "priority") is not None

    @property
    def priority(self) -> int:
        priority = self.redis.hget(self.redis_key, "priority")
        if priority is None:
            raise MissingJobData(f"Job {self.id} has no priority")
        return int(priority)

    @priority.setter
    def priority(self, new_priority: int) -> None:
        self.redis.hset(self.redis_key, "priority", new_priority)

    @property
    def queue_name(self) -> str:
        return self.get_queue_name(missing_ok=False)  # type:ignore[return-value]

    def get_queue_name(self, *, missing_ok: bool = True) -> str | None:
        queue = self.redis.hget(self.redis_key, "queue")
        if not queue:
            if missing_ok:
                return None
            raise MissingJobData(f"Job {self.id} has no queue")
        return queue.decode()

    @property
    def kwargs(self) -> dict[str, Any]:
        kwargs_data = self.redis.hget(self.redis_key, "kwargs")
        if not kwargs_data:
            return {}
        return self.get_encoding().decode(kwargs_data)  # type: ignore[no-any-return]

    @property
    def callable_name(self) -> str:
        callable_name = self.redis.hget(self.redis_key, "callable")
        if not callable_name:
            raise MissingJobData(f"Job {self.id} has no callable name")
        return callable_name.decode()

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

    def get_queue(self) -> Queue | PriorityQueue:
        from minique.models.priority_queue import PriorityQueue
        from minique.models.queue import Queue

        if self.has_priority:
            return PriorityQueue(redis=self.redis, name=self.queue_name)
        return Queue(redis=self.redis, name=self.queue_name)

    def set_meta(self, meta: Any) -> None:
        """
        Set the "in-band" progress metadata for this job.
        Keep it short, though, for performance's sake...
        """
        self.redis.hset(self.redis_key, "meta", self.get_encoding().encode(meta))

    def refresh_heartbeat(self) -> None:
        """
        Set time.time() into heartbeat.
        """
        self.redis.hset(self.redis_key, "heartbeat", time.time())

    def __str__(self) -> str:
        return f"<job {self.id}>"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Job) and (self.id == other.id)
