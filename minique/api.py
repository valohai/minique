from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import minique.encoding as encoding
from minique.enums import JobStatus
from minique.excs import DuplicateJob
from minique.json import to_json_bytes
from minique.models.job import Job
from minique.models.priority_queue import PriorityQueue
from minique.models.queue import Queue
from minique.utils import get_random_pronounceable_string
from minique.utils.affinity import (
    get_affinity_queue_name,
    get_affinity_sub_queue_names,
    normalize_affinity,
)
from minique.utils.cleaning import (
    remove_orphaned_prio_hashes,
    trim_phantom_ids,
)

if TYPE_CHECKING:
    from minique.types import RedisClient


def enqueue(
    redis: RedisClient,
    queue_name: str,
    callable: Callable[..., Any] | str,
    kwargs: dict[str, Any] | None = None,
    job_id: str | None = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7,
    encoding_name: str | None = None,
    affinity: list[str] | None = None,
) -> Job:
    """
    Queue up callable as a job.

    :param redis: Redis connection
    :param queue_name: Name of the queue to enqueue the job in.
    :param callable: A dotted path to the callable to execute on the worker.
    :param kwargs: Keyword arguments to pass to the callable.
    :param job_id: An identifier for the job; defaults to a random string.
    :param job_ttl: Time-to-live for the job in seconds; defaults to never expire.
    :param result_ttl: Time-to-live for the result in seconds; defaults to 7 days.
    :param encoding_name: Name of the encoding to use for the job payload; defaults to JSON.
    :param affinity: Optional best-effort affinity specifiers (e.g. dataset IDs or
                     image digests). Workers that recently ran jobs with the same
                     specifier will prefer to pick this job up, to exploit warm caches.
                     This is purely an optimization; the job is always reachable on the
                     base queue regardless.
    :raises minique.excs.DuplicateJob: If a job with the same ID already exists.
    :raises minique.excs.NoSuchJob: If the job does not exist right after creation.
    """
    return _define_and_store_job(
        redis=redis,
        callable=callable,
        kwargs=kwargs,
        job_id=job_id,
        job_ttl=job_ttl,
        result_ttl=result_ttl,
        encoding_name=encoding_name,
        queue=Queue(redis, queue_name),
        affinity=affinity,
    )


def enqueue_priority(
    redis: RedisClient,
    queue_name: str,
    callable: Callable[..., Any] | str,
    kwargs: dict[str, Any] | None = None,
    job_id: str | None = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7,
    encoding_name: str | None = None,
    priority: int = 0,
    affinity: list[str] | None = None,
) -> Job:
    """
    Queue up callable as a job, placing the job at the last place for its priority.

    :param redis: Redis connection
    :param queue_name: Name of the queue to enqueue the job in.
    :param callable: A dotted path to the callable to execute on the worker.
    :param kwargs: Keyword arguments to pass to the callable.
    :param job_id: An identifier for the job; defaults to a random string.
    :param job_ttl: Time-to-live for the job in seconds; defaults to never expire.
    :param result_ttl: Time-to-live for the result in seconds; defaults to 7 days.
    :param encoding_name: Name of the encoding to use for the job payload; defaults to JSON.
    :param priority: Priority number of this job, defaults to zero.
    :param affinity: Optional best-effort affinity specifiers; see `enqueue`. Each
                     affinity sub-queue is itself a priority queue, so within-affinity
                     ordering still respects `priority`. Note that affinity outranks
                     priority on a warm worker: it checks its affinity sub-queues before
                     the base queue, so a lower-priority affine job can be picked up ahead
                     of a higher-priority job in the base queue. Don't attach affinity to
                     jobs that require strict global priority ordering.
    :raises minique.excs.DuplicateJob: If a job with the same ID already exists.
    :raises minique.excs.NoSuchJob: If the job does not exist right after creation.
    """
    return _define_and_store_job(
        redis=redis,
        callable=callable,
        kwargs=kwargs,
        job_id=job_id,
        job_ttl=job_ttl,
        result_ttl=result_ttl,
        encoding_name=encoding_name,
        queue=PriorityQueue(redis, queue_name),
        priority=priority,
        affinity=affinity,
    )


def store(
    redis: RedisClient,
    callable: Callable[..., Any] | str,
    kwargs: dict[str, Any] | None = None,
    job_id: str | None = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7,
    encoding_name: str | None = None,
) -> Job:
    """
    Store callable as a job without placing it in the queue.

    :param redis: Redis connection
    :param callable: A dotted path to the callable to execute on the worker.
    :param kwargs: Keyword arguments to pass to the callable.
    :param job_id: An identifier for the job; defaults to a random string.
    :param job_ttl: Time-to-live for the job in seconds; defaults to never expire.
    :param result_ttl: Time-to-live for the result in seconds; defaults to 7 days.
    :param encoding_name: Name of the encoding to use for the job payload; defaults to JSON.
    :raises minique.excs.DuplicateJob: If a job with the same ID already exists.
    :raises minique.excs.NoSuchJob: If the job does not exist right after creation.
    """
    return _define_and_store_job(
        redis=redis,
        callable=callable,
        kwargs=kwargs,
        job_id=job_id,
        job_ttl=job_ttl,
        result_ttl=result_ttl,
        encoding_name=encoding_name,
    )


def get_job(
    redis: RedisClient,
    job_id: str,
) -> Job:
    job = Job(redis, job_id)
    job.ensure_exists()
    return job


def cancel_job(
    redis: RedisClient,
    job_id: str,
    expire_time: int | None = None,
) -> bool:
    """
    Cancel the job with the given job ID.

    If a worker is already busy with the job, it may not immediately quit,
    and as such, the job is not set to canceled state.

    You can optionally specify an expiration time for the job object; this overrides
    any `job_ttl` set in the job payload itself.  The assumption is that you don't
    need a canceled job object to hang around forever.

    :param redis: Redis connection
    :param job_id: Job ID.
    :param expire_time: Expiration time for the job object in seconds.
    :raises minique.excs.NoSuchJob: If the job does not exist.
    """
    job = get_job(redis, job_id)
    if not (job.has_finished or job.has_started):
        with redis.pipeline() as p:
            p.hset(job.redis_key, "status", JobStatus.CANCELLED.value)
            queue_name = job.get_queue_name()
            if queue_name:
                job.dequeue()
            if expire_time:
                p.expire(job.redis_key, expire_time)
            p.execute()
        return True
    return False


def _define_and_store_job(  # noqa: C901
    *,
    redis: RedisClient,
    callable: Callable[..., Any] | str,
    kwargs: dict[str, Any] | None = None,
    job_id: str | None = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7,
    encoding_name: str | None = None,
    queue: Queue | None = None,
    priority: int | None = None,
    affinity: list[str] | None = None,
) -> Job:
    if not encoding_name:
        encoding_name = encoding.default_encoding_name
    encoder = encoding.registry[str(encoding_name)]()

    if not isinstance(callable, str):
        callable = f"{callable.__module__}.{callable.__qualname__}"

    if not job_id:
        job_id = get_random_pronounceable_string()

    job = Job(redis, job_id)
    if job.exists:
        raise DuplicateJob(f"duplicate job: {job_id}")

    payload: dict[str, Any] = {
        "callable": str(callable),
        "kwargs": encoder.encode(kwargs or {}),
        "encoding_name": encoding_name,
        "status": JobStatus.NONE.value,
        "ctime": time.time(),
        "job_ttl": int(job_ttl),
        "result_ttl": int(result_ttl),
    }
    if queue:
        payload["queue"] = queue.name
    if priority is not None:
        payload["priority"] = priority

    if affinity := normalize_affinity(affinity):
        payload["affinity"] = to_json_bytes(affinity)

    with redis.pipeline() as p:
        p.hset(job.redis_key, mapping=payload)  # type: ignore[arg-type]
        if payload["job_ttl"] > 0:
            p.expire(job.redis_key, payload["job_ttl"])

        if queue:
            if affinity:
                queue.add_job_to_pipeline(p, job)
                for specifier in affinity:
                    sub_queue_name = get_affinity_queue_name(queue.name, specifier)
                    sub_queue = type(queue)(redis, sub_queue_name)
                    sub_queue.add_job_to_pipeline(p, job)
            else:
                queue.add_job_to_pipeline(p, job)

        p.execute()

    return job


def clean_affinity_sub_queues(
    redis: RedisClient,
    base: str,
    *,
    priority: bool = False,
) -> int:
    """Trim residue from `base`'s affinity sub-queues; returns the number of items removed.

    This doesn't need to be called often.
    """
    removed = 0
    for name in get_affinity_sub_queue_names(redis, base):
        sub_queue = PriorityQueue(redis, name) if priority else Queue(redis, name)
        removed += trim_phantom_ids(redis, sub_queue.redis_key, priority=priority)
    if priority:
        removed += remove_orphaned_prio_hashes(redis, base)
    return removed
