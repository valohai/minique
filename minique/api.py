import time
from typing import Any, Callable, Dict, Optional, Union

from redis import Redis

import minique.encoding as encoding
from minique.enums import JobStatus
from minique.excs import DuplicateJob
from minique.models.job import Job
from minique.models.queue import Queue
from minique.utils import get_random_pronounceable_string


def enqueue(
    redis: "Redis[bytes]",
    queue_name: str,
    callable: Union[Callable[..., Any], str],
    kwargs: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7,
    encoding_name: Optional[str] = None,
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
    payload = {
        "queue": queue_name,
        "callable": str(callable),
        "kwargs": encoder.encode(kwargs or {}),
        "encoding_name": encoding_name,
        "status": JobStatus.NONE.value,
        "ctime": time.time(),
        "job_ttl": int(job_ttl),
        "result_ttl": int(result_ttl),
    }
    queue = Queue(redis, name=queue_name)
    queue.enqueue_initial(job=job, payload=payload)
    job.ensure_exists()
    return job


def get_job(redis: "Redis[bytes]", job_id: str) -> Job:
    job = Job(redis, job_id)
    job.ensure_exists()
    return job


def cancel_job(
    redis: "Redis[bytes]",
    job_id: str,
    expire_time: Optional[int] = None,
) -> bool:
    """
    Cancel the job with the given job ID.

    If a worker is already busy with the job, it may not immediately quit,
    and as such, the job is not set to cancelled state.

    You can optionally specify an expire time for the job object; this overrides
    any `job_ttl` set in the job payload itself.  The assumption is that you don't
    need a canceled job object to hang around forever.

    :param redis: Redis connection
    :param job_id: Job ID.
    :param expire_time: Expiration time for the job object in seconds.
    :raises minique.excs.NoSuchJob: if the job does not exist.
    """
    job = get_job(redis, job_id)
    if not (job.has_finished or job.has_started):
        with redis.pipeline() as p:
            # Cancel the job and remove it from the queue it may be in.
            redis.hset(job.redis_key, "status", JobStatus.CANCELLED.value)
            redis.lrem(job.get_queue().redis_key, 0, job.id)
            if expire_time:
                redis.expire(job.redis_key, expire_time)
            p.execute()
        return True
    return False
