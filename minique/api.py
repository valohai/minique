import json
import time
from typing import Callable, Optional, Union

from redis import Redis

from minique.enums import JobStatus
from minique.excs import DuplicateJob
from minique.models.job import Job
from minique.models.queue import Queue
from minique.utils import get_random_pronounceable_string


def enqueue(
    redis: Redis,
    queue_name: str,
    callable: Union[Callable, str],
    kwargs: Optional[dict] = None,
    job_id: Optional[str] = None,
    job_ttl: int = 0,
    result_ttl: int = 86400 * 7
) -> Job:
    if not isinstance(callable, str):
        callable = '{module}.{qualname}'.format(
            module=callable.__module__,
            qualname=callable.__qualname__,
        )

    if not job_id:
        job_id = get_random_pronounceable_string()
    job = Job(redis, job_id)
    if job.exists:
        raise DuplicateJob('duplicate job: {id}'.format(id=job_id))
    payload = {
        'queue': queue_name,
        'callable': str(callable),
        'kwargs': json.dumps(kwargs or {}),
        'status': JobStatus.NONE.value,
        'ctime': time.time(),
        'job_ttl': int(job_ttl),
        'result_ttl': int(result_ttl),
    }
    queue = Queue(redis, name=queue_name)
    queue.enqueue_initial(job=job, payload=payload)
    job.ensure_exists()
    return job


def get_job(redis: Redis, job_id: str) -> Job:
    job = Job(redis, job_id)
    job.ensure_exists()
    return job


def cancel_job(redis: Redis, job_id: str) -> bool:
    """
    Cancel the job with the given job ID.

    If a worker is already busy with the job, it may not immediately quit,
    and as such, the job is not set to cancelled state.

    :type redis: redis.Redis
    :param job_id: Job ID.
    :raises minique.excs.NoSuchJob: if the job does not exist.
    """
    job = get_job(redis, job_id)
    if not (job.has_finished or job.has_started):
        # Cancel the job and remove it from the queue it may be in.
        redis.hset(job.redis_key, 'status', JobStatus.CANCELLED.value)
        redis.lrem(job.get_queue().redis_key, 0, job.id)
        return True
    return False
