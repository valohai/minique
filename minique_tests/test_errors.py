import time

import pytest

from minique.api import enqueue
from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, DuplicateJob, NoSuchJob
from minique.models.queue import Queue
from minique.work.worker import Worker
from minique_tests.jobs import job_with_unjsonable_retval


def test_unjsonable_retval(redis, random_queue_name):
    job = enqueue(redis, random_queue_name, job_with_unjsonable_retval)
    Worker.for_queue_names(redis, random_queue_name).tick()
    assert job.status == JobStatus.FAILED
    assert job.result['exception_type'] == 'TypeError'
    assert 'not JSON serializable' in job.result['exception_value']


def test_disappeared_job(redis, random_queue_name):
    enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values', job_ttl=1)
    assert Queue(redis, random_queue_name).length == 1
    time.sleep(2)
    worker = Worker.for_queue_names(redis, random_queue_name)
    with pytest.raises(NoSuchJob):  # It's expired :(
        worker.tick()


def test_rerun_done_job(redis, random_queue_name):
    job = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values')
    worker = Worker.for_queue_names(redis, random_queue_name)
    worker.tick()
    assert job.has_finished
    # This should normally never be possible,
    # but let's re-enqueue the job anyway by touching some internals:
    queue = Queue(redis, random_queue_name)
    redis.rpush(queue.redis_key, job.id)
    with pytest.raises(AlreadyAcquired):
        worker.tick()


def test_duplicate_names(redis, random_queue_name):
    job = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values')
    with pytest.raises(DuplicateJob):
        enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values', job_id=job.id)
