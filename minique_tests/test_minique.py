import pytest
from redis.client import Redis

from minique.api import cancel_job, enqueue, get_job
from minique.enums import JobStatus
from minique.models.queue import Queue
from minique.testing import run_synchronously
from minique.work.worker import Worker
from minique_tests.jobs import reverse_job_id


@pytest.mark.parametrize('success', (False, True))
def test_basics(redis: Redis, success: bool, random_queue_name: str) -> None:
    kwargs = {'a': 10, 'b': (15 if success else 0)}
    job = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values', kwargs)
    assert not job.has_finished
    assert job.kwargs == kwargs
    worker = Worker.for_queue_names(redis, [random_queue_name])
    r_job = worker.tick()
    assert job == r_job  # we executed that particular job, right?
    for job in (job, get_job(redis, job.id)):
        assert job.has_finished
        assert job.acquisition_info['worker'] == worker.id
        assert job.duration > 0
        if success:
            assert job.status == JobStatus.SUCCESS
            assert job.result == 25
        else:
            assert job.status == JobStatus.FAILED


def test_worker_empty_queue(redis: Redis, random_queue_name: str) -> None:
    worker = Worker.for_queue_names(redis, random_queue_name)
    assert not worker.tick()


def test_job_object_access(redis: Redis, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, reverse_job_id)
    run_synchronously(job)
    assert job.result == job.id[::-1]


def test_cancel(redis: Redis, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values')
    assert Queue(redis, random_queue_name).length == 1
    cancel_job(redis, job.id)
    assert Queue(redis, random_queue_name).length == 0  # Canceling does remove the job from the queue
    worker = Worker.for_queue_names(redis, random_queue_name).tick()


def test_ensure_enqueued(redis: Redis, random_queue_name: str) -> None:
    j1 = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values')
    j2 = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values')
    queue = j1.get_queue()
    assert queue.length == 2
    assert j1.ensure_enqueued() == (False, 0)  # Did not need to re-enqueue
    assert j2.ensure_enqueued() == (False, 1)  # Did not need to re-enqueue
    assert redis.lpop(queue.redis_key) == j1.id.encode()  # pop first item, must be the first job
    assert queue.length == 1
    assert j1.ensure_enqueued() == (True, 1)  # Did re-enqueue in last position
    assert j2.ensure_enqueued() == (False, 0)  # Did not need to re-enqueue
    Worker.for_queue_names(redis, queue.name).tick()
    assert queue.length == 1
    Worker.for_queue_names(redis, queue.name).tick()
    assert queue.length == 0
    for job in (j1, j2):
        with pytest.raises(Exception):  # Refuses to be enqueued after completion
            job.ensure_enqueued()
