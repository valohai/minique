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
