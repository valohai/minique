import pytest

from minique.api import enqueue, get_job
from minique.enums import JobStatus
from minique.work.worker import Worker
from minique_tests.jobs import reverse_job_id


@pytest.mark.parametrize('success', (False, True))
def test_basics(redis, success, random_queue_name):
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


def test_worker_empty_queue(redis, random_queue_name):
    worker = Worker.for_queue_names(redis, random_queue_name)
    assert not worker.tick()


def test_job_object_access(redis, random_queue_name):
    job = enqueue(redis, random_queue_name, reverse_job_id)
    Worker.for_queue_names(redis, random_queue_name).tick()
    assert job.result == job.id[::-1]
