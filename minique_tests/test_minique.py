import threading

import pytest
from redis.client import Redis

from minique.api import cancel_job, enqueue, get_job, store
from minique.enums import JobStatus
from minique.excs import InvalidStatus, MissingJobData
from minique.models.queue import Queue
from minique.testing import run_synchronously
from minique_tests.jobs import (
    job_with_a_message,
    message_test_inbox,
    message_test_outbox,
    reverse_job_id,
)
from minique_tests.worker import TestWorker


@pytest.mark.parametrize("success", (False, True))
def test_basics(redis: Redis, success: bool, random_queue_name: str) -> None:
    kwargs = {"a": 10, "b": (15 if success else 0)}
    job = enqueue(
        redis, random_queue_name, "minique_tests.jobs.sum_positive_values", kwargs
    )
    assert not job.has_finished
    assert job.kwargs == kwargs
    worker = TestWorker.for_queue_names(redis, [random_queue_name])
    r_job = worker.tick()
    assert job == r_job  # we executed that particular job, right?
    for job in (job, get_job(redis, job.id)):  # noqa: B020
        assert job.encoding_name == "json"
        assert job.has_finished
        assert job.acquisition_info["worker"] == worker.id
        assert job.duration > 0
        if success:
            assert job.status == JobStatus.SUCCESS
            assert job.result == 25
        else:
            assert job.status == JobStatus.FAILED


def test_worker_empty_queue(redis: Redis, random_queue_name: str) -> None:
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    assert not worker.tick()


def test_job_object_access(redis: Redis, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, reverse_job_id)
    run_synchronously(job)
    assert job.result == job.id[::-1]


def test_job_message(redis: Redis, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, job_with_a_message)
    worker_thread = threading.Thread(target=run_synchronously, args=(job,))
    worker_thread.start()

    def check_mailbox(expected_message):
        nonce = message_test_outbox.get()
        assert job.meta == expected_message
        message_test_inbox.put(nonce)

    check_mailbox("oh, hello")
    check_mailbox({"message": "progress occurs", "status": [1, 2, 3]})
    worker_thread.join()
    assert job.result == 42


def test_cancel(redis: Redis, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    assert Queue(redis, random_queue_name).length == 1
    cancel_job(redis, job.id)
    # Canceling does remove the job from the queue?
    assert Queue(redis, random_queue_name).length == 0
    TestWorker.for_queue_names(redis, random_queue_name).tick()


def test_ensure_enqueued(redis: Redis, random_queue_name: str) -> None:
    j1 = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    j2 = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    queue = j1.get_queue()
    assert queue.length == 2
    assert j1.ensure_enqueued() == (False, 0)  # Did not need to re-enqueue
    assert j2.ensure_enqueued() == (False, 1)  # Did not need to re-enqueue
    j1_id = j1.id.encode()
    assert redis.lpop(queue.redis_key) == j1_id  # pop first item, must be the first job
    assert queue.length == 1
    assert j1.ensure_enqueued() == (True, 1)  # Did re-enqueue in last position
    assert j2.ensure_enqueued() == (False, 0)  # Did not need to re-enqueue
    TestWorker.for_queue_names(redis, queue.name).tick()
    assert queue.length == 1
    TestWorker.for_queue_names(redis, queue.name).tick()
    assert queue.length == 0
    for job in (j1, j2):
        with pytest.raises(InvalidStatus):  # Refuses to be enqueued after completion
            job.ensure_enqueued()


def test_get_job_dequeue(redis: Redis, random_queue_name: str) -> None:
    j1 = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    j2 = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    j3 = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    queue = Queue(redis, random_queue_name)
    assert queue.length == 3

    # dequeue on initial job
    assert j1.dequeue()
    assert queue.length == 2

    # dequeue on a fetched job
    r_j2 = get_job(redis, j2.id)
    assert r_j2 == j2
    assert r_j2.dequeue()
    assert queue.length == 1

    # dequeue returns False if nothing is removed
    assert not j1.dequeue()
    assert not r_j2.dequeue()
    assert queue.length == 1
    assert get_job(redis, j3.id) == j3


def test_stored_jobs(redis: Redis, random_queue_name: str) -> None:
    job = store(redis, reverse_job_id)
    with pytest.raises(MissingJobData, match="has no queue"):
        _ = job.queue_name
    r_job = get_job(redis, job.id)
    assert r_job == job


def test_dequeue_stored_job(redis: Redis):
    stored_job = store(
        redis,
        "minique_tests.jobs.sum_positive_values",
    )
    with pytest.raises(MissingJobData, match="has no queue"):
        stored_job.dequeue()


def test_stored_job_cancel(redis: Redis, random_queue_name: str) -> None:
    job = store(redis, reverse_job_id)
    assert get_job(redis, job.id).status == JobStatus.NONE
    assert cancel_job(redis, job.id)
    assert get_job(redis, job.id).status == JobStatus.CANCELLED
