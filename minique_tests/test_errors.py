import time
import uuid

import pytest
from redis import Redis

from minique.api import enqueue
from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, DuplicateJob, NoSuchJob
from minique.models.queue import Queue
from minique_tests.jobs import job_with_unjsonable_retval
from minique_tests.worker import TestWorker


def check_sentry_event_calls(sentry_event_calls, num_expected: int):
    # Check that an error got recorded, if Sentry is enabled
    if sentry_event_calls is not None:
        assert len(sentry_event_calls) == num_expected

        error_in_kwargs = any(
            call.kwargs.get("event", {}).get("level") == "error"
            for call in sentry_event_calls
        )
        if error_in_kwargs:
            return

        error_in_args = any(
            call.args and call.args[0]["level"] == "error"
            for call in sentry_event_calls
        )
        if error_in_args:
            return

        raise AssertionError("No `error` level event recorded in Sentry")


def test_unjsonable_arg(redis: Redis, random_queue_name: str):
    kwargs = {
        "phooey": uuid.uuid4(),
    }
    with pytest.raises(TypeError):
        enqueue(redis, random_queue_name, job_with_unjsonable_retval, kwargs)


def test_unjsonable_retval(redis: Redis, random_queue_name: str, sentry_event_calls):
    job = enqueue(redis, random_queue_name, job_with_unjsonable_retval)
    TestWorker.for_queue_names(redis, random_queue_name).tick()
    assert job.status == JobStatus.FAILED
    assert job.result["exception_type"] == "TypeError"
    assert "not JSON serializable" in job.result["exception_value"]
    check_sentry_event_calls(sentry_event_calls, 1)


def test_disappeared_job(redis: Redis, random_queue_name: str):
    enqueue(
        redis, random_queue_name, "minique_tests.jobs.sum_positive_values", job_ttl=1
    )
    assert Queue(redis, random_queue_name).length == 1
    time.sleep(2)
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    with pytest.raises(NoSuchJob):  # It's expired :(
        worker.tick()


def test_rerun_done_job(redis: Redis, random_queue_name: str, sentry_event_calls):
    job = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.tick()
    assert job.has_finished
    # This should normally never be possible,
    # but let's re-enqueue the job anyway by touching some internals:
    queue = Queue(redis, random_queue_name)
    redis.rpush(queue.redis_key, job.id)
    with pytest.raises(AlreadyAcquired):
        worker.tick()
    check_sentry_event_calls(sentry_event_calls, 2)


def test_duplicate_names(redis: Redis, random_queue_name: str):
    job = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    with pytest.raises(DuplicateJob):
        enqueue(
            redis,
            random_queue_name,
            "minique_tests.jobs.sum_positive_values",
            job_id=job.id,
        )


def test_invalid_callable_name(
    redis: Redis, random_queue_name: str, sentry_event_calls
):
    job = enqueue(redis, random_queue_name, "os.system", {"command": "evil"})
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.tick()
    assert job.has_finished
    assert job.status == JobStatus.FAILED
    assert job.result["exception_type"] == "InvalidJob"
    check_sentry_event_calls(sentry_event_calls, 1)
