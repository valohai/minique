from collections.abc import Iterable

import pytest
from redis.client import Redis

from minique.api import cancel_job, enqueue, enqueue_priority
from minique.models.job import Job
from minique.models.priority_queue import PriorityQueue
from minique_tests.worker import TestWorker


def assert_queue_items(queue: PriorityQueue, jobs: Iterable[Job]) -> None:
    queue_items = queue.redis.lrange(queue.redis_key, 0, -1)
    assert [i.decode() for i in queue_items] == [j.id for j in jobs]


def assert_queue_priorities(
    queue: PriorityQueue, priority_job_map: Iterable[tuple[Job, int]]
) -> None:
    assert queue.redis.hgetall(queue.prio_key) == {
        job.id.encode(): str(prio).encode() for job, prio in priority_job_map
    }


@pytest.fixture()
def enqueue_job(redis: Redis, random_queue_name: str):
    def inner(priority=0, job_id=None):
        return enqueue_priority(
            redis,
            random_queue_name,
            "minique_tests.jobs.sum_positive_values",
            priority=priority,
            job_id=job_id,
        )

    return inner


def test_priority_queueing(redis: Redis, enqueue_job):
    """Test that jobs can be added to the queue with varying priority"""

    j_p0 = enqueue_job(priority=0)
    j_pn1 = enqueue_job(priority=-1)
    j_p2 = enqueue_job(priority=2)
    j_pn2 = enqueue_job(priority=-2)

    # Add more jobs in existing priorities; these should slot into the right places in the
    # queue list
    j_p0_2 = enqueue_job(priority=0)
    j_pn1_2 = enqueue_job(priority=-1)
    j_p2_2 = enqueue_job(priority=2)
    j_pn2_2 = enqueue_job(priority=-2)

    # Add a new job in a new priority between existing ones
    j_p1 = enqueue_job(priority=1)

    queue = j_p0.get_queue()
    assert isinstance(queue, PriorityQueue)
    assert queue.length == 9

    # Exact order of items is by priority block, and FIFO within priority blocks
    assert_queue_items(
        queue,
        [
            j_p2,
            j_p2_2,
            j_p1,
            j_p0,
            j_p0_2,
            j_pn1,
            j_pn1_2,
            j_pn2,
            j_pn2_2,
        ],
    )
    assert_queue_priorities(
        queue,
        [
            (j_p2, 2),
            (j_p2_2, 2),
            (j_p1, 1),
            (j_p0, 0),
            (j_p0_2, 0),
            (j_pn1, -1),
            (j_pn1_2, -1),
            (j_pn2, -2),
            (j_pn2_2, -2),
        ],
    )


def test_larger_queue(redis: Redis, enqueue_job, random_queue_name):
    job_ids = [
        enqueue_job(priority=i % 13 - 6, job_id=f"job_{random_queue_name}_1_{i}").id
        for i in range(1000)
    ]

    queue = PriorityQueue(redis=redis, name=random_queue_name)
    assert queue.length == 1000

    # All jobs are queued and order is correct
    max_priority = 6
    for job_id in redis.lrange(queue.redis_key, 0, -1):
        job_id = job_id.decode()
        job_priority = int(redis.hget(queue.prio_key, job_id))
        assert job_priority <= max_priority
        assert job_id in job_ids
        max_priority = min(job_priority, max_priority)

    # Drain the queue, refill it and rebuild the priority lookup hash
    redis.delete(queue.redis_key)
    new_job_ids = {
        enqueue_job(
            priority=i % 13 - 6, job_id=f"job_{random_queue_name}_2_{i}"
        ).id.encode()
        for i in range(1000)
    }
    assert queue.periodic_clean() == 1000
    # New jobs are the only jobs in the prio hash
    assert set(redis.hkeys(queue.prio_key)) == new_job_ids


@pytest.mark.parametrize("operation", ["cancel_job", "dequeue"])
def test_dequeue_job(redis: Redis, enqueue_job, operation):
    j_p0, j_p1, j_p2 = [enqueue_job(priority=n) for n in range(3)]
    queue = j_p0.get_queue()
    if operation == "dequeue":
        # Dequeueing removes the job from the queue and priority hash right away
        assert j_p0.dequeue()
    elif operation == "cancel_job":
        # Cancelling the job also removes it from the queue and priority hash right away
        assert cancel_job(redis, j_p0.id)
    assert_queue_priorities(
        queue,
        [
            (j_p1, 1),
            (j_p2, 2),
        ],
    )
    assert queue.length == 2
    assert_queue_items(
        queue,
        [
            j_p2,
            j_p1,
        ],
    )


def test_ensure_priority_queued(redis: Redis, random_queue_name: str, enqueue_job):
    j_p0, j_p1, j_p2 = [enqueue_job(priority=n) for n in range(3)]
    j_p1.dequeue()
    queue = j_p0.get_queue()
    was_queued, index = j_p1.ensure_enqueued()
    assert was_queued
    assert index == 1
    # returns to the appropriate place (= used priority queue)
    assert_queue_items(
        queue,
        [
            j_p2,
            j_p1,
            j_p0,
        ],
    )


def test_worker_consumes_priority_job(
    redis: Redis, random_queue_name: str, enqueue_job
):
    worker = TestWorker.for_queue_names(redis, [random_queue_name])

    j_p0 = enqueue_job(priority=0)
    j_p1 = enqueue_job(priority=1)
    j_p1_2 = enqueue_job(priority=1)
    queue = j_p0.get_queue()

    run_job = worker.tick()
    assert run_job == j_p1

    # worker doesn't remove the job from the priorities hash
    assert_queue_priorities(
        queue,
        [
            (j_p0, 0),
            (j_p1, 1),
            (j_p1_2, 1),
        ],
    )

    # finishing the job removes the job from the priorities hash
    j_p1.cleanup()
    assert_queue_priorities(
        queue,
        [
            (j_p0, 0),
            (j_p1_2, 1),
        ],
    )

    # it can be called again
    j_p1.cleanup()


def test_non_priority_mixup(redis: Redis, random_queue_name: str):
    """Test that priority queueing does not crash even if there are jobs without a
    priority value in the hash table

    This can happen if the queue clients use priority and non-priority queues interchangeably
    for some reason. However, we don't guarantee the queue is in a sensible order if so nor
    attempt to repair it.
    """
    for i in range(10):
        if i % 2:
            enqueue_priority(
                redis,
                random_queue_name,
                "minique_tests.jobs.sum_positive_values",
                priority=i - 5,
            )
        else:
            enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    assert PriorityQueue(redis, random_queue_name).length == 10
