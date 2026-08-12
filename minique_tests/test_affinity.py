from __future__ import annotations

import time
from typing import TYPE_CHECKING

from minique.api import (
    clean_affinity_sub_queues,
    enqueue,
    enqueue_priority,
)
from minique.consts import QUEUE_KEY_PREFIX
from minique.enums import JobStatus
from minique.models.priority_queue import PriorityQueue
from minique.models.queue import Queue
from minique.utils.affinity import get_affinity_queue_name, get_affinity_sub_queue_names
from minique_tests.worker import TestWorker

if TYPE_CHECKING:
    from minique.types import RedisClient


def _list_ids(redis: RedisClient, key: str) -> list[str]:
    return [v.decode() for v in redis.lrange(key, 0, -1)]


def test_dual_write(redis: RedisClient, random_queue_name: str) -> None:
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        affinity=["x"],
    )
    assert job.affinity == ["x"]

    base_key = Queue(redis, random_queue_name).redis_key
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")
    # The job id is present in both the base queue and the affinity sub-queue.
    assert _list_ids(redis, base_key) == [job.id]
    assert _list_ids(redis, sub_key) == [job.id]


def test_no_affinity_leaves_no_extra_keys(
    redis: RedisClient, random_queue_name: str
) -> None:
    job = enqueue(redis, random_queue_name, "minique_tests.jobs.sum_positive_values")
    assert job.affinity == []
    assert list(get_affinity_sub_queue_names(redis, random_queue_name)) == []


def test_affinity_is_deduplicated(redis: RedisClient, random_queue_name: str) -> None:
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        affinity=["x", "x", "", "y"],
    )
    # Empty specifiers dropped, duplicates collapsed, order preserved.
    assert job.affinity == ["x", "y"]
    assert sorted(get_affinity_sub_queue_names(redis, random_queue_name)) == sorted(
        [
            get_affinity_queue_name(random_queue_name, "x"),
            get_affinity_queue_name(random_queue_name, "y"),
        ]
    )


def test_warm_worker_key_order(redis: RedisClient, random_queue_name: str) -> None:
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    base_key = Queue(redis, random_queue_name).redis_key

    # Cold worker: byte-for-byte the plain base-queue key list (no warm specifier).
    assert worker.get_blpop_keys() == {base_key: None}

    # Warm worker: the matching affinity sub-queue is listed ahead of the base queue,
    # mapped to the specifier it serves.
    worker.warm_specifiers["x"] = time.time()
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")
    assert worker.get_blpop_keys() == {sub_key: "x", base_key: None}
    # Insertion order is BLPOP priority order: sub-queue before base queue.
    assert list(worker.get_blpop_keys()) == [sub_key, base_key]


def test_warm_specifiers_prune_and_cap(redis: RedisClient) -> None:
    worker = TestWorker.for_queue_names(redis, "q")
    worker.affinity_window = 100
    worker.affinity_max_specifiers = 2

    now = time.time()
    worker.warm_specifiers = {
        "old": now - 1000,  # past the window -> pruned
        "a": now - 30,
        "b": now - 20,
        "c": now - 10,  # most recent
    }
    # Pruned to within-window, capped to 2, most-recent first.
    assert worker.get_current_warm_specifiers() == ["c", "b"]
    assert "old" not in worker.warm_specifiers
    # The cap applies to the backing dict too, not just the returned view, so the warm
    # set can't grow (and be re-sorted) without bound between ticks.
    assert set(worker.warm_specifiers) == {"c", "b"}


def test_worker_becomes_warm_after_running(
    redis: RedisClient, random_queue_name: str
) -> None:
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    job = worker.tick()
    assert job is not None and job.status == JobStatus.SUCCESS
    # Running an x-job makes the worker warm for x.
    assert worker.get_current_warm_specifiers() == ["x"]


def test_warm_worker_prefers_sub_queue(
    redis: RedisClient, random_queue_name: str
) -> None:
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()

    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    base_key = Queue(redis, random_queue_name).redis_key
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")

    # The warm worker pops the sub-queue copy and runs the job; acquiring it drains the
    # sibling base copy, so no phantom is left on the base queue to accumulate.
    ran = worker.tick()
    assert ran == job and job.status == JobStatus.SUCCESS
    assert _list_ids(redis, sub_key) == []
    assert _list_ids(redis, base_key) == []
    # Nothing left to pick up; no double-run.
    assert worker.tick() is None


def test_job_reports_affinity_source(
    redis: RedisClient, random_queue_name: str
) -> None:
    # A warm worker that pops a job off an affinity sub-queue exposes which specifier it
    # matched, so a JobRunner subclass can surface that it served warm work.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")

    enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    job = worker.tick()
    assert job is not None
    assert job.source_queue_key == sub_key
    assert job.affinity_specifier == "x"


def test_job_reports_base_queue_source_when_cold(
    redis: RedisClient, random_queue_name: str
) -> None:
    # A cold worker pops the base copy: source is the base queue and there's no specifier.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    base_key = Queue(redis, random_queue_name).redis_key

    enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    job = worker.tick()
    assert job is not None
    assert job.source_queue_key == base_key
    assert job.affinity_specifier is None


def test_base_queue_does_not_accumulate_phantoms(
    redis: RedisClient, random_queue_name: str
) -> None:
    # Regression: a warm worker draining many affine jobs via their sub-queues must not
    # leave a growing pile of phantom copies on the base queue.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()
    base_key = Queue(redis, random_queue_name).redis_key
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")

    for _ in range(5):
        enqueue(
            redis,
            random_queue_name,
            "minique_tests.jobs.sum_positive_values",
            kwargs={"a": 1, "b": 2},
            affinity=["x"],
        )
    for _ in range(5):
        assert worker.tick() is not None

    # Every copy of every job was drained at acquisition: nothing lingers anywhere.
    assert _list_ids(redis, base_key) == []
    assert _list_ids(redis, sub_key) == []


def test_dequeue_removes_affinity_copies(
    redis: RedisClient, random_queue_name: str
) -> None:
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    base_key = Queue(redis, random_queue_name).redis_key
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")

    # dequeue() removes the job from the base queue AND its affinity sub-queue(s)...
    assert job.dequeue()
    assert _list_ids(redis, base_key) == []
    assert _list_ids(redis, sub_key) == []
    # ...so a warm worker can no longer pop it from the sub-queue.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()
    assert worker.tick() is None


def test_clear_also_clears_affinity_sub_queues(
    redis: RedisClient, random_queue_name: str
) -> None:
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    base_key = Queue(redis, random_queue_name).redis_key
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")
    assert _list_ids(redis, sub_key) == [job.id]

    Queue(redis, random_queue_name).clear()

    # Neither the base queue nor the affinity sub-queue retains runnable work...
    assert _list_ids(redis, base_key) == []
    assert _list_ids(redis, sub_key) == []
    # ...so a warm worker finds nothing to run.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()
    assert worker.tick() is None


def test_clear_priority_queue_clears_affinity_sub_queue_prio_hashes(
    redis: RedisClient, random_queue_name: str
) -> None:
    enqueue_priority(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        priority=5,
        affinity=["x"],
    )
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")
    sub_prio_key = f"{sub_key}prio"
    base_prio_key = f"{QUEUE_KEY_PREFIX}{random_queue_name}prio"
    assert redis.exists(sub_key) and redis.exists(sub_prio_key)
    assert redis.exists(base_prio_key)

    PriorityQueue(redis, random_queue_name).clear()
    assert not redis.exists(sub_key)
    assert not redis.exists(sub_prio_key)
    # The base queue's own `…prio` hash has no affinity infix, so it must be cleared
    # explicitly rather than relying on the sub-queue glob (which can't match it).
    assert not redis.exists(base_prio_key)


def test_multi_queue_key_order_groups_by_queue(redis: RedisClient) -> None:
    # Queue-order-as-priority must be preserved: all of q1 (its affinity sub-queues
    # included) is preferred over anything in q2.
    worker = TestWorker.for_queue_names(redis, ["q1", "q2"])
    worker.warm_specifiers["x"] = time.time()

    q1 = Queue(redis, "q1").redis_key
    q2 = Queue(redis, "q2").redis_key
    q1x = QUEUE_KEY_PREFIX + get_affinity_queue_name("q1", "x")
    q2x = QUEUE_KEY_PREFIX + get_affinity_queue_name("q2", "x")
    assert list(worker.get_blpop_keys()) == [q1x, q1, q2x, q2]


def test_cold_worker_serves_base_copy(
    redis: RedisClient, random_queue_name: str
) -> None:
    # An affine job must be reachable by a worker that is not warm for its specifier.
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        affinity=["x"],
    )
    cold = TestWorker.for_queue_names(redis, random_queue_name)
    assert not cold.get_current_warm_specifiers()
    ran = cold.tick()
    assert ran == job and job.status == JobStatus.SUCCESS


def test_priority_affinity_sub_queue_and_cleanup(
    redis: RedisClient, random_queue_name: str
) -> None:
    job = enqueue_priority(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        priority=5,
        affinity=["x"],
    )
    sub_name = get_affinity_queue_name(random_queue_name, "x")
    sub_key = QUEUE_KEY_PREFIX + sub_name
    sub_prio_key = f"{sub_key}prio"
    # The sub-queue is a priority sub-queue with its own priority lookup hash.
    assert _list_ids(redis, sub_key) == [job.id]
    assert redis.hget(sub_prio_key, job.id) == b"5"

    # Simulate the job hash having expired, leaving a phantom id behind in the sub-queue.
    redis.delete(job.redis_key)
    removed = clean_affinity_sub_queues(redis, random_queue_name, priority=True)
    assert removed == 1
    # The phantom id (and its now-empty list and prio hash) are gone.
    assert not redis.exists(sub_key)
    assert not redis.exists(sub_prio_key)


def test_clean_affinity_sub_queues_removes_orphaned_prio_hash(
    redis: RedisClient, random_queue_name: str
) -> None:
    enqueue_priority(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs={"a": 1, "b": 2},
        priority=5,
        affinity=["x"],
    )
    sub_key = QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x")
    sub_prio_key = f"{sub_key}prio"
    # Drain the list directly (as a BLPOP would), orphaning the prio lookup hash.
    redis.delete(sub_key)
    assert redis.exists(sub_prio_key)

    removed = clean_affinity_sub_queues(redis, random_queue_name, priority=True)
    assert removed == 1
    assert not redis.exists(sub_prio_key)


def test_priority_sub_queue_prio_hash_does_not_leak_while_flowing(
    redis: RedisClient, random_queue_name: str
) -> None:
    # Regression: acquiring a priority affine job must drain its prio-hash entry from the
    # affinity sub-queue, so a busy specifier's `…prio` hash stays bounded.
    worker = TestWorker.for_queue_names(redis, random_queue_name)
    worker.warm_specifiers["x"] = time.time()
    sub_prio_key = (
        QUEUE_KEY_PREFIX + get_affinity_queue_name(random_queue_name, "x") + "prio"
    )

    for _ in range(5):
        enqueue_priority(
            redis,
            random_queue_name,
            "minique_tests.jobs.sum_positive_values",
            kwargs={"a": 1, "b": 2},
            priority=5,
            affinity=["x"],
        )
        assert worker.tick() is not None

    # Every acquired job removed its own entry; nothing accumulated.
    assert redis.hlen(sub_prio_key) == 0
