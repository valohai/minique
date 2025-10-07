from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from minique.consts import QUEUE_KEY_PREFIX
from minique.excs import NoSuchJob
from minique.utils.redis_list import read_list

if TYPE_CHECKING:
    from minique.models.job import Job
    from minique.types import RedisClient


class Queue:
    def __init__(self, redis: RedisClient, name: str) -> None:
        self.redis = redis
        self.name = str(name)

    @cached_property
    def redis_key(self) -> str:
        return f"{QUEUE_KEY_PREFIX}{self.name}"

    @property
    def length(self) -> int:
        return self.redis.llen(self.redis_key)

    def clear(self) -> Any:
        """
        Entirely clear this queue. Do not call this unless you're willing to risk orphaned jobs.
        """
        return self.redis.delete(self.redis_key)

    def get_queue_index(self, job: Job) -> int | None:
        # TODO: use `LPOS` (https://redis.io/commands/lpos/) when available for this
        job_id_bytes = str(job.id).encode()
        for index, value in enumerate(read_list(self.redis, self.redis_key)):
            if value == job_id_bytes:
                return index
        return None

    def ensure_enqueued(self, job: Job) -> tuple[bool, int]:
        """
        Ensure the job is in the queue.

        This should only be called if the job itself is in a state that would allow its execution,
        and even then only in dire straits, as it is a fixer.

        :return: A 2-tuple of a boolean and an integer describing the queue position;
                 the boolean is true if the job was indeed re-queued
        """
        if not self.redis.exists(job.redis_key):
            raise NoSuchJob(f"Job object for {id} has disappeared, can not re-enqueue")

        # NB: This is mildly racy; two processes could be doing this scan concurrently.
        #     Technically having an entry in the queue multiple times shouldn't be a problem, since
        #     the second attempt at acquiring the same job will fail anyway (HSETNX in acquire()),
        #     but some heuristics regarding queue length could be off.

        index = self.get_queue_index(job)
        if index is None:
            index = self.add_job(job)
            return True, index

        return False, index

    def add_job(self, job: Job) -> int:
        """Add a job to the queue in the appropriate position for its priority.

        :param job: The Job object to be added.

        :return: index position of the job in the queue
        """
        return self.redis.rpush(self.redis_key, job.id) - 1

    def dequeue_job(self, job_id: str) -> bool:
        """Dequeue the job with the given job ID"""
        num_removed = self.redis.lrem(self.redis_key, 0, job_id)
        return num_removed > 0
