from typing import TYPE_CHECKING, Any, Optional, Tuple

from redis import Redis

from minique.consts import QUEUE_KEY_PREFIX
from minique.excs import NoSuchJob
from minique.utils import cached_property
from minique.utils.redis_list import read_list

if TYPE_CHECKING:
    from minique.models.job import Job


class Queue:
    def __init__(self, redis: "Redis[bytes]", name: str) -> None:
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

    def enqueue_initial(self, job: "Job", payload: dict) -> None:  # type: ignore[type-arg]
        assert payload["queue"] == self.name
        with self.redis.pipeline() as p:
            p.hmset(job.redis_key, payload)
            if payload["job_ttl"] > 0:
                p.expire(job.redis_key, payload["job_ttl"])
            p.rpush(self.redis_key, job.id)
            p.execute()

    def get_queue_index(self, job: "Job") -> Optional[int]:
        # TODO: use `LPOS` (https://redis.io/commands/lpos/) when available for this
        job_id_bytes = str(job.id).encode()
        for index, value in enumerate(read_list(self.redis, self.redis_key)):
            if value == job_id_bytes:
                return index
        return None

    def ensure_enqueued(self, job: "Job") -> Tuple[bool, int]:
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
            # RPUSH: Integer reply: the length of the list after the push operation.
            index = self.redis.rpush(self.redis_key, job.id) - 1
            return (True, index)

        return (False, index)
