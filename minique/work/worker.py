from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from os import getpid
from platform import node
from typing import TYPE_CHECKING, Any

from minique.compat import sentry_sdk
from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, NoSuchJob
from minique.models.job import Job
from minique.models.queue import Queue
from minique.utils.affinity import get_affinity_sub_queue_key
from minique.work.job_runner import JobRunner

if TYPE_CHECKING:
    from minique.types import ContextDict, ExcInfo, RedisClient


class Worker:
    queue_timeout = 1
    job_runner_class = JobRunner

    # This property may be ignored by subclasses, but it's here for convenience's sake.
    allowed_callable_patterns: Iterable[str] = frozenset()

    # Best-effort affinity tuning (subclass-overridable):
    affinity_window = 1800  # seconds a specifier stays "warm" after last use
    affinity_max_specifiers = 50  # cap on simultaneously-warm specifiers

    def __init__(
        self,
        redis: RedisClient,
        queues: list[Queue],
    ) -> None:
        self.id = self.compute_id()
        self.redis = redis
        self.queues = list(queues)
        self.log = logging.getLogger(f"{__name__}.{self.id.replace('.', '_')}")
        assert all(isinstance(q, Queue) for q in queues)
        self.warm_specifiers: dict[str, float] = {}

    @classmethod
    def for_queue_names(
        cls,
        redis: RedisClient,
        queue_names: list[str] | str,
        **kwargs: Any,
    ) -> Worker:
        if isinstance(queue_names, str):
            queue_names = [queue_names]
        kwargs.setdefault("queues", [Queue(redis, name) for name in queue_names])
        return cls(redis, **kwargs)

    def compute_id(self) -> str:
        # Override me in a subclass if you like!
        node_base = node().split(".")[0]
        return f"w-{node_base}-{getpid()}"

    def get_current_warm_specifiers(self) -> list[str]:
        """Return currently-warm specifiers, most-recently-used first.

        As a side-effect, prunes old entries.
        """
        cutoff = time.time() - self.affinity_window
        self.warm_specifiers = {
            s: t for s, t in self.warm_specifiers.items() if t >= cutoff
        }
        kept = sorted(
            self.warm_specifiers,
            key=self.warm_specifiers.__getitem__,
            reverse=True,
        )[: self.affinity_max_specifiers]
        self.warm_specifiers = {s: self.warm_specifiers[s] for s in kept}
        return kept

    def get_blpop_keys(self) -> dict[str, str | None]:
        """Build the BLPOP key set as an ordered `redis_key -> affinity specifier` map,
        grouped per queue: for each queue (in its existing priority order) its warm
        affinity sub-queues, then its base queue.

        The mapping value is the specifier whose sub-queue the key is, or None for a base
        queue, so a popped key can be classified without reversing its (one-way hashed)
        name. The dict is insertion-ordered, which BLPOP reads as priority order.
        """
        # Grouping per queue preserves queue-order-as-priority — all of a higher-priority
        # queue's work (affinity sub-queues included) is preferred over a lower-priority
        # queue's. Affinity only re-orders *within* a queue. With an empty warm set this is
        # byte-for-byte the plain base-queue key list, so a cold worker is unchanged.
        keys: dict[str, str | None] = {}  # de-dupes, order-preserving
        warm = self.get_current_warm_specifiers()  # Has side-effects, so run once only
        for q in self.queues:
            for s in warm:
                keys.setdefault(get_affinity_sub_queue_key(q.name, s), s)
            keys.setdefault(q.redis_key, None)
        return keys

    def get_next_job(self) -> Job | None:
        key_map = self.get_blpop_keys()
        rv = self.redis.blpop(list(key_map), self.queue_timeout)
        if not rv:  # Timed out with nothing to pop. Carry on.
            return None
        raw_source_key, raw_job_id = rv
        source_key = raw_source_key.decode()
        job = Job(self.redis, raw_job_id.decode())
        job.source_queue_key = source_key
        job.affinity_specifier = key_map.get(source_key)
        return job

    def tick(self) -> Job | None:
        job = self.get_next_job()
        if not job:
            return None

        with self.redis.pipeline(transaction=False) as p:
            # Grab the data we need in a single pipeline.
            p.hget(job.redis_key, "status")
            p.hexists(job.redis_key, "acquired")
            p.hget(job.redis_key, "affinity")
            status_raw, acquired, affinity_raw = p.execute()
        job._cache_affinity(affinity_raw)

        if status_raw is None:
            self.log.warning("skipping phantom of vanished job %s", job.id)
            return None

        if JobStatus(status_raw.decode()) == JobStatus.CANCELLED:
            return None

        if acquired:
            self.log.debug("skipping phantom of already-run job %s", job.id)
            return None

        try:
            runner = self.job_runner_class(worker=self, job=job)
            runner.run()
        except NoSuchJob:  # Job hash expired? Phantom.
            self.log.debug("skipping phantom of vanished job %s", job.id)
            return None
        except AlreadyAcquired:  # Same-instant pop, so phantom for us.
            self.log.debug("skipping phantom of already-acquired job %s", job.id)
            return None

        # Done, we're now warm for this.
        now = time.time()
        for specifier in job.affinity:
            self.warm_specifiers[specifier] = now

        return job

    def loop(self) -> None:  # pragma: no cover
        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                break
            except Exception:
                self.log.error("Unexpected worker tick error", exc_info=True)

    def process_exception(
        self,
        excinfo: ExcInfo | None = None,
        context: ContextDict | None = None,
    ) -> None:  # pragma: no cover
        """
        A hook to log or process exceptions.

        If sentry_sdk is installed, this implementation will automatically be use it.

        :param excinfo: Optionally, the sys.exc_info() 3-tuple
        """
        if sentry_sdk:
            with sentry_sdk.new_scope() as scope:
                if context:
                    scope.set_context("minique", context)
                sentry_sdk.capture_exception(excinfo)
