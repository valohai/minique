from __future__ import annotations

import threading
from contextlib import closing
from typing import TYPE_CHECKING

from minique.api import enqueue
from minique.enums import JobStatus
from minique.models.job import Job
from minique.work.worker import Worker
from minique_tests.jobs import reverse_job_id

if TYPE_CHECKING:
    from minique.types import RedisClient

"""
Here is a small example how to use the Heartbeat feature.
Heartbeat is saved in Job's Redis Hash and updated via Job.refresh_heartbeat.

TestHeartbeatWorker starts a pacemaker thread that updates the heartbeat every minute
"""


class TestHeartbeatWorker(Worker):
    """
    Example of one way to implement the heartbeat with threads
    """

    __test__ = False
    heartbeat_interval = 60

    def tick(self) -> Job | None:
        job = self.get_next_job()
        if not job:
            return None
        job.ensure_exists()
        if job.status == JobStatus.CANCELLED:  # Simply skip running cancelled jobs
            return None
        runner = self.job_runner_class(worker=self, job=job)
        heart = JobPacemakerThread(
            job=job, interval=self.heartbeat_interval
        )  # post a heartbeat once a minute
        heart.start()
        with closing(heart):
            runner.run()
        heart.join()
        return job


class JobPacemakerThread(threading.Thread):
    """
    Simple thread to update the heartbeat while runner is running a job
    """

    __test__ = False

    def __init__(self, *, job: Job, interval: float):
        self.job = job
        super().__init__(name=f"JobHeartbeat-{job.id}")
        self._stop_signal = threading.Event()
        self._interval = interval

    def run(self):
        while not self._stop_signal.is_set():
            self._tick()
            self._stop_signal.wait(self._interval)

    def _tick(self):
        self.job.refresh_heartbeat()

    def close(self):
        self._stop_signal.set()


def test_heartbeat_worker(redis: RedisClient, random_queue_name: str) -> None:
    job = enqueue(redis, random_queue_name, reverse_job_id)
    assert job.heartbeat is None
    worker = TestHeartbeatWorker.for_queue_names(redis, [random_queue_name])
    worker.tick()
    assert isinstance(job.heartbeat, float)
