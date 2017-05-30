import logging
from os import getpid
from platform import node

from minique.enums import JobStatus
from minique.models.job import Job
from minique.models.queue import Queue
from minique.work.job_runner import JobRunner


class Worker:
    queue_timeout = 1

    def __init__(self, redis, queues):
        self.id = self.compute_id()
        self.redis = redis
        self.queues = queues
        self.log = logging.getLogger('{}.{}'.format(__name__, self.id.replace('.', '_')))
        assert all(isinstance(q, Queue) for q in queues)

    @classmethod
    def for_queue_names(cls, redis, queue_names, **kwargs):
        if isinstance(queue_names, str):
            queue_names = [queue_names]
        kwargs.setdefault('queues', [Queue(redis, name) for name in queue_names])
        return cls(redis, **kwargs)

    def compute_id(self):
        # Override me in a subclass if you like!
        return 'w-{node}-{pid}'.format(node=node().split('.')[0], pid=getpid())

    def get_next_job(self):
        rv = self.redis.blpop([q.redis_key for q in self.queues], self.queue_timeout)
        if rv:  # The rv is a 2-tuple (queue name, value)
            job_id = rv[1].decode()
            return Job(self.redis, job_id)

    def tick(self):
        job = self.get_next_job()
        if not job:
            return None
        job.ensure_exists()
        if job.status == JobStatus.CANCELLED:  # Simply skip running cancelled jobs
            return None
        runner = JobRunner(worker=self, job=job)
        runner.run()
        return job

    def loop(self):  # pragma: no cover
        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                break
            except:
                self.log.error('Unexpected worker tick error', exc_info=True)
