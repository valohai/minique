import json
import logging
import sys
import time
import traceback

from redis import StrictRedis

from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, AlreadyResulted
from minique.models.job import Job
from minique.utils import _set_current_job, import_by_string


class JobRunner:
    def __init__(self, worker, job):
        assert isinstance(job, Job)
        self.worker = worker
        self.job = job
        self.redis = job.redis
        assert isinstance(self.redis, StrictRedis)
        self.log = logging.getLogger('{}.{}'.format(__name__, self.job.id.replace('.', '_')))
        job.ensure_exists()

    def acquire(self):
        new_acquisition_info = json.dumps({'worker': self.worker.id, 'time': time.time()})
        if not self.redis.hsetnx(self.job.redis_key, 'acquired', new_acquisition_info):
            raise AlreadyAcquired('job {id} already acquired: {info}'.format(
                id=self.job.id,
                info=self.job.acquisition_info,
            ))
        self.redis.hset(self.job.redis_key, 'status', JobStatus.ACQUIRED.value)
        self.redis.persist(self.job.redis_key)

    def execute(self):
        func = self.get_callable()
        kwargs = self.job.kwargs
        self.log.info('calling %s(%r)', func, kwargs)
        with _set_current_job(job=self.job):
            return func(**kwargs)

    def get_callable(self):
        return import_by_string(self.job.callable_name)

    def complete(self, success, value, duration):
        assert isinstance(success, bool)
        update_payload = {
            'status': (JobStatus.SUCCESS if success else JobStatus.FAILED).value,
            'duration': float(duration),
        }
        if not self.redis.setnx(self.job.result_redis_key, value):  # pragma: no cover
            raise AlreadyResulted('job {id} already has result'.format(id=self.job.id))
        self.redis.hmset(self.job.redis_key, update_payload)
        # Update expiries to the result TTL for both the job and the result
        self.redis.expire(self.job.result_redis_key, self.job.result_ttl)
        self.redis.expire(self.job.redis_key, self.job.result_ttl)
        if success:
            self.log.info('finished in %f seconds', duration)
        else:
            self.log.warning('errored in %f seconds', duration)

    def run(self):
        self.acquire()
        interrupt = False
        success = False
        value = {'error': 'unknown'}
        start_time = time.time()
        try:
            value = self.execute()
            value = json.dumps(value)
            success = True
        except BaseException as exc:
            success = False
            exc_type, exc_value, exc_tb = excinfo = sys.exc_info()
            value = json.dumps({
                'exception_type': exc_type.__qualname__,
                'exception_value': str(exc_value),
                'traceback': traceback.format_exc(),
            }, default=str)
            interrupt = isinstance(exc, KeyboardInterrupt)
            try:
                self.process_exception(excinfo)
            except:  # noqa
                self.log.warning('error running process_exception()', exc_info=True)
        finally:
            end_time = time.time()
            self.complete(success=success, value=value, duration=(end_time - start_time))
        if interrupt:  # pragma: no cover
            raise KeyboardInterrupt('Interrupt')

    def process_exception(self, excinfo):  # pragma: no cover
        """
        A hook for subclasses to log (e.g. Sentry) or otherwise process exceptions.

        :param excinfo: The sys.exc_info() 3-tuple
        """
        pass
