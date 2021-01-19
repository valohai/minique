import fnmatch
import json
import logging
import sys
import time
import traceback
from typing import Callable

from redis import Redis

from minique.compat import TYPE_CHECKING
from minique.enums import JobStatus
from minique.excs import AlreadyAcquired, AlreadyResulted, InvalidJob
from minique.models.job import Job
from minique.utils import _set_current_job, import_by_string

if TYPE_CHECKING:
    from minique.work.worker import Worker


class JobRunner:
    def __init__(self, worker: "Worker", job: Job) -> None:
        self.worker = worker
        self.job = job
        self.redis = job.redis
        assert isinstance(self.redis, Redis)
        self.log = logging.getLogger(
            "{}.{}".format(__name__, str(self.job.id).replace(".", "_"))
        )
        job.ensure_exists()

    def acquire(self) -> None:
        new_acquisition_info = json.dumps(self.get_acquisition_info(), default=str)
        if not self.redis.hsetnx(self.job.redis_key, "acquired", new_acquisition_info):
            raise AlreadyAcquired(
                "job {id} already acquired: {info}".format(
                    id=self.job.id,
                    info=self.job.acquisition_info,
                )
            )
        self.redis.hset(self.job.redis_key, "status", JobStatus.ACQUIRED.value)
        self.redis.persist(self.job.redis_key)

    def get_acquisition_info(self) -> dict:
        # Override me in a subclass if you like!
        return {"worker": self.worker.id, "time": time.time()}

    def execute(self):
        func = self.get_callable()
        kwargs = self.job.kwargs
        self.log.info("calling %s(%r)", func, kwargs)
        with _set_current_job(job=self.job):
            return func(**kwargs)

    def verify_callable_name(self, name: str) -> bool:
        if not any(
            fnmatch.fnmatch(name, pat) for pat in self.worker.allowed_callable_patterns
        ):
            raise InvalidJob(
                "Name {} doesn't match any pattern in {}".format(
                    name,
                    self.worker.allowed_callable_patterns,
                )
            )
        return True

    def get_callable(self) -> Callable:
        name = self.job.callable_name
        if not self.verify_callable_name(name):
            raise InvalidJob("Invalid job definition")
        return import_by_string(name)

    def complete(self, success: bool, value: str, duration: float) -> None:
        assert isinstance(success, bool)
        update_payload = {
            "status": (JobStatus.SUCCESS if success else JobStatus.FAILED).value,
            "duration": float(duration),
        }
        if not self.redis.setnx(self.job.result_redis_key, value):  # pragma: no cover
            raise AlreadyResulted("job {id} already has result".format(id=self.job.id))
        self.redis.hmset(self.job.redis_key, update_payload)
        # Update expiries to the result TTL for both the job and the result
        self.redis.expire(self.job.result_redis_key, self.job.result_ttl)
        self.redis.expire(self.job.redis_key, self.job.result_ttl)
        if success:
            self.log.info("finished in %f seconds", duration)
        else:
            self.log.warning("errored in %f seconds", duration)

    def run(self) -> None:
        try:
            encoding = self.job.get_encoding()
            self.acquire()
        except Exception as exc:
            self.process_exception(sys.exc_info())
            raise exc  # could have had an exception in process_exception
        interrupt = False
        success = False
        value = {"error": "unknown"}
        start_time = time.time()
        try:
            value = self.execute()
            value = encoding.encode(value)
            success = True
        except BaseException as exc:
            success = False
            exc_type, exc_value, exc_tb = excinfo = sys.exc_info()
            value = encoding.encode(
                {
                    "exception_type": exc_type.__qualname__,
                    "exception_value": str(exc_value),
                    "traceback": traceback.format_exc(),
                },
                failsafe=True,
            )
            interrupt = isinstance(exc, KeyboardInterrupt)
            self.process_exception(excinfo)
        finally:
            end_time = time.time()
            self.complete(
                success=success, value=value, duration=(end_time - start_time)
            )
        if interrupt:  # pragma: no cover
            raise KeyboardInterrupt("Interrupt")

    def process_exception(self, excinfo):
        try:
            self.worker.process_exception(
                excinfo,
                context={
                    "id": str(self.job.id),
                    "queue": str(self.job.queue_name),
                },
            )
        except Exception:  # noqa
            self.log.warning("error running process_exception()", exc_info=True)
