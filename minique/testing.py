from typing import Any, Callable, Dict, Optional

from minique.models.job import Job
from minique.utils import _set_current_job, import_by_string
from minique.work.job_runner import JobRunner
from minique.work.worker import Worker


class TestJobRunner(JobRunner):
    def execute(self) -> Any:
        func = self.job.replacement_callable or import_by_string(self.job.callable_name)
        if self.job.replacement_kwargs is not None:
            kwargs = self.job.replacement_kwargs
        else:
            kwargs = self.job.kwargs
        with _set_current_job(job=self.job):
            return func(**kwargs)


def run_synchronously(
    job: Job,
    replacement_callable: Optional[Callable[..., Any]] = None,
    replacement_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    job.replacement_callable = replacement_callable
    job.replacement_kwargs = replacement_kwargs
    worker = Worker(job.redis, [])
    TestJobRunner(worker=worker, job=job).run()
