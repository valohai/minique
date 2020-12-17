import pytest
from redis import Redis

from minique.api import enqueue
from minique.work.job_runner import JobRunner
from minique.work.worker import Worker


class HonkJobRunner(JobRunner):
    def acquire(self) -> None:
        print("Hooooooooonk.")

    def process_exception(self, excinfo):
        print("Alarmed honk! {}".format(excinfo))


class HonkWorker(Worker):
    job_runner_class = HonkJobRunner


@pytest.mark.parametrize("problem", (False, True))
def test_job_runner_override(
    redis: Redis, random_queue_name: str, capsys, problem: bool
):
    args = {"a": "err", "b": -8} if problem else {"a": 10, "b": 15}
    job = enqueue(
        redis, random_queue_name, "minique_tests.jobs.sum_positive_values", args
    )
    assert not job.has_finished
    worker = HonkWorker.for_queue_names(redis, [random_queue_name])
    r_job = worker.tick()
    assert job == r_job  # we executed that particular job, right?
    assert job.has_finished
    output = capsys.readouterr()[0]
    assert "Hooooooooonk." in output
    assert ("Alarmed honk!" in output) == problem
