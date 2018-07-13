from minique.api import enqueue
from minique.work.job_runner import JobRunner
from minique.work.worker import Worker


class HonkJobRunner(JobRunner):
    def acquire(self):
        print('Hooooooooonk.')


class HonkWorker(Worker):
    job_runner_class = HonkJobRunner


def test_job_runner_override(redis, random_queue_name, capsys):
    job = enqueue(redis, random_queue_name, 'minique_tests.jobs.sum_positive_values', {'a': 10, 'b': 15})
    assert not job.has_finished
    worker = HonkWorker.for_queue_names(redis, [random_queue_name])
    r_job = worker.tick()
    assert job == r_job  # we executed that particular job, right?
    assert job.has_finished
    assert 'Hooooooooonk.' in capsys.readouterr()[0]
