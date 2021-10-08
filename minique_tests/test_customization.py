import pytest
from redis import Redis

from minique.api import enqueue
from minique.encoding import register_encoding, JSONEncoding
from minique.testing import run_synchronously
from minique.work.job_runner import JobRunner
from minique_tests.worker import TestWorker


def special_dump(o):
    if isinstance(o, set):
        return {"$set$": list(o)}
    raise TypeError(f"Unable to encode: {o}")


def special_load(o):
    if len(o) == 1 and set(o) == {"$set$"}:
        return set(next(iter(o.values())))
    return o


@register_encoding("special_json")
class SpecialJSONEncoding(JSONEncoding):
    load_kwargs = dict(JSONEncoding.load_kwargs, object_hook=special_load)
    dump_kwargs = dict(JSONEncoding.dump_kwargs, default=special_dump)


class HonkJobRunner(JobRunner):
    def acquire(self) -> None:
        print("Hooooooooonk.")

    def process_exception(self, excinfo):
        print(f"Alarmed honk! {excinfo}")


class HonkWorker(TestWorker):
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


def test_custom_encoding(redis: Redis, random_queue_name: str):
    job = enqueue(
        redis,
        random_queue_name,
        callable="minique_tests.jobs.wrap_kwargs",
        kwargs={
            "foo": {1, 4, 8},
        },
        encoding_name="special_json",
    )
    run_synchronously(job)
    assert job.encoding_name == "special_json"
    assert job.result == {"foo": {8, 1, 4}}
    assert b"$set$" in job.encoded_result
