from minique.api import enqueue
from minique.cli import main


def test_cli(redis, redis_url, random_queue_name):
    kwargs = {"a": 10, "b": 15}
    job = enqueue(
        redis,
        random_queue_name,
        "minique_tests.jobs.sum_positive_values",
        kwargs,
    )
    main(
        [
            "-u",
            redis_url,
            "-q",
            random_queue_name,
            "--allow-callable",
            "minique_tests.*",
            "--single-tick",
        ],
    )
    assert job.result == 25
