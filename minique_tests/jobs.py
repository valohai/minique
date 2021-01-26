import datetime
import queue
import time

from minique.utils import get_current_job

# This is only needed for synchronization of the "job_with_a_message" test...
message_test_inbox = queue.Queue(maxsize=1)
message_test_outbox = queue.Queue(maxsize=1)


def sum_positive_values(a, b):
    if not (a > 0 and b > 0):
        raise ValueError("values not positive enough")
    return a + b


def wrap_kwargs(**kwargs):
    return dict(kwargs.copy())


def job_with_unjsonable_retval() -> datetime.datetime:
    return datetime.datetime.now()


def reverse_job_id() -> str:
    # tests that one can access the current job object
    return get_current_job().id[::-1]


def job_with_a_message() -> str:
    def _wait_for_message_passing():
        nonce = time.time()
        message_test_outbox.put(nonce)
        assert message_test_inbox.get() == nonce

    job = get_current_job()
    job.set_meta("oh, hello")
    _wait_for_message_passing()
    job.set_meta({"message": "progress occurs", "status": [1, 2, 3]})
    _wait_for_message_passing()
    return 42
