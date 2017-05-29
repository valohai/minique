import datetime

from minique.utils import get_current_job


def sum_positive_values(a, b):
    if not (a > 0 and b > 0):
        raise ValueError('values not positive enough')
    return a + b


def job_with_unjsonable_retval():
    return datetime.datetime.now()


def reverse_job_id():
    # tests that one can access the current job object
    return get_current_job().id[::-1]
