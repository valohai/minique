import json
import random
from contextlib import contextmanager
from importlib import import_module
from threading import local
from typing import Any, Callable, Optional

from minique._compat import TYPE_CHECKING

if TYPE_CHECKING:
    from minique.models.job import Job

_current_jobs = local()


def import_by_string(callable: str) -> Callable:
    module, _, func = callable.rpartition('.')
    module = import_module(module)
    func = getattr(module, func)
    return func


@contextmanager
def _set_current_job(job: 'Job'):
    assert not get_current_job()
    setattr(_current_jobs, 'current_job', job)
    try:
        yield
    finally:
        setattr(_current_jobs, 'current_job', None)


def get_current_job() -> Optional['Job']:
    return getattr(_current_jobs, 'current_job', None)


class cached_property(object):
    """A property that is only computed once per instance and then replaces
       itself with an ordinary attribute. Deleting the attribute resets the
       property.

       Source: https://github.com/bottlepy/bottle/blob/0.11.5/bottle.py#L175
    """

    def __init__(self, func: Callable) -> None:
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj: Any, cls: Any) -> str:
        if obj is None:
            # We're being accessed from the class itself, not from an object
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


consonants = 'cdgkklmmnnprst'
vowels = 'aeiou'


def get_random_pronounceable_string(length: int = 12) -> str:
    s = []
    while length > 0 and len(s) < length:
        s.append(random.choice(consonants))
        s.append(random.choice(vowels) * random.choice((1, 1, 2)))
    return ''.join(s)[:length]


def get_json_or_none(value: bytes) -> Any:
    """
    If the value is truthy, decode it as JSON. Otherwise return None.
    """
    if value:
        if isinstance(value, bytes):
            value = value.decode()
        return json.loads(str(value))
    return None
