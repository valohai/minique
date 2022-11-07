import random
from contextlib import contextmanager
from importlib import import_module
from threading import local
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional

if TYPE_CHECKING:
    from minique.models.job import Job

_current_jobs = local()


def import_by_string(spec: str) -> Any:
    module_name, _, func_name = spec.rpartition(".")
    module = import_module(module_name)
    func = getattr(module, func_name)
    return func


@contextmanager
def _set_current_job(job: "Job") -> Iterator[None]:
    assert not get_current_job()
    _current_jobs.current_job = job
    try:
        yield
    finally:
        _current_jobs.current_job = None


def get_current_job() -> Optional["Job"]:
    return getattr(_current_jobs, "current_job", None)


class cached_property:  # TODO: remove when py3.8+ only  # noqa: N801
    """A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property.

    Source: https://github.com/bottlepy/bottle/blob/0.11.5/bottle.py#L175
    """

    def __init__(self, func: Callable[..., Any]) -> None:
        self.__doc__ = getattr(func, "__doc__", "")
        self.func = func

    def __get__(self, obj: Any, cls: Any) -> Any:
        if obj is None:
            # We're being accessed from the class itself, not from an object
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


consonants = "cdgkklmmnnprst"
vowels = "aeiou"


def get_random_pronounceable_string(length: int = 12) -> str:
    s = []  # type: List[str]
    while length > 0 and len(s) < length:
        s.append(random.choice(consonants))
        s.append(random.choice(vowels) * random.choice((1, 1, 2)))
    return "".join(s)[:length]
