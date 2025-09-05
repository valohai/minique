import random
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from threading import local
from typing import TYPE_CHECKING, Any, Optional

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
    if get_current_job():
        raise RuntimeError("Current job is already set")
    _current_jobs.current_job = job
    try:
        yield
    finally:
        _current_jobs.current_job = None


def get_current_job() -> Optional["Job"]:
    return getattr(_current_jobs, "current_job", None)


consonants = "cdgkklmmnnprst"
vowels = "aeiou"


def get_random_pronounceable_string(length: int = 12) -> str:
    s: list[str] = []
    while length > 0 and len(s) < length:
        s.append(random.choice(consonants))
        s.append(random.choice(vowels) * random.choice((1, 1, 2)))
    return "".join(s)[:length]
