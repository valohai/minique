import logging
import os

import pytest
from redis import Redis

from minique.utils import get_random_pronounceable_string


def pytest_configure() -> None:
    logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)


@pytest.fixture()
def redis() -> Redis:
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:  # pragma: no cover
        pytest.skip('no REDIS_URL (required for redis fixture)')
    return Redis.from_url(redis_url)


@pytest.fixture()
def random_queue_name() -> str:
    return 'test_queue_%s' % get_random_pronounceable_string()
