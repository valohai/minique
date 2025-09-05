from __future__ import annotations

import logging
import os
from unittest.mock import Mock

import pytest
from redis import Redis

from minique.compat import sentry_sdk
from minique.utils import get_random_pronounceable_string


def pytest_configure() -> None:
    logging.basicConfig(datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)


@pytest.fixture(scope="session")
def redis_url() -> str:
    url = os.environ.get("REDIS_URL")
    if not url:  # pragma: no cover
        pytest.skip("no REDIS_URL (required for redis fixture)")
    return url


@pytest.fixture()
def redis(redis_url: str) -> Redis:
    return Redis.from_url(redis_url)


@pytest.fixture()
def random_queue_name() -> str:
    return f"test_queue_{get_random_pronounceable_string()}"


@pytest.fixture
def sentry_event_calls(monkeypatch) -> list | None:
    if not sentry_sdk:
        return None
    client = sentry_sdk.Client(dsn="http://a:a@example.com/123")
    client.capture_event = Mock()
    # TODO: this doesn't clean up the global scope – maybe we don't need to?
    sentry_sdk.get_global_scope().set_client(client)
    return client.capture_event.call_args_list
