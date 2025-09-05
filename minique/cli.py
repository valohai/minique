from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from minique.compat import sentry_sdk
from minique.work.worker import Worker

if TYPE_CHECKING:
    from minique.types import RedisClient


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--redis-url", required=True)
    parser.add_argument("-q", "--queues", nargs="+", required=True)
    parser.add_argument("--allow-callable", nargs="+", required=True)
    parser.add_argument("--single-tick", action="store_true")
    return parser


def get_redis(url: str) -> RedisClient:
    try:
        from valkey import Valkey

        return Valkey.from_url(url)  # type: ignore[return-value]
    except ImportError:
        from redis import Redis

        return Redis.from_url(url)


def main(argv: list[str] | None = None) -> None:
    parser = get_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)
    redis = get_redis(args.redis_url)
    worker = Worker.for_queue_names(redis=redis, queue_names=args.queues)
    worker.allowed_callable_patterns = set(args.allow_callable)
    worker.log.info("Worker initialized")
    if sentry_sdk:
        try:
            sentry_sdk.init()
        except Exception as exc:
            worker.log.warning("Failed to initialize Sentry: %s", exc, exc_info=True)
        else:
            if (client := sentry_sdk.get_client()) and client.is_active():
                worker.log.info("Sentry configured with a valid DSN")

    if args.single_tick:
        worker.tick()
    else:
        worker.loop()
