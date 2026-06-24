from __future__ import annotations

import hashlib
from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from minique.consts import AFFINITY_QUEUE_INFIX, PRIO_KEY_SUFFIX, QUEUE_KEY_PREFIX
from minique.utils.cleaning import _scan_decoded

if TYPE_CHECKING:
    from minique.types import RedisClient


def normalize_affinity(affinity: list[Any] | None) -> list[str]:
    """De-duplicate (order-preserving) and validate affinity specifiers."""
    if not affinity:
        return []
    if any(not isinstance(s, str) for s in affinity):
        raise TypeError("affinity specifiers must be strings")
    specifiers = [s for s in affinity if s]
    return list(dict.fromkeys(specifiers))


def get_affinity_sub_queue_names(redis: RedisClient, base: str) -> Iterable[str]:
    """Return the (base-relative) names of all existing affinity sub-queues for `base`."""
    match = f"{QUEUE_KEY_PREFIX}{affinity_queue_name_glob(base)}"
    prefix_len = len(QUEUE_KEY_PREFIX)
    for key in _scan_decoded(redis, match):
        # The same prefix also matches each PriorityQueue's `…prio` lookup hash; skip it.
        if key.endswith(PRIO_KEY_SUFFIX):
            continue
        yield key[prefix_len:]


@lru_cache
def get_affinity_queue_name(base: str, specifier: str) -> str:
    """Derive the name of `base` queue's affinity sub-queue for `specifier`."""
    # Truncated SHA-1 hash to keep collisions at bay, instead of having to escape specifiers.
    digest = hashlib.sha1(specifier.encode("utf-8")).hexdigest()[:16]
    return f"{base}{AFFINITY_QUEUE_INFIX}{digest}"


@lru_cache
def get_affinity_sub_queue_key(base: str, specifier: str) -> str:
    """Full Redis key (prefix included) of the affinity sub-queue for `specifier`."""
    return f"{QUEUE_KEY_PREFIX}{get_affinity_queue_name(base, specifier)}"


@lru_cache
def glob_escape(value: str) -> str:
    """Escape Redis glob-style `MATCH` metacharacters so `value` is matched literally."""
    escaped = []
    for char in value:
        if char in "\\*?[]":
            escaped.append("\\")
        escaped.append(char)
    return "".join(escaped)


def affinity_queue_name_glob(base: str) -> str:
    """`MATCH` glob (base-relative) for every affinity sub-queue name of `base`."""
    return f"{glob_escape(base)}{AFFINITY_QUEUE_INFIX}*"
