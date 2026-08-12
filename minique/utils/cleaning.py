from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from minique.consts import (
    JOB_KEY_PREFIX,
    PRIO_KEY_SUFFIX,
    QUEUE_KEY_PREFIX,
)

if TYPE_CHECKING:
    from minique.types import RedisClient


def _scan_decoded(redis: RedisClient, match: str) -> Iterator[str]:
    """Yield `SCAN` matches as `str`, decoding the bytes a `redis[bytes]` client returns."""
    for raw_key in redis.scan_iter(match=match):
        yield raw_key.decode() if isinstance(raw_key, bytes) else raw_key


def trim_phantom_ids(redis: RedisClient, list_key: str, *, priority: bool) -> int:
    """Remove ids whose job hash no longer exists from one affinity sub-queue list."""
    ids = [raw_id.decode() for raw_id in redis.lrange(list_key, 0, -1)]
    if not ids:
        return 0
    # Check existence of all ids in one round-trip, then trim the phantoms.
    with redis.pipeline(transaction=False) as p:
        for job_id in ids:
            p.exists(f"{JOB_KEY_PREFIX}{job_id}")
        exists_flags = p.execute()
    phantoms = [
        jid for jid, exists in zip(ids, exists_flags, strict=True) if not exists
    ]
    if not phantoms:
        return 0
    prio_key = f"{list_key}{PRIO_KEY_SUFFIX}"
    with redis.pipeline(transaction=False) as p:
        for job_id in phantoms:
            if priority:  # mirror PriorityQueue.dequeue_job: HDEL then LREM
                p.hdel(prio_key, job_id)
            p.lrem(list_key, 0, job_id)
        p.execute()
    return len(phantoms)


ORPHANED_PRIO_CLEANER_SCRIPT = """
local deleted = 0
for i = 1, #KEYS do
    if redis.call("LLEN", ARGV[i]) == 0 then
        deleted = deleted + redis.call("DEL", KEYS[i])
    end
end
return deleted
"""


def remove_orphaned_prio_hashes(redis: RedisClient, base: str) -> int:
    """Delete `…prio` hashes left orphaned after their affinity sub-queue list drained."""
    from minique.utils.affinity import affinity_queue_name_glob

    # Per key pair, delete the `…prio` hash (KEYS[i]) only if its sub-queue list (ARGV[i])
    # is empty, atomically. A plain LLEN-then-DEL split across round-trips would race a
    # producer's ADD_JOB_SCRIPT (which RPUSHes the list and HSETs the hash atomically): the
    # producer could re-fill both between our check and our delete, and we'd then wipe a live
    # prio hash, silently corrupting within-specifier priority ordering.

    match = f"{QUEUE_KEY_PREFIX}{affinity_queue_name_glob(base)}{PRIO_KEY_SUFFIX}"
    prio_keys = list(_scan_decoded(redis, match))
    if not prio_keys:
        return 0
    list_keys = [pk[: -len(PRIO_KEY_SUFFIX)] for pk in prio_keys]
    delete_script = redis.register_script(ORPHANED_PRIO_CLEANER_SCRIPT)
    deleted = delete_script(prio_keys, list_keys)
    return int(deleted)
