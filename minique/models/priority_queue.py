from __future__ import annotations

from typing import TYPE_CHECKING

from minique.models.queue import Queue

if TYPE_CHECKING:
    from redis import Redis

    from minique.models.job import Job


ADD_JOB_SCRIPT = """
local queue_key = ARGV[1]
local queue_prio_lookup = ARGV[2]
local job_key = ARGV[3]
local job_id = ARGV[4]
local job_priority = tonumber(redis.call("HGET", job_key, "priority")) or 0
local job_index = 1
local insert_status = -1

-- Check the priority for the last job in the queue, we might become the new last job
-- This is generally more efficient than iterating the queue.
local last_job_id = redis.call("LRANGE", queue_key, -1, -1)[1]
if last_job_id ~= nil then
    local last_job_priority = tonumber(redis.call("HGET", queue_prio_lookup, last_job_id)) or 0
    if last_job_priority >= job_priority then
        insert_status = redis.call("RPUSH", queue_key, job_id)
        -- Insert status is the length of the queue
        job_index = insert_status
    end
else
    -- The queue is empty.
    insert_status = redis.call("RPUSH", queue_key, job_id)
end

if insert_status == -1 then
    -- Our place in the queue is somewhere in the middle, search for it
    for i, queued_job_id in ipairs(redis.call("LRANGE", queue_key, 0, -1)) do
        local queued_job_priority = tonumber(redis.call("HGET", queue_prio_lookup, queued_job_id)) or 0
        if queued_job_priority < job_priority then
            -- Found the first job with a lower priority, insert before it
            insert_status = redis.call("LINSERT", queue_key, "BEFORE", queued_job_id, job_id)
            job_index = i
            break
        end
    end
end

if insert_status == -1 then
    -- No higher priority jobs found, insert to the end
    redis.call("RPUSH", queue_key, job_id)
end

-- Introduce the newly added job to the priority lookup hash
redis.call("HSET", queue_prio_lookup, job_id, job_priority)

return job_index
"""

PRIO_HASH_CLEANER_SCRIPT = """
local queue_key = ARGV[1]
local queue_prio_lookup = ARGV[2]

local queue_len = tonumber(redis.call("LLEN", queue_key))
local hash_len = tonumber(redis.call("HLEN", queue_prio_lookup))

-- Tolerate some excess
if queue_len + 100 >= hash_len then
    return -1
end

local new_hash = {}
local count = 0

for i, queued_job_id in ipairs(redis.call("LRANGE", queue_key, 0, -1)) do
    local priority = redis.call("HGET", queue_prio_lookup, queued_job_id)
    new_hash[queued_job_id] = priority
    count = i
end
redis.call("DEL", queue_prio_lookup)

for queued_job_id, priority in pairs(new_hash) do
    redis.call("HSET", queue_prio_lookup, queued_job_id, priority)
end

return hash_len - count
"""


class PriorityQueue(Queue):
    """Alternate Queue implementation that uses the optional `Job.priority` attribute
    to sort the queue when adding jobs.

    When using the priority queue, ensure the `PriorityQueue.finish_job()` method is called
    for each job after it leaves the queue to trim the job priority lookup hash.
    This can be performed by either the consumer or the job manager, or both.

    Alternately, or in addition, periodically call `PriorityQueue.periodic_clean()` to
    remove stale keys from the priority lookup hash.
    """

    def __init__(self, redis: Redis[bytes], name: str) -> None:
        super().__init__(redis, name)
        self.add_job_script = redis.register_script(ADD_JOB_SCRIPT)
        self.hash_clean_script = redis.register_script(PRIO_HASH_CLEANER_SCRIPT)

    @property
    def prio_key(self) -> str:
        return f"{self.redis_key}prio"

    def add_job(self, job: Job) -> int:
        script_response = self.add_job_script(
            keys=[self.redis_key, self.prio_key, job.redis_key],
            args=[self.redis_key, self.prio_key, job.redis_key, job.id],
        )
        return int(script_response) - 1

    def dequeue_job(self, job_id: str) -> bool:
        """Dequeue a job from any position in the queue, cleaning it from the priority
        lookup hash.
        """
        self.redis.hdel(self.prio_key, job_id)
        num_removed = self.redis.lrem(self.redis_key, 0, job_id)
        return num_removed > 0

    def clean_job(self, job: Job) -> None:
        """Cleans up job data after the job has exited the queue."""
        self.redis.hdel(self.prio_key, job.id)

    def periodic_clean(self) -> int:
        """Perform occasional maintenance on the data structures

        :return: Number of cleaned up values
        """
        script_response = self.hash_clean_script(
            keys=[self.redis_key, self.prio_key],
            args=[self.redis_key, self.prio_key],
        )
        return int(script_response)
