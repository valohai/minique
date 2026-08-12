# minique /miniːk/

A minimal Redis 4.0+/Valkey job queue for Python 3.10 and above.

## Requirements

### Client

* Python 3.10+
* Either redis-py 2.10+, or valkey 6.0

### Server

* A Redis 4.0+ or Valkey server

## Usage

- Have a Redis 4.0+ or Valkey server running.

### Client

```python
from redis import Redis  # (or from valkey import Valkey as Redis)
from minique.api import enqueue, get_job

# Get a Redis connection, somehow.
redis = Redis.from_url('redis://localhost:6379/4')

job = enqueue(
    redis=redis,
    queue_name='work',
    callable='my_jobs.calcumacalate',  # Dotted path to your callable.
    kwargs={'a': 5, 'b': 5},  # Only kwargs supported.
    # You can also set a `job_id` yourself (but it must be unique)
)

job_id = job.id  # Save the job ID somewhere, maybe?

while not job.has_finished:
    pass  # Twiddle thumbs...

print(job.result)  # Okay!

# Get the same job later (though not later than 7 days (by default)):
job = get_job(redis, job_id)
```

### Worker(s)

- Ensure your workers are able to import the functions you wish to run.
- Set the callables the worker will allow with `--allow-callable`.
    - Alternately, you may wish to subclass `minique.work.job_runner.JobRunner`
      to specify an entirely different lookup mechanism.

```bash
$ minique -u redis://localhost:6379/4 -q work -q anotherqueue -q thirdqueue --allow-callable 'my_jobs.*'
```

### Priority Queues

Minique supports priority queueing as an optional feature using the `enqueue_priority` API.

Priority queues are compatible with standard workers. However, priority is implemented using a
helper data structure, requiring the client needs to call `job.cleanup()` after each job and/or
`PriorityQueue(...).periodic_clean()` to prune this structure of jobs that have already been
processed.

Priority queue requires Lua scripting permissions from the Redis queue service.

```python
from redis import Redis  # (or from valkey import Valkey as Redis)
from minique.api import enqueue_priority, get_job

# Get a Redis connection, somehow.
redis = Redis.from_url('redis://localhost:6379/4')

job = enqueue_priority(
    redis=redis,
    queue_name='urgent_work',
    callable='my_jobs.calcumacalate',  # Dotted path to your callable.
    kwargs={'a': 5, 'b': 5},  # Only kwargs supported.
    priority=1,  # Integer
    # You can also set a `job_id` yourself (but it must be unique)
)

job_id = job.id  # Save the job ID somewhere, maybe?

while not job.has_finished:
    pass  # Twiddle thumbs...

print(job.result)  # Okay!

# Job priorities are stored in a helper hash table which should be cleaned using this method
# after the job has left the queue.
job.cleanup()

# Get the same job later (though not later than 7 days (by default)):
job = get_job(redis, job_id)
```

### Affinity

Jobs can carry one or more opaque **affinity specifiers** — typically identifiers for
something expensive to fetch, for instance.
A worker that recently ran a job with a given specifier becomes "warm" for it and *prefers*
to pick up further jobs with the same specifier.

This is purely an optimization and never a constraint. Workers may always end up pulling jobs
from the base queue, and jobs may be picked up by any worker regardless of affinity.

```python
from minique.api import enqueue

# Two jobs that both need the same large dataset will prefer the same worker:
job_a = enqueue(redis, 'training', 'my_jobs.train', kwargs={'fold': 0}, affinity=['dataset-42'])
job_b = enqueue(redis, 'training', 'my_jobs.train', kwargs={'fold': 1}, affinity=['dataset-42'])
```

`enqueue_priority` accepts `affinity` too; each affinity sub-queue is itself a priority queue,
so ordering within a specifier still respects `priority`.

> **Affinity outranks priority on a warm worker.** Because a warm worker checks its affinity
> sub-queues before the base queue, it can pick up a *lower*-priority affine job ahead of a
> *higher*-priority job waiting in the base priority queue.

#### Cleaning up sub-queue residue

Affinity sub-queues self-heal while jobs for a specifier keep flowing. Once a specifier goes
quiet, a little residue can linger (phantom ids for expired jobs, and orphaned `…prio` hashes
for priority queues). Trim it on demand with

`minique.api.clean_affinity_sub_queues(redis, base_queue_name, priority=...)`.

The call is idempotent, best-effort, and safe to run concurrently from many workers, so the
simplest approach is to fire it occasionally with e.g. `random.random() < 0.01`.

## Library support

### orjson

If [`orjson`](https://github.com/ijl/orjson) is installed,
it will automatically be used for faster JSON serialization and deserialization of job arguments and results.

### Sentry

Minique automatically integrates with the [Sentry](https://sentry.io/welcome/)
exception tracking service.

You can use the `[sentry]` [installation extra][extras] to install `sentry-sdk` along with Minique,
or you can do it manually.

Simply set the `SENTRY_DSN` environment variable; if all goes well,
you should see a "Sentry configured with a valid DSN" message at CLI boot.

The [other environment-configurable options](https://docs.sentry.io/platforms/python/configuration/options/)
also work as you would expect.

Exceptions occurring during job execution will be sent to Sentry and annotated with `minique`
context describing the job ID and queue name.

### Development

```shell
# install `minique` in editable mode with development dependencies
pip install -e .[sentry,test] pre-commit mypy==1.0.0 types-redis && pre-commit install

# run lints
pre-commit run --all-files

# run type checks
mypy --strict --install-types --show-error-codes minique

# run tests against the specified Redis database
REDIS_URL=redis://localhost:6379/0 pytest .
```

### Release

```shell
# decide on a new version number and set it
vim minique/__init__.py
__version__ = "0.9.0"

npx auto-changelog --commit-limit=0 -v 0.9.0

# undo changes changelog generation did to the older entries

git add -u
git commit -m "Become 0.9.0"
git tag -m "v0.9.0" -a v0.9.0

git push --follow-tags
```

[extras]: https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras
