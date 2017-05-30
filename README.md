minique /miniːk/
================

A minimal Redis job queue for Python 3.

Usage
-----

* Have a Redis server running.

### Client

```python
from redis import StrictRedis
from minique.api import enqueue, get_job

# Get a Redis connection, somehow.
redis = StrictRedis.from_url('redis://localhost:6379/4')

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

* Ensure your workers are able to import the functions you wish to run.

```bash
$ minique -u redis://localhost:6379/4 -q work -q anotherqueue -q thirdqueue
```

Todo
----

* Sentry support
