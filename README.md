# minique /miniːk/

A minimal Redis job queue for Python 3.

## Usage

- Have a Redis server running.

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

- Ensure your workers are able to import the functions you wish to run.
- Set the callables the worker will allow with `--allow-callable`.
  - Alternately, you may wish to subclass `minique.work.job_runner.JobRunner`
    to specify an entirely different lookup mechanism.

```bash
$ minique -u redis://localhost:6379/4 -q work -q anotherqueue -q thirdqueue --allow-callable 'my_jobs.*'
```

## Sentry Support

Minique automatically integrates with the [Sentry](https://sentry.io/welcome/)
exception tracking service.

You can use the `[sentry]` [installation extra][extras] to install `sentry-sdk` along with Minique,
or you can do it manually.

Simply set the `SENTRY_DSN` environment variable; if all goes well,
you should see a "Sentry configured with a valid DSN" message at CLI boot.

The [other environment-configurable options](https://docs.sentry.io/platforms/python/configuration/options/)
also work as you would expect.

Exceptions occurring during job execution will be sent to Sentry and annotated with `minique` context
describing the job ID and queue name.

### Development

* Linting happens with `pre-commit`.
* Tests are run with `pytest`. (Install with `pip install -e .[test]`.)
* Code in `minique` is expected to pass `mypy --strict`.

[extras]: https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras
