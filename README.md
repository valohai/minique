# minique /miniːk/

A minimal Redis 4.0+ job queue for Python 3.7 and above.

## Requirements

* Python 3.7+
* Redis 4.0+

## Usage

- Have a Redis 4.0+ server running.

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
