import argparse
import logging

from redis import StrictRedis

from minique.work.worker import Worker

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--redis-url', required=True)
parser.add_argument('-q', '--queues', nargs='+', required=True)


def main():
    args = parser.parse_args()
    logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
    redis = StrictRedis.from_url(args.redis_url)
    worker = Worker.for_queue_names(redis=redis, queue_names=args.queues)
    worker.log.info('Worker initialized')
    while True:
        try:
            worker.tick()
        except KeyboardInterrupt:
            break
        except:
            worker.log.error('Unexpected worker tick error', exc_info=True)
