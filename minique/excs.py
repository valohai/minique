class NoSuchJob(Exception):
    pass


class DuplicateJob(Exception):
    pass


class AlreadyAcquired(Exception):
    pass


class AlreadyResulted(Exception):
    pass
