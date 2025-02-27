class NoSuchJob(Exception):
    pass


class DuplicateJob(Exception):
    pass


class InvalidStatus(Exception):
    pass


class AlreadyAcquired(InvalidStatus):
    pass


class AlreadyResulted(InvalidStatus):
    pass


class InvalidJob(ValueError):
    pass


class MissingJobData(ValueError):
    pass


class Retry(Exception):
    pass
