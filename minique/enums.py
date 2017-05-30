from enum import Enum


class JobStatus(Enum):
    NONE = 'none'
    ACQUIRED = 'acquired'
    SUCCESS = 'success'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
