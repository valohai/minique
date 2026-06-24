QUEUE_KEY_PREFIX = "mqq:"
JOB_KEY_PREFIX = "mqj:"
RESULT_KEY_PREFIX = "mqr:"

# Suffix of a PriorityQueue's priority lookup hash key (see PriorityQueue.prio_key).
PRIO_KEY_SUFFIX = "prio"

# Infix separating a base queue name from a hashed affinity specifier in an affinity sub-queue name.
# No special (e.g. invisible/control) characters are used, so as to keep things introspectable
# using ASCII only.
AFFINITY_QUEUE_INFIX = "+AFF+"
