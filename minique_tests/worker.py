from minique.work.worker import Worker


class TestWorker(Worker):
    __test__ = False
    allowed_callable_patterns = {
        "minique_tests.*",
    }
