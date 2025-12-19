from queue import SimpleQueue

from dispatcherd.publish import task
from dispatcherd.worker.task import TaskWorker


# Must define here to be importable
def my_bound_task(dispatcher):
    assert dispatcher.uuid == '12345'


def test_run_method_with_bind(registry):

    task(bind=True, registry=registry)(my_bound_task)

    dmethod = registry.get_from_callable(my_bound_task)

    queue = SimpleQueue()
    worker = TaskWorker(1, registry=registry, message_queue=queue, finished_queue=queue)
    worker.run_callable({"task": dmethod.serialize_task(), "uuid": "12345"})
    worker.mark_shutdown_notified()


class DummyQueue:
    def __init__(self) -> None:
        self.messages = []

    def put(self, item, block=True):  # type: ignore[no-untyped-def]
        self.messages.append((item, block))
