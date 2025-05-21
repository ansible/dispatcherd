from queue import SimpleQueue

from dispatcherd.worker.target import work_loop_internal
from dispatcherd.worker.task import TaskWorker
from tests.data.methods import print_hello


class RecordingQueue:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def put(self, item) -> None:  # type: ignore[no-untyped-def]
        self.items.append(item)


def test_work_loop_internal_runs_demo_task():
    # Importing print_hello registers it in the default registry via its decorator
    assert print_hello.__module__ == 'tests.data.methods'

    message_queue: SimpleQueue = SimpleQueue()
    finished_queue = RecordingQueue()
    worker = TaskWorker(worker_id=3, message_queue=message_queue, finished_queue=finished_queue)

    message_queue.put({'task': 'tests.data.methods.print_hello'})
    message_queue.put('stop')

    work_loop_internal(worker)

    assert finished_queue.items[0] == worker.get_ready_message()
    processed_message = finished_queue.items[1]
    assert processed_message['event'] == 'done'
    assert processed_message['worker'] == 3
    assert processed_message['result'] is None
    assert finished_queue.items[2] == worker.get_shutdown_message()
