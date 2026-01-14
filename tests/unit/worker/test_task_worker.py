from queue import SimpleQueue

import pytest

from dispatcherd.publish import task
from dispatcherd.worker.exceptions import DispatcherExit
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


@pytest.mark.parametrize("followup_key", ("callbacks", "errbacks"))
def test_perform_work_marks_stopping_for_followup_exit(registry, followup_key):
    queue = SimpleQueue()
    worker = TaskWorker(worker_id=9, registry=registry, message_queue=queue, finished_queue=queue)
    ran: list[str] = []

    @task(registry=registry)
    def ok_task():
        return 'ok'

    @task(registry=registry)
    def failing_task():
        raise RuntimeError('boom')

    @task(registry=registry)
    def exiting_followup():
        raise DispatcherExit('<stop>')

    @task(registry=registry)
    def should_not_run():
        ran.append(followup_key)

    if followup_key == 'callbacks':
        primary = ok_task
    else:
        primary = failing_task

    message: dict[str, object] = {
        'task': registry.get_from_callable(primary).serialize_task(),
        'uuid': f'{followup_key}-exit',
        followup_key: [
            {'task': registry.get_from_callable(exiting_followup).serialize_task()},
            {'task': registry.get_from_callable(should_not_run).serialize_task()},
        ],
    }

    finished = worker.perform_work(message)

    assert finished['is_stopping'] is True
    assert ran == []


def test_perform_work_skips_callbacks_after_signal(registry):
    queue = SimpleQueue()
    worker = TaskWorker(worker_id=11, registry=registry, message_queue=queue, finished_queue=queue)
    ran: list[str] = []

    @task(registry=registry)
    def signal_task():
        worker.signal_handler.exit_gracefully()
        return 'done'

    @task(registry=registry)
    def should_not_run():
        ran.append('callback')

    message = {
        'task': registry.get_from_callable(signal_task).serialize_task(),
        'uuid': 'sig-cb',
        'callbacks': [{'task': registry.get_from_callable(should_not_run).serialize_task()}],
    }

    finished = worker.perform_work(message)

    assert finished['is_stopping'] is True
    assert ran == []
