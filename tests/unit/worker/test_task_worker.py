import multiprocessing

from dispatcherd.publish import task
from dispatcherd.worker.task import TaskWorker


# Must define here to be importable
def my_bound_task(dispatcher):
    assert dispatcher.uuid == '12345'


def test_run_method_with_bind(registry):

    task(bind=True, registry=registry)(my_bound_task)

    dmethod = registry.get_from_callable(my_bound_task)

    worker = TaskWorker(1, registry=registry, message_queue=multiprocessing.Queue(), finished_queue=multiprocessing.Queue())
    worker.run_callable({
        "task": dmethod.serialize_task(),
        "uuid": "12345"
    })
