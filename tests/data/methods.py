import time

from dispatcherd.publish import task


@task(queue='test_channel')
def sleep_function(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='discard')
def sleep_discard(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='serial')
def sleep_serial(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='queue_one')
def sleep_queue_one(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel')
def print_hello():
    print('hello world!!')


@task(queue='test_channel', bind=True)
def hello_world_binder(binder):
    print(f'Values in binder {vars(binder)}')
    print(f'Hello world, from worker {binder.worker_id} running task {binder.uuid}')


@task(queue='test_channel', timeout=1)
def task_has_timeout():
    time.sleep(5)


def get_queue_name():
    return 'test_channel'


@task(queue=get_queue_name)
def use_callable_queue():
    print('sucessful run using callable queue')


@task(queue=get_queue_name)
class RunJob:
    def run(self):
        print('successful run using callable queue with class')


@task(bind=True)
def prints_running_tasks(binder):
    r = binder.control('running')
    print(f'Obtained data on running tasks, result:\n{r}')


@task(bind=True)
def schedules_another_task(binder):
    r = binder.control('run', data={'task': 'tests.data.methods.print_hello'})
    print(f'Scheduled another task, result: {r}')
