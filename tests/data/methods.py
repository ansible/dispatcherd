import time

from dispatcher.publish import task


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


# fibonacci is a slow method in raw processing power
# demo values - times n in seconds
# 30 - 0.052
# 29 - 0.035
# 28 - 0.024
# 27 - 0.015
# 26 - 0.012
# 25 - 0.0097

@task(queue='test_channel')
def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)
