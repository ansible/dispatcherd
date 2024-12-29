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
