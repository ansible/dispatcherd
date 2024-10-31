import time

from dispatcher.publish import task


@task(queue='test_channel')
def sleep_function(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel')
def print_hello():
    print('hello world!!')
