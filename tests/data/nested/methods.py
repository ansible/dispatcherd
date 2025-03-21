from dispatcherd.publish import task
from tests.data.nested.nested_registry import surprised_registry

"""
This module is intended to never be imported at top-of-file by tests
this creates the situation of a, valid, but "surprise" registration
to challenge our registry logic.
"""


@task(queue='test_channel', registry=surprised_registry)
def print_hello():
    print('hello world!!')
