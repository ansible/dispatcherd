from unittest import mock

import pytest

from dispatcherd.processors.blocker import Blocker
from dispatcherd.publish import submit_task, task


@pytest.fixture
def mock_apply_async():
    with mock.patch('dispatcherd.registry.DispatcherMethod.apply_async') as apply_async:
        yield apply_async


def test_method_normal_call(registry, mock_apply_async):
    def test_method():
        return

    test_method = task(registry=registry)(test_method)

    test_method.delay()

    mock_apply_async.assert_called_once_with(args=(), kwargs={})


def test_method_call_with_args_kwargs(registry, mock_apply_async):
    def test_method(*args, **kwargs):
        return

    test_method = task(registry=registry)(test_method)

    test_method.delay(1, 2, 3, foo=6, bar=7)

    mock_apply_async.assert_called_once_with(args=(1, 2, 3), kwargs={"foo": 6, "bar": 7})


def test_method_call_with_options(registry, mock_apply_async):
    def test_method(*args, **kwargs):
        return

    test_method = task(registry=registry)(test_method)

    test_method.apply_async(args=[1, 2], kwargs={"foo": 6, "bar": 7}, queue='foo_channel', uuid='1234', on_duplicate='run_once')

    mock_apply_async.assert_called_once_with(args=[1, 2], kwargs={"foo": 6, "bar": 7}, queue='foo_channel', uuid='1234', on_duplicate='run_once')


def test_using_as_decorator(registry, mock_apply_async):
    @task(registry=registry)
    def test_method():
        return

    test_method.delay()

    mock_apply_async.assert_called_once_with(args=(), kwargs={})


def test_decorator_kwargs(registry):
    @task(queue='foobar', on_duplicate='run_once', registry=registry)
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)
    assert len(dmethod.processor_options) == 1

    assert dmethod.get_async_body()['on_duplicate'] == 'run_once'


def test_class_normal_call(registry, mock_apply_async):
    class TestMethod:
        def run(self):
            return

    task(registry=registry)(TestMethod)

    TestMethod.delay()

    mock_apply_async.assert_called_once_with(args=(), kwargs={})


def test_submit_task_method(registry, mock_apply_async, test_settings):
    @task(queue='foobar', registry=registry)
    def test_method():
        return

    submit_task(test_method, registry=registry, settings=test_settings)

    mock_apply_async.assert_called_once()
    args, kwargs = mock_apply_async.call_args
    assert not args
    assert len(kwargs) > 0
    for kwarg, value in kwargs.items():
        if kwarg == 'settings':
            assert value is test_settings
        else:
            assert not value


def test_decorate_with_processor(registry):
    @task(queue='foobar', registry=registry, processor_options=(Blocker.Params(on_duplicate='serial'),))
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)

    kwargs = dmethod.get_async_body()
    assert kwargs['on_duplicate'] == 'serial'
