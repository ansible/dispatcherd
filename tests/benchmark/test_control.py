import asyncio

import pytest

from dispatcher.factories import get_control_from_settings, get_publisher_from_settings


@pytest.mark.benchmark(group="control")
def test_alive_benchmark(benchmark, with_full_server, test_settings):
    control = get_control_from_settings(settings=test_settings)

    def alive_check():
        r = control.control_with_reply('alive')
        assert r == [{'node_id': 'benchmark-server'}]

    with with_full_server(4):
        benchmark(alive_check)


@pytest.mark.benchmark(group="control")
@pytest.mark.parametrize('messages', [0, 3, 4, 5, 10, 100])
def test_alive_benchmark_while_busy(benchmark, with_full_server, benchmark_settings, messages):
    control = get_control_from_settings(settings=benchmark_settings)
    broker = get_publisher_from_settings('pg_notify', settings=benchmark_settings)
    broker.get_connection()  # warm connection saver

    def alive_check():
        function = 'lambda: __import__("time").sleep(0.01)'
        for i in range(messages):
            broker.publish_message(channel='test_channel', message=function)
        r = control.control_with_reply('alive', timeout=2)
        assert r == [{'node_id': 'benchmark-server'}]

    with with_full_server(4):
        benchmark(alive_check)
