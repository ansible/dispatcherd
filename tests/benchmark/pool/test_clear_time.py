import os
import sys

import pytest


@pytest.mark.benchmark(group="by_task")
@pytest.mark.parametrize('times', [1, 10, 100, 1000])
def test_clear_sleep_by_task_number(benchmark, times, with_pool_server):
    with with_pool_server(4, function='lambda: __import__("time").sleep(0.01)') as pool_server:
        benchmark(pool_server.run_benchmark_test, pool_server.queue_in, pool_server.queue_out, times)


@pytest.mark.benchmark(group="by_task")
@pytest.mark.parametrize('times', [1, 10, 100, 1000])
def test_clear_no_op_by_task_number(benchmark, times, with_pool_server):
    with with_pool_server(4, function='lambda: None') as pool_server:
        benchmark(pool_server.run_benchmark_test, pool_server.queue_in, pool_server.queue_out, times)


@pytest.mark.benchmark(group="by_worker_sleep")
@pytest.mark.parametrize('workers', [1, 4, 12, 24, 50, 75])
def test_clear_sleep_by_worker_count(benchmark, workers, with_pool_server):
    with with_pool_server(workers, function='lambda: __import__("time").sleep(0.01)') as pool_server:
        benchmark(pool_server.run_benchmark_test, pool_server.queue_in, pool_server.queue_out, 100)


@pytest.mark.benchmark(group="by_worker_math")
@pytest.mark.parametrize('workers', [1, 4, 12, 24, 50, 75])
def test_clear_math_by_worker_count(benchmark, workers, with_pool_server):
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
    sys.path.append(root_dir)

    with with_pool_server(workers, function='lambda: __import__("tests.data.methods").fibonacci(26)') as pool_server:
        benchmark(pool_server.run_benchmark_test, pool_server.queue_in, pool_server.queue_out, 100)
