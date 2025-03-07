import pytest


@pytest.mark.benchmark(group="by_system")
def test_clear_time_with_full_server(benchmark, with_full_server):
    with with_full_server(4) as server:
        benchmark(server.run_benchmark_test, server.queue_in, server.queue_out, 100)


@pytest.mark.benchmark(group="by_system")
def test_clear_time_with_only_pool(benchmark, with_pool_server):
    with with_pool_server(4) as pool_server:
        benchmark(pool_server.run_benchmark_test, pool_server.queue_in, pool_server.queue_out, 100)
