import asyncio
import contextlib
import multiprocessing
import time
from copy import deepcopy

import pytest

from dispatcher.brokers.pg_notify import create_connection
from dispatcher.config import DispatcherSettings
from dispatcher.factories import from_settings


class PoolServer:
    """Before you read more, know there are 3 contexts involved.

    This produces a method to be passed to pytest-benchmark.
    That method has to be ran inside a context manager,
    which will run (and stop) the relevant dispatcher code in a background process.
    """

    def __init__(self, config):
        self.config = config

    def run_benchmark_test(self, queue_in, queue_out, times):
        print(f'submitting message to pool server {times}')
        queue_in.put(str(times))
        print('waiting for reply message from pool server')
        message_in = queue_out.get()
        print(f'finished running round with {times} messages, got: {message_in}')
        if message_in == 'error':
            raise Exception('Test subprocess runner exception, look back in logs')

    @classmethod
    async def run_pool(cls, config, queue_in, queue_out, workers, function='lambda: __import__("time").sleep(0.01)'):
        this_config = config.copy()
        this_config['service']['pool_kwargs']['max_workers'] = workers
        dispatcher = from_settings(DispatcherSettings(this_config))
        pool = dispatcher.pool
        await pool.start_working(dispatcher.fd_lock)
        queue_out.put('ready')

        print('waiting for message to start test')
        loop = asyncio.get_event_loop()
        while True:
            print('pool server listening on queue_in')
            message = await loop.run_in_executor(None, queue_in.get)
            print(f'pool server got message {message}')
            if message == 'stop':
                print('shutting down pool server')
                pool.shutdown()
                break
            else:
                times = int(message.strip())
                print('creating cleared event task')
                cleared_event = asyncio.create_task(pool.events.work_cleared.wait())
                print('creating tasks for submissions')
                submissions = [pool.dispatch_task({'task': function, 'uuid': str(i)}) for i in range(times)]
                print('awaiting submission task')
                await asyncio.gather(*submissions)
                print('waiting for cleared event')
                await cleared_event
                pool.events.work_cleared.clear()
                await loop.run_in_executor(None, queue_out.put, 'done')
        print('exited forever loop of pool server')

    @classmethod
    def run_pool_loop(cls, config, queue_in, queue_out, workers, **kwargs):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(cls.run_pool(config, queue_in, queue_out, workers, **kwargs))
        except Exception:
            import traceback

            traceback.print_exc()
            # We are in a subprocess here, so even if we handle the exception
            # the main process will not know and still wait forever
            # so give them a kick on our way out
            print('sending error message after error')
            queue_out.put('error')
        finally:
            print('closing asyncio loop')
            loop.close()
        print('finished closing async loop')

    def start_server(self, workers, **kwargs):
        self.queue_in = multiprocessing.Queue()
        self.queue_out = multiprocessing.Queue()
        process = multiprocessing.Process(target=self.run_pool_loop, args=(self.config, self.queue_in, self.queue_out, workers), kwargs=kwargs)
        process.start()
        return process

    @contextlib.contextmanager
    def with_server(self, *args, **kwargs):
        process = self.start_server(*args, **kwargs)
        msg = self.queue_out.get()
        if msg != 'ready':
            raise RuntimeError('never got ready message from subprocess')
        try:
            yield self
        finally:
            self.queue_in.put('stop')
            process.terminate()  # SIGTERM
            # Poll to close process resources, due to race condition where it is not still running
            for i in range(3):
                time.sleep(0.1)
                try:
                    process.close()
                    break
                except Exception:
                    if i == 2:
                        raise


class FullServer(PoolServer):
    def run_benchmark_test(self, queue_in, queue_out, times):
        print('sending wakeup message to set new clear event')
        queue_in.put('wake')
        print('sending pg_notify messages')
        function = 'lambda: __import__("time").sleep(0.01)'
        conn = create_connection(**self.config['brokers']['pg_notify']['config'])
        with conn.cursor() as cur:
            for i in range(times):
                cur.execute(f"SELECT pg_notify('test_channel', '{function}');")
        print('waiting for reply message from pool server')
        message_in = queue_out.get()
        print(f'finished running round with {times} messages, got: {message_in}')

    @classmethod
    async def run_pool(cls, config, queue_in, queue_out, workers):
        this_config = config.copy()
        this_config['service']['pool_kwargs']['max_workers'] = workers
        this_config['service']['pool_kwargs']['min_workers'] = workers
        dispatcher = from_settings(DispatcherSettings(this_config))
        await dispatcher.start_working()
        # Make sure the dispatcher is listening before starting the tests which will submit messages
        for producer in dispatcher.producers:
            await producer.events.ready_event.wait()
        queue_out.put('ready')

        print('waiting for message to start test')
        loop = asyncio.get_event_loop()
        while True:
            print('pool server listening on queue_in')
            message = await loop.run_in_executor(None, queue_in.get)
            print(f'pool server got message {message}')
            if message == 'stop':
                print('shutting down server')
                dispatcher.shutdown()
                break
            print('creating cleared event task')
            cleared_event = asyncio.create_task(dispatcher.pool.events.queue_cleared.wait())
            print('waiting for cleared event')
            await cleared_event
            dispatcher.pool.events.queue_cleared.clear()
            await loop.run_in_executor(None, queue_out.put, 'done')
        print('exited forever loop of pool server')


@pytest.fixture
def benchmark_config(test_config):
    config = deepcopy(test_config)
    config['service']['main_kwargs']['node_id'] = 'benchmark-server'
    return config


@pytest.fixture
def benchmark_settings(benchmark_config):
    return DispatcherSettings(benchmark_config)


@pytest.fixture
def with_pool_server(benchmark_config):
    server_thing = PoolServer(benchmark_config)
    return server_thing.with_server


@pytest.fixture
def with_full_server(benchmark_config):
    server_thing = FullServer(benchmark_config)
    return server_thing.with_server
