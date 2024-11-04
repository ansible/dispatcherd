import asyncio
import logging
import multiprocessing
import os

from dispatcher.worker.task import work_loop

logger = logging.getLogger(__name__)


class PoolWorker:
    def __init__(self, worker_id, finished_queue):
        self.worker_id = worker_id
        # TODO: rename message_queue to call_queue, because this is what cpython ProcessPoolExecutor calls them
        self.message_queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(target=work_loop, args=(self.worker_id, self.message_queue, finished_queue))
        self.current_task = None
        self.finished_count = 0
        self.status = 'initialized'
        self.exit_msg_event = asyncio.Event()

    async def start(self):
        self.status = 'spawned'
        self.process.start()
        logger.debug(f'Worker {self.worker_id} pid={self.process.pid} subprocess has spawned')
        self.status = 'starting'  # Not ready until it sends callback message

    async def join(self):
        logger.debug(f'Joining worker {self.worker_id} pid={self.process.pid} subprocess')
        self.process.join()

    async def stop(self):
        self.message_queue.put("stop")
        if self.current_task:
            uuid = self.current_task.get('uuid', '<unknown>')
            logger.warning(f'Worker {self.worker_id} is currently running task (uuid={uuid}), canceling for shutdown')
            self.cancel()

        try:
            await asyncio.wait_for(self.exit_msg_event.wait(), timeout=3)
            self.exit_msg_event.clear()
            self.process.join(3)  # argument is timeout
        except asyncio.TimeoutError:
            logger.error(f'Worker {self.worker_id} pid={self.process.pid} failed to send exit message in 3 seconds')
            self.status = 'error'

        for i in range(3):
            if self.process.is_alive():
                logger.error(f'Worker {self.worker_id} pid={self.process.pid} is still alive after SIGKILL, try {i}')
                await asyncio.sleep(1)
                self.process.kill()
            else:
                logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited code={self.process.exitcode}')
                self.status = 'initialized'
                return

        logger.critical(f'Worker {self.worker_id} pid={self.process.pid} failed to exit after SIGKILL')
        self.status = 'error'
        return

    def cancel(self):
        self.process.terminate()  # SIGTERM

    def mark_finished_task(self):
        self.current_task = None
        self.finished_count += 1


class WorkerPool:
    def __init__(self, num_workers, fd_lock=None):
        self.num_workers = num_workers
        self.workers = {}
        self.next_worker_id = 0
        self.finished_queue = multiprocessing.Queue()
        self.queued_messages = []  # TODO: use deque, invent new kinds of logging anxiety
        self.read_results_task = None
        self.start_worker_task = None
        self.shutting_down = False
        self.finished_count = 0
        self.shutdown_timeout = 3
        self.management_event = asyncio.Event()  # Process spawning is backgrounded, so this is the kicker
        self.management_lock = asyncio.Lock()
        self.fd_lock = fd_lock or asyncio.Lock()

    async def start_working(self, dispatcher):
        self.read_results_task = asyncio.create_task(self.read_results_forever())
        self.read_results_task.add_done_callback(dispatcher.fatal_error_callback)
        self.management_task = asyncio.create_task(self.manage_workers())
        self.management_task.add_done_callback(dispatcher.fatal_error_callback)

    async def manage_workers(self):
        """Enforces worker policy like min and max workers, and later, auto scale-down"""
        while not self.shutting_down:
            while len(self.workers) < self.num_workers:
                await self.up()

            for worker in self.workers.values():
                if worker.status == 'initialized':
                    logger.debug(f'Starting subprocess for worker {worker.worker_id}')
                    async with self.fd_lock:  # never fork while connecting
                        await worker.start()

            await self.management_event.wait()
        logger.debug('Pool worker management task exiting')

    async def up(self):
        worker = PoolWorker(worker_id=self.next_worker_id, finished_queue=self.finished_queue)
        self.workers[self.next_worker_id] = worker
        self.next_worker_id += 1

    async def stop_workers(self):
        stop_tasks = [worker.stop() for worker in self.workers.values()]
        await asyncio.gather(*stop_tasks)

    async def force_shutdown(self):
        for worker in self.workers.values():
            if worker.process.is_alive():
                logger.warning(f'Force killing worker {worker.worker_id} pid={worker.process.pid}')
                os.kill(worker.process.pid)

        self.read_results_task.cancel()
        logger.info('Finished watcher had to be canceled, awaiting it a second time')
        try:
            await self.read_results_task
        except asyncio.CancelledError:
            pass

    async def shutdown(self):
        self.shutting_down = True
        self.management_event.set()
        await self.stop_workers()
        self.finished_queue.put('stop')

        if self.read_results_task:
            logger.info('Waiting for the finished watcher to return')
            try:
                await asyncio.wait_for(self.read_results_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning(f'The finished task failed to cancel in {self.shutdown_timeout} seconds, will force.')
                await self.force_shutdown()
            except asyncio.CancelledError:
                logger.info('The finished task was canceled, but we are shutting down so that is alright')
            except Exception:
                # traceback logged in fatal callback
                if not hasattr(self.read_results_task, '_dispatcher_tb_logged'):
                    logger.exception('Pool shutdown saw an unexpected exception from results task')

        if self.start_worker_task:
            logger.info('Canceling worker spawn task')
            self.start_worker_task.cancel()
            try:
                await asyncio.wait_for(self.start_worker_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.error('The scaleup task failed to shut down')
            except asyncio.CancelledError:
                pass  # intended

        if self.queued_messages:
            uuids = [message.get('uuid', '<unknown>') for message in self.queued_messages]
            logger.error(f'Dispatcher shut down with queued work, uuids: {uuids}')

        logger.info('The finished watcher has returned. Pool is shut down')

    async def dispatch_task(self, message):
        async with self.management_lock:
            uuid = message.get("uuid", "<unknown>")
            worker = None
            for candidate_worker in self.workers.values():
                if (not candidate_worker.current_task) and candidate_worker.status == 'ready':
                    worker = candidate_worker
                    break

            if not worker or self.shutting_down:
                # TODO: under certain conditions scale up workers
                if self.shutting_down:
                    logger.info(f'Not starting task (uuid={uuid}) because we are shutting down')
                else:
                    logger.warning(f'Ran out of workers, queueing task (uuid={uuid}), current ct={len(self.queued_messages)}')
                self.queued_messages.append(message)
                return

            logger.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")

            # Put the message in the selected worker's queue
            worker.current_task = message  # NOTE: this marks the worker as busy
        worker.message_queue.put(message)

    async def drain_queue(self):
        if self.queued_messages and (not self.shutting_down):
            requeue_message = self.queued_messages.pop()
            await self.dispatch_task(requeue_message)

    async def process_finished(self, worker, message):
        uuid = message.get('uuid', '<unknown>')
        msg = f"Worker {worker.worker_id} finished task (uuid={uuid}), ct={worker.finished_count}"
        if message.get("result"):
            result = message["result"]
            msg += f", result: {result}"
        logger.debug(msg)

        # Mark the worker as no longer busy
        async with self.management_lock:
            worker.mark_finished_task()
            self.finished_count += 1

    async def read_results_forever(self):
        """Perpetual task that continuously waits for task completions."""
        loop = asyncio.get_event_loop()
        while True:
            # Wait for a result from the finished queue
            message = await loop.run_in_executor(None, self.finished_queue.get)

            if message == 'stop':
                if self.shutting_down:
                    stats = [worker.status for worker in self.workers.values()]
                    logger.debug(f'Results message got administrative stop message, worker status: {stats}')
                    return
                else:
                    logger.error('Results queue got stop message even through not shutting down')
                    continue

            worker_id = message["worker"]
            event = message["event"]
            worker = self.workers[worker_id]

            if event == 'ready':
                worker.status = 'ready'
                await self.drain_queue()

            elif event == 'shutdown':
                async with self.management_lock:
                    worker.status = 'exited'
                    worker.exit_msg_event.set()
                if self.shutting_down:
                    if all(worker.status in ['exited', 'error', 'initialized'] for worker in self.workers.values()):
                        logger.debug(f"Worker {worker_id} exited and that is all of them, exiting results read task.")
                        return
                    else:
                        stats = [worker.status for worker in self.workers.values()]
                        logger.debug(f"Worker {worker_id} exited and that is a good thing because we are trying to shut down. Remaining: {stats}")
                else:
                    await worker.join()
                    async with self.management_lock:
                        del self.workers[worker.worker_id]
                        self.management_event.set()
                    logger.debug(f"Worker {worker_id} finished exiting. It will be restarted.")

            elif event == 'done':
                await self.process_finished(worker, message)
                await self.drain_queue()
