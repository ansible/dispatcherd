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

    async def start(self):
        self.status = 'spawned'
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.process.start)
        self.status = 'starting'  # Not ready until it sends callback message

    async def stop(self):
        self.status = 'stopping'
        self.message_queue.put("stop")
        if self.current_task:
            uuid = self.current_task.get('uuid', '<unknown>')
            logger.warning(f'Worker {self.worker_id} is currently running task (uuid={uuid}), canceling for shutdown')
            self.cancel()
        self.process.join()
        logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited code={self.process.exitcode}')
        self.status = 'initialized'

    def cancel(self):
        # SIGTERM
        self.process.terminate()

    def mark_finished_task(self):
        self.current_task = None
        self.finished_count += 1


class WorkerPool:
    def __init__(self, num_workers):
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

    async def start_working(self, dispatcher):
        await self._spawn_workers()
        self.read_results_task = asyncio.create_task(self.read_results_forever())
        self.read_results_task.add_done_callback(dispatcher.fatal_error_callback)

    async def worker_start(self, worker):
        "Wraps worker start method just so we can set pool variables as result"
        logger.debug(f'Starting subprocess for worker {worker.worker_id}')
        await worker.start()
        self.start_worker_task = None  # de-reference myself
        # if we bursted a whole bunch of pool.up(), like for startup, we may need to do more work
        await self.start_next_worker()
        await self.drain_queue()  # see if there is any waiting work this new worker can handle

    async def start_next_worker(self):
        """If a worker is initialized but does not have a process started, start a task to do so

        The reason for this as opposed to just... starting the workers,
        is so we do not block the main process
        this should be an idepotent method which may be called in many hooks"""
        if self.start_worker_task:
            return  # do not be starting up more than 1 worker at a time
        for worker in self.workers.values():
            if worker.status == 'initialized':
                self.start_worker_task = asyncio.create_task(self.worker_start(worker))
                return

    async def up(self):
        worker = PoolWorker(worker_id=self.next_worker_id, finished_queue=self.finished_queue)
        self.workers[self.next_worker_id] = worker
        self.next_worker_id += 1
        await self.start_next_worker()

    async def _spawn_workers(self):
        while len(self.workers) < self.num_workers:
            await self.up()

    async def stop_workers(self):
        for worker in self.workers.values():
            await worker.stop()

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
        await self.stop_workers()

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
                await self.start_worker_task
            except asyncio.CancelledError:
                pass  # intended

        logger.info('The finished watcher has returned. Pool is shut down')

    async def dispatch_task(self, message):
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

        logging.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")

        # Put the message in the selected worker's queue
        worker.current_task = message  # NOTE: this marks the worker as busy
        worker.message_queue.put(message)

    async def drain_queue(self):
        if self.queued_messages and (not self.shutting_down):
            requeue_message = self.queued_messages.pop()
            await self.dispatch_task(requeue_message)

    async def process_finished(self, worker, message):
        msg = f"Worker {worker.worker_id} finished task, ct={worker.finished_count}"
        if message.get("result"):
            result = message["result"]
            msg += f", result: {result}"
        logger.debug(msg)

        # Mark the worker as no longer busy
        worker.mark_finished_task()
        self.finished_count += 1

    async def read_results_forever(self):
        """Perpetual task that continuously waits for task completions."""
        loop = asyncio.get_event_loop()
        while True:
            # Wait for a result from the finished queue
            message = await loop.run_in_executor(None, self.finished_queue.get)
            worker_id = message["worker"]
            event = message["event"]
            worker = self.workers[worker_id]

            if event == 'ready':
                worker.status = 'ready'
                await self.drain_queue()

            elif event == 'shutdown':
                # TODO: remove worker from worker list... but we do not have autoscale pool yet so need that
                worker.status = 'exited'
                if self.shutting_down:
                    if all(worker.status == 'exited' for worker in self.workers.values()):
                        logger.debug(f"Worker {worker_id} exited and that is all, exiting finished monitoring.")
                        break
                    else:
                        logger.debug(f"Worker {worker_id} exited and that is a good thing because we are trying to shut down.")
                elif not self.workers:
                    logger.info('All workers exited, exiting results thread out of abundance of caution')
                    break
                else:
                    logger.debug(f"Worker {worker_id} finished exiting. The rest of this is not yet coded.")
                    continue

            elif event == 'done':
                await self.process_finished(worker, message)
                await self.drain_queue()
