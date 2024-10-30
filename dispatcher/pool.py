import asyncio
import json
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

    def start(self):
        self.process.start()
        self.status = 'starting'

    async def stop(self):
        self.status = 'stopping'
        self.message_queue.put("stop")
        self.process.join()

    def mark_finished_task(self):
        self.current_task = None
        self.finished_count += 1


class WorkerPool:
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.workers = {}
        self.next_worker_id = 0
        self.finished_queue = multiprocessing.Queue()
        self.queued_messages = []  # TODO: use deque, invent new kinds of message anxiety and panic
        self.read_results_task = None
        self.shutting_down = False
        self.finished_count = 0
        self.shutdown_timeout = 3
        # TODO: worker management lock

    async def start_working(self):
        self._spawn_workers()
        self.read_results_task = asyncio.create_task(self.read_results_forever())

    def _spawn_workers(self):
        for i in range(self.num_workers):
            worker = PoolWorker(worker_id=self.next_worker_id, finished_queue=self.finished_queue)
            worker.start()
            self.workers[self.next_worker_id] = worker
            self.next_worker_id += 1

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
        logger.info('The finished watcher has returned. Pool is shut down')

    async def dispatch_task(self, message):
        for candidate_worker in self.workers.values():
            if not candidate_worker.current_task:
                worker = candidate_worker
                break
        else:
            # TODO: under certain conditions scale up workers
            logger.warning(f'Ran out of available workers, queueing up next task, current queued {len(self.queued_messages)}')
            self.queued_messages.append(message)
            return

        logging.debug(f"Dispatching task to worker {worker.process.pid}: {message}")

        # Put the message in the selected worker's queue, NOTE: this marks the worker as busy
        worker.current_task = message

        # Go ahead and do the put synchronously, because it is just putting it on the queue
        worker.message_queue.put(message)

    async def process_finished(self, worker, message):
        result = message["result"]
        logger.debug(f"Task completed by worker {worker.worker_id}: {result}")

        # Mark the worker as no longer busy
        worker.mark_finished_task()
        self.finished_count += 1

    async def read_results_forever(self):
        """Perpetual task that continuously waits for task completions."""
        loop = asyncio.get_event_loop()
        while True:
            # Wait for a result from the finished queue (blocking)
            # worker_id, finished_message
            message = await loop.run_in_executor(None, self.finished_queue.get)
            worker_id = message["worker"]
            event = message["event"]
            worker = self.workers[worker_id]

            if event == 'ready':
                worker.status = 'ready'

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

            if self.queued_messages and (not self.shutting_down):
                requeue_message = self.queued_messages.pop()
                await self.dispatch_task(requeue_message)
