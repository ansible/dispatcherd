import asyncio
import contextlib
import logging
import multiprocessing
import sys
import traceback
from multiprocessing.context import BaseContext
from types import ModuleType
from typing import Union

from .asyncio import adispatcher_service

logger = logging.getLogger(__name__)


class CommunicationItems:
    """Various things used for communication between the parent process and the subprocess service

    When using the dispatcher_service context manager, this is yielded to be used by tests.
    Checking q_out can allow waiting for dispatcher events in synchronous code, like clearing of work queue.

    This will be passed in the call to the subprocess.
    """

    def __init__(self, main_events: tuple[str], pool_events: tuple[str], context: Union[BaseContext, ModuleType]) -> None:
        self.q_in: multiprocessing.Queue = context.Queue()
        self.q_out: multiprocessing.Queue = context.Queue()
        self.main_events = main_events
        self.pool_events = pool_events


async def asyncio_target(config: dict, comms: CommunicationItems) -> None:
    """Replaces the DispatcherMain.main method, similar to how most asyncio tests work"""
    loop = asyncio.get_event_loop()
    async with adispatcher_service(config) as dispatcher:
        comms.q_out.put('ready')

        events: dict[str, asyncio.Event] = {}
        events['exit_event'] = dispatcher.shared.exit_event
        for event_name in comms.pool_events:
            events[event_name] = getattr(dispatcher.pool.events, event_name)

        event_tasks: dict[str, asyncio.Task] = {}
        for event_name, event in events.items():
            event_tasks[event_name] = asyncio.create_task(event.wait(), name=f'waiting_for_{event_name}')

        new_message_task = None

        while True:
            if new_message_task is None:
                new_message_task = loop.run_in_executor(None, comms.q_in.get)

            all_tasks = list(event_tasks.values()) + [new_message_task]
            await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

            # Update our parent process with any events they requested from us
            for event_name, event in events.items():
                if event.is_set():
                    comms.q_out.put(event_name)
                event.clear()
                event_tasks[event_name] = asyncio.create_task(event.wait())

            # If no no instructions came from parent then work is done, continue loop
            if not new_message_task.done():
                continue

            message = new_message_task.result()
            new_message_task = None

            if message == 'stop':
                print('shutting down pool server')
                for event in events.values():
                    event.set()  # close out other tasks
                # NOTE: the context manager calls .shutdown
                break
            else:
                eval(message)


def subprocess_main(config, comms):
    """The subprocess (synchronous) target for the testing dispatcherd service"""
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(asyncio_target(config, comms))
    except Exception:
        # The main process is very likely waiting for message of an event
        # and exceptions may not automatically halt the test, so give a value
        comms.q_out.put('error')
        stack_trace = traceback.format_exc()
        comms.q_out.put(stack_trace)
        raise
    finally:
        loop.close()


@contextlib.contextmanager
def dispatcher_service(config, main_events=(), pool_events=()):
    """Testing utility to run a dispatcherd service as a subprocess

    Note this is likely to have problems if mixed with code running asyncio loops.
    It is mainly intended to be called from synchronous python.
    """
    ctx = multiprocessing.get_context('fork')
    comms = CommunicationItems(main_events=main_events, pool_events=pool_events, context=ctx)
    process = multiprocessing.Process(target=subprocess_main, args=(config, comms))
    try:
        process.start()
        ready_msg = comms.q_out.get()
        if ready_msg != 'ready':
            if ready_msg == 'error':
                tb = comms.q_out.get()
                print(tb)
            raise RuntimeError(f'Never got "ready" message from server, got {ready_msg}')
        yield comms
    finally:
        comms.q_in.put('stop')
        process.join(timeout=1)
        if process.is_alive():
            print(f"Process {process.pid} did not exit after stop message, sending SIGTERM")
            process.terminate()  # SIGTERM
            process.join(timeout=1)

            if process.is_alive():
                print(f"Process {process.pid} still alive after SIGTERM, sending SIGKILL")
                process.kill()
                process.join(timeout=1)

        comms.q_in.close()
        comms.q_out.close()
        sys.stdout.flush()
        sys.stderr.flush()
