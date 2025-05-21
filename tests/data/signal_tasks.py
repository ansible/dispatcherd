import signal
import time

from dispatcherd.worker.exceptions import DispatcherExit


def dispatcher_exit_task():
    """Raise DispatcherExit immediately."""
    raise DispatcherExit("<exit>")


def wait_forever():
    """Block until external signal arrives."""
    while True:
        time.sleep(0.1)


def sigint_handler_task():
    """Install a SIGINT handler that requests worker shutdown."""

    def _handle_sigint(signum, frame):
        raise DispatcherExit("<sigint>")

    signal.signal(signal.SIGINT, _handle_sigint)
    while True:
        time.sleep(0.1)
