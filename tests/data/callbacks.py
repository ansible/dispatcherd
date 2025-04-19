import logging
from pathlib import Path

from dispatcherd.worker.task import TaskWorker


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


log_path = Path(__file__).resolve().parents[2] / "logs" / "app.log"
logger.addHandler(logging.FileHandler(log_path, mode="a"))


class TestWorker(TaskWorker):

    def on_start(self) -> None:
        print('on_start')
        logger.info('on_start')

    def on_shutdown(self) -> None:
        print('on_shutdown')
        logger.info('on_shutdown')

    def pre_task(self, message) -> None:
        print(f'pre_task: {message}')
        logger.info(f'pre_task: {message}')

    def post_task(self, result) -> None:
        print(f'post_task: {result}')
        logger.info(f'post_task: {result}')

    def on_idle(self) -> None:
        print('on_idle')
        logger.info('on_idle')
