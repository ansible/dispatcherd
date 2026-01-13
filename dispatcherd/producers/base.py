import asyncio
import time

from ..protocols import Producer as ProducerProtocol


class ProducerEvents:
    def __init__(self) -> None:
        self.ready_event = asyncio.Event()
        self.recycle_event = asyncio.Event()


class BaseProducer(ProducerProtocol):
    can_recycle: bool = False

    def __init__(self) -> None:
        self.events = ProducerEvents()
        self.produced_count = 0
        self.started_at = time.monotonic()

    def get_status_data(self) -> dict:
        return {
            'produced_count': self.produced_count,
            'uptime_seconds': time.monotonic() - self.started_at,
        }

    def __str__(self) -> str:
        module_name = self.__class__.__module__.rsplit('.', 1)[-1]
        return f'{module_name.replace("_", "-")}-producer'
