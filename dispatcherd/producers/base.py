import asyncio

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

    def get_status_data(self) -> dict:
        return {'produced_count': self.produced_count}
