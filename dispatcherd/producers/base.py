import asyncio

from ..protocols import Producer


class ProducerEvents:
    def __init__(self) -> None:
        self.ready_event = asyncio.Event()
        self.recycle_event = asyncio.Event()


class BaseProducer(Producer):

    def __init__(self) -> None:
        self.events = ProducerEvents()
        self.produced_count = 0
