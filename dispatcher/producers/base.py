import asyncio


class ProducerEvents:
    def __init__(self):
        self.ready_event = asyncio.Event()


class BaseProducer:

    def __init__(self) -> None:
        self.events = ProducerEvents()
        self.produced_count = 0

    async def start_producing(self, dispatcher) -> None: ...

    async def shutdown(self): ...
