import asyncio


class ProducerEvents:
    def __init__(self):
        self.ready_event = asyncio.Event()


class BaseProducer:
    pass
