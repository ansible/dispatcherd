import asyncio
from types import SimpleNamespace


class BaseProducer:
    def _create_events(self):
        return SimpleNamespace(ready_event=asyncio.Event())
