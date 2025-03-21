import asyncio
import contextlib
import logging
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def adispatcher_service(config: dict) -> AsyncGenerator[DispatcherMain, Any]:
    dispatcher = None
    try:
        settings = DispatcherSettings(config)
        dispatcher = from_settings(settings=settings)  # type: ignore[arg-type]

        await asyncio.wait_for(dispatcher.connect_signals(), timeout=1)
        await asyncio.wait_for(dispatcher.start_working(), timeout=1)
        await asyncio.wait_for(dispatcher.wait_for_producers_ready(), timeout=1)
        await asyncio.wait_for(dispatcher.pool.events.workers_ready.wait(), timeout=1)

        assert dispatcher.pool.finished_count == 0  # sanity
        assert dispatcher.control_count == 0

        yield dispatcher
    finally:
        if dispatcher:
            try:
                await dispatcher.shutdown()
                await dispatcher.cancel_tasks()
            except Exception:
                logger.exception('shutdown had error')
