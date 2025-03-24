import asyncio
import logging

from dispatcherd.factories import from_settings

logger = logging.getLogger(__name__)


def run_service() -> None:
    """
    Runs dispatcherd task service (runs tasks due to messages from brokers and other local producers)
    Before calling this you need to configure by calling dispatcherd.config.setup
    """
    loop = asyncio.get_event_loop()
    dispatcher = from_settings()
    try:
        loop.run_until_complete(dispatcher.main())
    except KeyboardInterrupt:
        logger.info('Dispatcherd stopped by KeyboardInterrupt')
    finally:
        loop.close()
