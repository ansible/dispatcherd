import argparse
import asyncio
import logging
import sys
from datetime import timedelta

from dispatcher.main import DispatcherMain

logger = logging.getLogger(__name__)


# TODO: obviously stop hard-coding this
CELERYBEAT_SCHEDULE = {
    'lambda: __import__("time").sleep(1)': {'schedule': timedelta(seconds=3)},
    'lambda: __import__("time").sleep(2)': {'schedule': timedelta(seconds=3)},
}


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel2']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


def standalone():
    parser = argparse.ArgumentParser(description="CLI entrypoint for dispatcher, mainly intended for testing.")
    parser.add_argument(
        '--log-level',
        type=str,
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Python log level to standard out. If you want to log to file you are in the wrong place.',
    )

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stdout)

    logging.debug(f"Configured standard out logging at {args.log_level} level")

    config = {
        "producers": {"brokers": {"pg_notify": {"conninfo": CONNECTION_STRING}, "channels": CHANNELS}, "scheduled": CELERYBEAT_SCHEDULE},
        "pool": {"max_workers": 3},
    }

    loop = asyncio.get_event_loop()
    dispatcher = DispatcherMain(config)
    try:
        loop.run_until_complete(dispatcher.main())
        # asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('CLI entry point leaving')
    finally:
        loop.close()
