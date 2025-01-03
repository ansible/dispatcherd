import argparse
import asyncio
import logging
import os
import sys

import yaml

from dispatcher.main import DispatcherMain

logger = logging.getLogger(__name__)


def standalone() -> None:
    parser = argparse.ArgumentParser(description="CLI entrypoint for dispatcher, mainly intended for testing.")
    parser.add_argument(
        '--log-level',
        type=str,
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Python log level to standard out. If you want to log to file you are in the wrong place.',
    )
    parser.add_argument(
        '--config',
        type=os.path.abspath,
        default='dispatcher.yml',
        help='Path to dispatcher config.',
    )

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stdout)

    logger.debug(f"Configured standard out logging at {args.log_level} level")

    with open(args.config, 'r') as f:
        config_content = f.read()

    config = yaml.safe_load(config_content)

    loop = asyncio.get_event_loop()
    dispatcher = DispatcherMain(config)
    try:
        loop.run_until_complete(dispatcher.main())
    except KeyboardInterrupt:
        logger.info('CLI entry point leaving')
    finally:
        loop.close()
