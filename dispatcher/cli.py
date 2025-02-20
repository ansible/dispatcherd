import argparse
import logging
import os
import sys

from dispatcher import run_service
from dispatcher.config import setup

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

    setup(file_path=args.config)

    run_service()
