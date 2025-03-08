import argparse
import logging
import os
import sys

import yaml

from . import run_service
from .config import setup
from .factories import get_control_from_settings

logger = logging.getLogger(__name__)


def get_parser() -> argparse.ArgumentParser:
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
    return parser


def setup_from_parser(parser) -> argparse.Namespace:
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stdout)

    logger.debug(f"Configured standard out logging at {args.log_level} level")

    setup(file_path=args.config)
    return args


def standalone() -> None:
    setup_from_parser(get_parser())
    run_service()


def control() -> None:
    parser = get_parser()
    parser.add_argument('command', help='The control action to run.')
    parser.add_argument(
        '--task',
        type=str,
        default=None,
        help='Task name to filter on.',
    )
    parser.add_argument(
        '--uuid',
        type=str,
        default=None,
        help='Task uuid to filter on.',
    )
    parser.add_argument(
        '--expected-replies',
        type=int,
        default=1,
        help='Expected number of replies, in case you are have more than 1 service running.',
    )
    args = setup_from_parser(parser)
    data = {}
    for field in ('task', 'uuid'):
        val = getattr(args, field)
        if val:
            data[field] = val
    ctl = get_control_from_settings()
    returned = ctl.control_with_reply(args.command, data=data, expected_replies=args.expected_replies)
    print(yaml.dump(returned, default_flow_style=False))
