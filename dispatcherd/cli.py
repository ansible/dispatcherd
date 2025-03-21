import argparse
import logging
import os
import sys

import yaml

from . import run_service
from .config import setup
from .factories import get_control_from_settings
from .service import control_tasks

logger = logging.getLogger(__name__)


DEFAULT_CONFIG_FILE = 'dispatcher.yml'


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI entrypoint for dispatcherd, mainly intended for testing.")
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
        default=DEFAULT_CONFIG_FILE,
        help='Path to dispatcherd config.',
    )
    return parser


def setup_from_parser(parser) -> argparse.Namespace:
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stdout)

    logger.debug(f"Configured standard out logging at {args.log_level} level")

    if os.getenv('DISPATCHERD_CONFIG_FILE') and args.config == os.path.abspath(DEFAULT_CONFIG_FILE):
        logger.info(f'Using config from environment variable DISPATCHERD_CONFIG_FILE={os.getenv("DISPATCHERD_CONFIG_FILE")}')
        setup()
    else:
        logger.info(f'Using config from file {args.config}')
        setup(file_path=args.config)
    return args


def standalone() -> None:
    setup_from_parser(get_parser())
    run_service()


def control() -> None:
    parser = get_parser()
    parser.add_argument('command', choices=[cmd for cmd in control_tasks.__all__], help='The control action to run.')
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
    if len(returned) < args.expected_replies:
        logger.error(f'Obtained only {len(returned)} of {args.expected_replies}, exiting with non-zero code')
        sys.exit(1)
