import argparse
import inspect
import logging
import os
import sys
import textwrap

import yaml

from . import run_service
from .config import setup
from .factories import get_control_from_settings
from .service import control_tasks

logger = logging.getLogger(__name__)


DEFAULT_CONFIG_FILE = 'dispatcher.yml'
CONTROL_ARGS_MARKER = 'Control Args:'
CONTROL_ARG_SCHEMAS = {}
TYPE_CASTERS = {'str': str, 'int': int, 'float': float}


def _base_cli_parent() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        '--log-level',
        type=str,
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Python log level to standard out for the CLI command itself. If you want to log to file you are in the wrong place.',
    )
    parent.add_argument(
        '--config',
        type=os.path.abspath,
        default=DEFAULT_CONFIG_FILE,
        help='Path to dispatcherd config.',
    )
    return parent


def _control_common_parent() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        '--expected-replies',
        type=int,
        default=1,
        help='Expected number of replies, in case you have more than 1 service running.',
    )
    return parent


def _extract_control_args_yaml(doc: str) -> str | None:
    marker_index = doc.find(CONTROL_ARGS_MARKER)
    if marker_index == -1:
        return None
    block_lines = []
    after = doc[marker_index + len(CONTROL_ARGS_MARKER) :].splitlines()  # NOQA: E203 black and flake8 conflict
    collecting = False
    for line in after:
        if not collecting:
            if not line.strip():
                continue
            if line.startswith((' ', '\t')):
                collecting = True
            else:
                break
        if collecting:
            if line.startswith((' ', '\t')):
                block_lines.append(line)
            elif not line.strip():
                break
            else:
                break
    if not block_lines:
        return None
    return textwrap.dedent('\n'.join(block_lines))


def _load_control_arg_schema(func) -> dict:
    doc = inspect.getdoc(func) or ''
    yaml_blob = _extract_control_args_yaml(doc)
    if not yaml_blob:
        return {}
    data = yaml.safe_load(yaml_blob)
    if not isinstance(data, dict):
        return {}
    return data


for _command in control_tasks.__all__:
    CONTROL_ARG_SCHEMAS[_command] = _load_control_arg_schema(getattr(control_tasks, _command, lambda: None))


def get_control_arg_schemas() -> dict[str, dict]:
    """Expose control argument schema for testing."""
    return CONTROL_ARG_SCHEMAS


def _register_control_arguments(parser: argparse.ArgumentParser, schema: dict | None) -> None:
    if not schema:
        return
    for arg_name, metadata in schema.items():
        option = f'--{arg_name.replace("_", "-")}'
        arg_kwargs: dict = {'dest': arg_name, 'help': metadata.get('help')}
        arg_type = metadata.get('type', 'str')
        if arg_type == 'bool':
            arg_kwargs['action'] = 'store_true'
            arg_kwargs['default'] = metadata.get('default', False)
        else:
            arg_kwargs['type'] = TYPE_CASTERS.get(arg_type, str)
            arg_kwargs['default'] = metadata.get('default')
            if metadata.get('required'):
                arg_kwargs['required'] = True
        if 'choices' in metadata:
            arg_kwargs['choices'] = metadata['choices']
        parser.add_argument(option, **arg_kwargs)


def _build_command_data_from_args(args: argparse.Namespace, command: str) -> dict:
    schema = CONTROL_ARG_SCHEMAS.get(command, {})
    data = {}
    for field in schema.keys():
        value = getattr(args, field, None)
        if value is not None:
            data[field] = value
    return data


def get_parser(extra_parents: list[argparse.ArgumentParser] | None = None) -> argparse.ArgumentParser:
    parents = [_base_cli_parent()]
    if extra_parents:
        parents.extend(extra_parents)
    parser = argparse.ArgumentParser(description="CLI entrypoint for dispatcherd, mainly intended for testing.", parents=parents)
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
    parser = get_parser(extra_parents=[_control_common_parent()])
    subparsers = parser.add_subparsers(dest='command', metavar='command')
    subparsers.required = True
    shared_command_parents = [_base_cli_parent(), _control_common_parent()]
    for command in control_tasks.__all__:
        func = getattr(control_tasks, command)
        doc = inspect.getdoc(func) or ''
        summary = doc.splitlines()[0] if doc else None
        command_parser = subparsers.add_parser(command, help=summary, description=doc, parents=shared_command_parents)
        _register_control_arguments(command_parser, CONTROL_ARG_SCHEMAS.get(command))
    args = setup_from_parser(parser)
    data = _build_command_data_from_args(args, args.command)
    ctl = get_control_from_settings()
    returned = ctl.control_with_reply(args.command, data=data, expected_replies=args.expected_replies)
    print(yaml.dump(returned, default_flow_style=False))
    if len(returned) < args.expected_replies:
        logger.error(f'Obtained only {len(returned)} of {args.expected_replies}, exiting with non-zero code')
        sys.exit(1)
