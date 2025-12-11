import asyncio
import logging

import pytest

from dispatcherd.service import control_tasks


@pytest.fixture
def dispatcherd_logger():
    logger = logging.getLogger('dispatcherd')
    previous_level = logger.level
    try:
        yield logger
    finally:
        logger.setLevel(previous_level)


def test_set_log_level_updates_dispatcherd_logger(dispatcherd_logger):
    dispatcherd_logger.setLevel(logging.INFO)
    result = asyncio.run(control_tasks.set_log_level(dispatcher=None, data={'level': 'debug'}))
    assert dispatcherd_logger.level == logging.DEBUG
    assert result == {'logger': 'dispatcherd', 'level': 'DEBUG', 'previous_level': 'INFO'}


def test_set_log_level_rejects_unknown_level(dispatcherd_logger):
    dispatcherd_logger.setLevel(logging.WARNING)
    result = asyncio.run(control_tasks.set_log_level(dispatcher=None, data={'level': 'bogus'}))
    assert result == {'error': "Unknown log level 'bogus'."}
    assert dispatcherd_logger.level == logging.WARNING


def test_set_log_level_bad_type(dispatcherd_logger):
    dispatcherd_logger.setLevel(logging.ERROR)
    # container type
    result = asyncio.run(control_tasks.set_log_level(dispatcher=None, data={'level': [True]}))
    assert result == {'error': 'Log level must be provided as a string or int via the "level" key.'}
    assert dispatcherd_logger.level == logging.ERROR
    # bad boolean
    result = asyncio.run(control_tasks.set_log_level(dispatcher=None, data={'level': True}))
    assert result == {'error': 'Log level must be provided as a string or int via the "level" key.'}
    assert dispatcherd_logger.level == logging.ERROR


def test_set_log_level_integer(dispatcherd_logger):
    dispatcherd_logger.setLevel(logging.ERROR)
    result = asyncio.run(control_tasks.set_log_level(dispatcher=None, data={'level': 30}))
    assert dispatcherd_logger.level == logging.WARNING
    assert result == {'logger': 'dispatcherd', 'level': 'WARNING', 'previous_level': 'ERROR'}
