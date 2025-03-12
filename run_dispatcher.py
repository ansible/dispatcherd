#!/usr/bin/env python3

import json
import logging
import sys

from dispatcher import run_service
from dispatcher.factories import get_publisher_from_settings, get_control_from_settings
from dispatcher.utils import MODULE_METHOD_DELIMITER
from dispatcher.config import setup

from time import sleep

from tests.data.methods import sleep_function, sleep_discard, task_has_timeout, hello_world_binder


# Setup the global config from the settings file shared with the service
setup(file_path='dispatcher.yml')

broker = get_publisher_from_settings()

def main():
    run_service()

if __name__ == "__main__":
    logging.basicConfig(level='DEBUG', stream=sys.stdout)
    main()
