import asyncio

import pytest

from dispatcher.config import temporary_settings
from dispatcher import run_service


WORKER_ONLY_CONFIG = {
    "version": 2,
    "pool": {
        "pool_kwargs": {
            "min_workers": 1,
            "max_workers": 6
        }
    }
}

def test_run_after_another_loop():
    loop = asyncio.new_event_loop()
    loop.close()

    with temporary_settings(WORKER_ONLY_CONFIG):
        run_service()

