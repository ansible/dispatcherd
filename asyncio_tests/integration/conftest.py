import sys

import pytest


@pytest.fixture
def python311():
    if sys.version_info < (3, 11):
        pytest.skip("test requires python 3.11")
