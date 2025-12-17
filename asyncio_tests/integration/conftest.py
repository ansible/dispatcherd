import sys

import pytest


@pytest.fixture
def python312():
    if sys.version_info < (3, 12):
        pytest.skip("test requires python 3.12")
