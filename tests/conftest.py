import pytest

from dispatcher.main import DispatcherMain


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel2']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


@pytest.fixture
def pg_dispatcher():
    return DispatcherMain({"producers": {"brokers": {"pg_notify": {"conninfo": CONNECTION_STRING}, "channels": CHANNELS}}, "pool": {"max_workers": 3}})
