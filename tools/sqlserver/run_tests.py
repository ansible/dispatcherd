"""Run SQL Server integration and shared broker contracts in separate processes."""

import os
import subprocess
import sys

sys.path.insert(0, '/tests/sqlserver')
from database import database_config, provision  # type: ignore[import-not-found]  # noqa: E402

provision()
os.environ['DISPATCHERD_TEST_SQLSERVER_DSN'] = database_config()['connection_string']
os.environ['DISPATCHERD_SUBPROCESS_COVERAGE'] = '0'
options = ['-q', '-o', 'asyncio_mode=strict', '-o', 'asyncio_default_fixture_loop_scope=function']
for arguments in [
    ['--confcutdir=/tests/sqlserver', '/tests/sqlserver'],
    ['-k', 'sqlserver', '/tests/asyncio_tests/integration/brokers/test_broker_contract.py'],
]:
    subprocess.run([sys.executable, '-m', 'pytest', *options, *arguments], check=True)
