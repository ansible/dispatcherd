import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prometheus_client import CollectorRegistry

from dispatcherd.service.metrics import CustomHttpServer, DispatcherMetricsServer, RequestTimeoutError

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_registry():
    """Fixture for a mocked CollectorRegistry."""
    return MagicMock(spec=CollectorRegistry)


@pytest.fixture
def server(mock_registry):
    """Fixture for CustomHttpServer instance with a mocked registry."""
    return CustomHttpServer(registry=mock_registry)


@pytest.fixture
def mock_stream_writer():
    """Fixture for a mocked asyncio.StreamWriter."""
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.get_extra_info.return_value = ('127.0.0.1', 12345)  # For peername
    writer.is_closing.return_value = False
    return writer


@pytest.fixture
def mock_stream_reader():
    """Fixture for a mocked asyncio.StreamReader."""
    return AsyncMock(spec=asyncio.StreamReader)


async def test_server_start_stop(server, mock_registry):
    """Test if the server starts and stops correctly."""
    with patch('asyncio.start_server', new_callable=AsyncMock) as mock_start_server:
        # Mock the server object that start_server would return
        mock_actual_server = AsyncMock()
        mock_actual_server.close = MagicMock()
        mock_start_server.return_value = mock_actual_server
        mock_actual_server.sockets = [MagicMock(getsockname=MagicMock(return_value=('127.0.0.1', 8080)))]

        start_task = asyncio.create_task(server.start('127.0.0.1', 8080, ready_event=asyncio.Event()))

        # Give the server a moment to start up
        await asyncio.sleep(0.01)

        mock_start_server.assert_called_once_with(server.handle_request, '127.0.0.1', 8080)

        # Stop the server
        await server.stop()

        mock_actual_server.close.assert_called_once()
        mock_actual_server.wait_closed.assert_called_once()

        # We count on task being done, or else might hang the test run
        assert start_task.done()


@patch('dispatcherd.service.metrics.generate_latest')
async def test_handle_request_metrics_success(mock_generate_latest, server, mock_registry, mock_stream_reader, mock_stream_writer):
    """Test handling of GET /metrics request successfully."""
    mock_generate_latest.return_value = b"test_metric_data"
    mock_stream_reader.readline.side_effect = [b"GET /metrics HTTP/1.1\r\n", b"Host: localhost\r\n", b"\r\n"]

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    mock_generate_latest.assert_called_once_with(mock_registry)

    # Check what was written to the stream
    # We need to capture all calls to write and join them
    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)

    assert b"HTTP/1.1 200 OK" in written_data
    assert b"Content-Type: text/plain; version=0.0.4; charset=utf-8" in written_data
    assert b"Content-Length: 16" in written_data  # len(b"test_metric_data")
    assert b"\r\n\r\ntest_metric_data" in written_data

    mock_stream_writer.close.assert_called_once()
    mock_stream_writer.wait_closed.assert_called_once()

    status = server.get_status_data()
    assert status['metrics_requests_served'] == 1
    assert status['connections_total'] == 1
    assert status['internal_errors'] == 0


async def test_handle_request_bad_request(server, mock_stream_reader, mock_stream_writer):
    """Test handling of a malformed request line (400)."""
    mock_stream_reader.readline.side_effect = [
        b"INVALID_REQUEST_LINE\r\n",  # Malformed request
    ]

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)

    assert b"HTTP/1.1 400 Bad Request" in written_data

    mock_stream_writer.close.assert_called_once()
    mock_stream_writer.wait_closed.assert_called_once()

    status = server.get_status_data()
    assert status['bad_requests'] == 1
    assert status['connections_total'] == 1


@patch('dispatcherd.service.metrics.generate_latest')
async def test_handle_request_metrics_exception(mock_generate_latest, server, mock_registry, mock_stream_reader, mock_stream_writer):
    """Test handling of GET /metrics when generate_latest raises an exception."""
    mock_generate_latest.side_effect = Exception("Metrics generation error")
    mock_stream_reader.readline.side_effect = [b"GET /metrics HTTP/1.1\r\n", b"Host: localhost\r\n", b"\r\n"]

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    mock_generate_latest.assert_called_once_with(mock_registry)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)

    assert b"HTTP/1.1 500 Internal Server Error" in written_data
    assert b"Content-Type: text/plain; charset=utf-8" in written_data
    assert b"Error generating metrics" in written_data  # Check for specific error body

    mock_stream_writer.close.assert_called_once()
    mock_stream_writer.wait_closed.assert_called_once()

    status = server.get_status_data()
    assert status['internal_errors'] == 1
    assert status['metrics_requests_served'] == 0


async def test_handle_request_not_found(server, mock_stream_reader, mock_stream_writer):
    """Test handling of an unsupported HTTP method."""
    mock_stream_reader.readline.side_effect = [b"POST /metrics HTTP/1.1\r\n", b"Host: localhost\r\n", b"\r\n"]

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)
    assert b"HTTP/1.1 404 Not Found" in written_data

    mock_stream_writer.close.assert_called_once()
    mock_stream_writer.wait_closed.assert_called_once()

    status = server.get_status_data()
    assert status['not_found_responses'] == 1
    assert status['connections_total'] == 1


async def test_handle_request_path_not_found(server, mock_stream_reader, mock_stream_writer):
    """GET requests to paths other than /metrics should return 404."""
    mock_stream_reader.readline.side_effect = [b"GET /not-metrics HTTP/1.1\r\n", b"Host: localhost\r\n", b"\r\n"]

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)
    assert b"HTTP/1.1 404 Not Found" in written_data

    status = server.get_status_data()
    assert status['not_found_responses'] == 1


async def test_handle_request_request_line_timeout(server, mock_stream_writer):
    """Request line read timeouts should return 408."""
    server._readline_with_timeout = AsyncMock(side_effect=RequestTimeoutError("request line"))

    await server.handle_request(AsyncMock(), mock_stream_writer)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)
    assert b"HTTP/1.1 408 Request Timeout" in written_data

    status = server.get_status_data()
    assert status['request_timeouts'] == 1


async def test_handle_request_header_timeout(server, mock_stream_writer):
    """Header read timeouts should also return 408."""
    server._readline_with_timeout = AsyncMock(side_effect=[b"GET /metrics HTTP/1.1\r\n", RequestTimeoutError("header line")])

    await server.handle_request(AsyncMock(), mock_stream_writer)

    written_data = b"".join(call.args[0] for call in mock_stream_writer.write.call_args_list)
    assert b"HTTP/1.1 408 Request Timeout" in written_data

    status = server.get_status_data()
    assert status['request_timeouts'] == 1


async def test_handle_request_empty_request_line(server, mock_stream_reader, mock_stream_writer):
    """Test handling of an empty request line from the client."""
    mock_stream_reader.readline.return_value = b""  # Empty line, simulating client disconnect

    await server.handle_request(mock_stream_reader, mock_stream_writer)

    # Ensure writer was closed without attempting to write a response body
    mock_stream_writer.write.assert_not_called()
    mock_stream_writer.close.assert_called_once()
    mock_stream_writer.wait_closed.assert_called_once()

    status = server.get_status_data()
    assert status['client_disconnects'] == 1
    assert status['connections_total'] == 1


async def test_server_start_exception(server):
    """Test server start failure."""
    with patch('asyncio.start_server', new_callable=AsyncMock) as mock_start_server:
        mock_start_server.side_effect = OSError("Address already in use")

        # We expect start() to log an error and return, not raise the OSError directly
        await server.start('127.0.0.1', 8080, ready_event=asyncio.Event())

        # Check that the server object was not set or was reset
        assert server.server is None
        mock_start_server.assert_called_once()


async def test_stop_idempotency(server):
    """Test that calling stop on a not-started/already-stopped server is safe."""
    # Call stop without starting
    await server.stop()
    # No error should occur

    # Simulate a started and then stopped server
    fake_server = MagicMock()
    fake_server.wait_closed = AsyncMock()
    fake_server.is_serving.return_value = False  # Indicate it's already closed
    server.server = fake_server
    await server.stop()
    fake_server.close.assert_called_once()  # Should still try to close
    fake_server.wait_closed.assert_called_once()
    assert server.server is None

    # Reset server.server to None as if it was never started or fully stopped
    server.server = None
    await server.stop()  # Should do nothing and not error

    # Test with server object that is None (initial state)
    server_initial_state = CustomHttpServer(registry=MagicMock())
    await server_initial_state.stop()  # should not raise any error.
    assert server_initial_state.server is None


async def test_dispatcher_metrics_server_status_data():
    """DispatcherMetricsServer exposes host/port and readiness information."""
    metrics_server = DispatcherMetricsServer(port=9000, host='0.0.0.0')

    status = metrics_server.get_status_data()

    assert status['host'] == '0.0.0.0'
    assert status['port'] == 9000
    assert status['ready'] is False
    assert status['http_server'] == {}
