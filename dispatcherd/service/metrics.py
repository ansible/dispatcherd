import asyncio
import logging
from typing import Any, Generator, Optional

# Metrics library
from prometheus_client import CollectorRegistry, generate_latest

# For production of the metrics
from prometheus_client.core import CounterMetricFamily
from prometheus_client.metrics_core import Metric
from prometheus_client.registry import Collector

from ..protocols import DispatcherMain
from .asyncio_tasks import ensure_fatal

PLAINTEXT_UTF8 = "text/plain; charset=utf-8"
PLAINTEXT_METRICS = "text/plain; version=0.0.4; charset=utf-8"

logger = logging.getLogger(__name__)


class RequestTimeoutError(Exception):
    """Raised when a client does not send data within the configured timeout."""


def metrics_data(dispatcher: DispatcherMain) -> Generator[Metric, Any, Any]:
    """
    Called each time metrics are gathered
    This defines all the metrics collected and gets them from the dispatcher object
    """
    yield CounterMetricFamily(
        'dispatcher_messages_received_total',
        'Number of messages received by dispatchermain',
        value=dispatcher.received_count,
    )
    yield CounterMetricFamily(
        'dispatcher_control_messages_count',
        'Number of control messages received.',
        value=dispatcher.control_count,
    )
    yield CounterMetricFamily(
        'dispatcher_worker_count',
        'Number of workers running.',
        value=len(list(dispatcher.pool.workers)),
    )


class CustomCollector(Collector):
    def __init__(self, dispatcher: DispatcherMain) -> None:
        self.dispatcher = dispatcher

    def collect(self) -> Generator[Metric, Any, Any]:
        for m in metrics_data(self.dispatcher):
            yield m


class CustomHttpServer:
    """Called from DispatcherMetricsServer, but with the registry initialized"""

    def __init__(self, registry: CollectorRegistry, read_timeout: float = 5.0):
        self.registry = registry
        self.read_timeout = read_timeout
        self.server: Optional[asyncio.Server] = None
        self.total_connections = 0
        self.metrics_requests_served = 0
        self.bad_request_count = 0
        self.internal_error_count = 0
        self.not_found_count = 0
        self.client_disconnect_count = 0
        self.request_timeout_count = 0
        self.is_running = False

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Callback passed to asyncio.start_server, called each time a request comes in

        This materializes the metrics and sends it as the response
        """
        addr = writer.get_extra_info('peername')
        logger.info(f"Received connection from {addr}")
        self.total_connections += 1

        try:
            await self._process_request(reader, writer, addr)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Unhandled error while serving metrics for {addr}")
            if not writer.is_closing():
                await self._send_response(
                    writer=writer,
                    status_line="HTTP/1.1 500 Internal Server Error",
                    body="Internal Server Error",
                    addr=addr,
                )
            self.internal_error_count += 1
        finally:
            if not writer.is_closing():
                writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                logger.debug(f"Error while closing writer for {addr}", exc_info=True)

    async def _process_request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        addr: Any,
    ) -> None:
        "Internal version of handle_request that does not handle server errors or closing writer"
        try:
            request_line = await self._readline_with_timeout(reader, addr, "request line")
        except RequestTimeoutError:
            await self._handle_request_timeout(writer, addr)
            return

        if not request_line:
            logger.info(f"Received blank line from {addr}, closing")
            self.client_disconnect_count += 1
            return

        try:
            request_line_str = request_line.decode('utf-8').strip()
        except UnicodeDecodeError:
            logger.warning(f"Invalid UTF-8 metrics request from {addr}")
            self.bad_request_count += 1
            await self._send_response(
                writer=writer,
                status_line="HTTP/1.1 400 Bad Request",
                body="Bad Request",
                addr=addr,
            )
            return

        logger.info(f"Received metrics request: {request_line_str}")

        # Parse the request line (simplified parsing)
        try:
            method, path, _ = request_line_str.split(maxsplit=2)
        except ValueError:
            logger.warning(f"Could not parse metrics request line: {request_line_str}")
            # Respond with 400 Bad Request for malformed request line
            self.bad_request_count += 1
            await self._send_response(
                writer=writer,
                status_line="HTTP/1.1 400 Bad Request",
                body="Bad Request",
                addr=addr,
            )
            return

        # Read headers (and ignore them for now)
        while True:
            try:
                header_line = await self._readline_with_timeout(reader, addr, "header line")
            except RequestTimeoutError:
                await self._handle_request_timeout(writer, addr)
                return

            if header_line == b'':
                logger.warning(f"Client {addr} disconnected before completing headers")
                self.client_disconnect_count += 1
                return
            if header_line in (b'\r\n', b'\n'):
                break

        if method != 'GET' or path != '/metrics':
            self.not_found_count += 1
            await self._send_response(
                writer=writer,
                status_line="HTTP/1.1 404 Not Found",
                body="Not Found",
                addr=addr,
            )
            return

        try:
            metrics_payload = generate_latest(self.registry)
        except Exception:
            # Raising any exceptions would pose problems for the overall task system, so logged
            logger.exception("Error generating metrics")
            self.internal_error_count += 1
            await self._send_response(
                writer=writer,
                status_line="HTTP/1.1 500 Internal Server Error",
                body="Error generating metrics",
                addr=addr,
            )
            return

        body = metrics_payload.decode('utf-8')
        self.metrics_requests_served += 1
        await self._send_response(
            writer,
            "HTTP/1.1 200 OK",
            body,
            addr,
            content_type=PLAINTEXT_METRICS,
        )

    async def _readline_with_timeout(
        self,
        reader: asyncio.StreamReader,
        addr: Any,
        context: str,
    ) -> bytes:
        try:
            return await asyncio.wait_for(reader.readline(), timeout=self.read_timeout)
        except asyncio.TimeoutError as exc:
            logger.warning(f"Timeout while reading {context} from {addr}")
            raise RequestTimeoutError(context) from exc

    async def _handle_request_timeout(self, writer: asyncio.StreamWriter, addr: Any) -> None:
        self.request_timeout_count += 1
        if writer.is_closing():
            return
        await self._send_response(
            writer=writer,
            status_line="HTTP/1.1 408 Request Timeout",
            body="Request Timeout",
            addr=addr,
        )

    async def _send_response(
        self,
        writer: asyncio.StreamWriter,
        status_line: str,
        body: str,
        addr: Any,
        content_type: str = PLAINTEXT_UTF8,
    ) -> None:
        body_bytes = body.encode('utf-8')
        response_headers = f"{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {len(body_bytes)}\r\nConnection: close\r\n\r\n"
        writer.write(response_headers.encode('utf-8') + body_bytes)
        await writer.drain()
        logger.info(f"Sent metrics {status_line} response to {addr}")

    def get_status_data(self) -> dict[str, int]:
        return {
            'connections_total': self.total_connections,
            'metrics_requests_served': self.metrics_requests_served,
            'bad_requests': self.bad_request_count,
            'internal_errors': self.internal_error_count,
            'not_found_responses': self.not_found_count,
            'client_disconnects': self.client_disconnect_count,
            'request_timeouts': self.request_timeout_count,
            'is_running': self.is_running,
        }

    async def start(self, host: str, port: int, ready_event: asyncio.Event) -> None:
        """Runs the server forever."""
        try:
            self.server = await asyncio.start_server(self.handle_request, host, port)
        except Exception as e:
            logger.error(f"Failed to start server on {host}:{port}: {e}")
            ready_event.set()
            self.server = None
            # Potentially re-raise or handle more gracefully if this is critical
            return

        addr = self.server.sockets[0].getsockname()
        logger.info(f'Serving dispatcherd metrics on {addr}')

        # The ready event is useful for testing and any code-level integrations
        try:
            async with self.server:
                ready_event.set()
                self.is_running = True  # used for metrics, a shortcut better than checking tasks
                await self.server.serve_forever()
        finally:
            self.is_running = False

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.debug("Dispatcherd metrics server stopped.")
            self.server = None


class DispatcherMetricsServer:
    def __init__(self, port: int = 8070, host: str = "localhost") -> None:
        self.port = port
        self.host = host
        self.ready_event = asyncio.Event()
        self.http_server: Optional[CustomHttpServer] = None
        self._task: Optional[asyncio.Task[None]] = None

    async def start_server(self, dispatcher: DispatcherMain) -> None:
        """Run Prometheus metrics server forever."""
        registry = CollectorRegistry(auto_describe=True)
        registry.register(CustomCollector(dispatcher))

        # Instantiate CustomHttpServer with the registry
        self.http_server = CustomHttpServer(registry=registry)

        logger.info(f'Starting dispatcherd prometheus server on {self.host}:{self.port} using CustomHttpServer.')

        # Start the CustomHttpServer
        # The start method in CustomHttpServer is an async method that starts the server.
        try:
            await self.http_server.start(host=self.host, port=self.port, ready_event=self.ready_event)
            logger.error('Metrics HTTP server exited unexpectedly')
        except Exception:
            logger.exception("CustomHttpServer failed to start or encountered an error")
            # Depending on desired behavior, might re-raise or handle
        finally:
            # Ensure graceful shutdown if start() completes or raises an exception
            # that's not KeyboardInterrupt (which is handled in CustomHttpServer's main example)
            logger.info("Attempting to stop CustomHttpServer...")
            await self.http_server.stop()  # Assuming stop is robust enough to be called even if start failed partially

    async def start_working(self, dispatcher: DispatcherMain) -> asyncio.Task[None] | None:
        if self._task and not self._task.done():
            return self._task

        self._task = ensure_fatal(asyncio.create_task(self.start_server(dispatcher), name='metrics_server'))
        return self._task

    async def shutdown(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        if self.http_server:
            await self.http_server.stop()

    def get_status_data(self) -> dict[str, Any]:
        http_server_status = self.http_server.get_status_data() if self.http_server else {}
        return {
            'host': self.host,
            'port': self.port,
            'ready': self.ready_event.is_set(),
            'http_server': http_server_status,
        }
