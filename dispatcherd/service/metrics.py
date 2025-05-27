import logging
from typing import Any, Generator
import asyncio

# Metrics library
from prometheus_client import CollectorRegistry

# For production of the metrics
from prometheus_client.core import CounterMetricFamily
from prometheus_client.metrics_core import Metric
from prometheus_client.registry import Collector
from prometheus_client import generate_latest, CollectorRegistry

from ..protocols import DispatcherMain

logger = logging.getLogger(__name__)


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
    def __init__(self, registry: CollectorRegistry):
        self.server = None
        self.registry = registry

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info(f"Received connection from {addr}")

        request_line = await reader.readline()
        if not request_line:
            logger.info(f"Received blank line from {addr}, closing")
            writer.close()
            await writer.wait_closed()
            return

        request_line_str = request_line.decode('utf-8').strip()
        logger.info(f"Request: {request_line_str}")

        # Parse the request line (simplified parsing)
        try:
            method, _, _ = request_line_str.split()
        except ValueError:
            logger.warning(f"Could not parse request line: {request_line_str}")
            # Respond with 400 Bad Request for malformed request line
            response_headers = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            writer.write(response_headers.encode('utf-8'))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        # Read headers (and ignore them for now)
        while True:
            header_line = await reader.readline()
            if header_line == b'\r\n':
                break

        if method == 'GET':
            try:
                metrics_data = generate_latest(self.registry)
                body = metrics_data.decode('utf-8')
                status_line = "HTTP/1.1 200 OK"
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            except Exception as e:
                logger.error(f"Error generating metrics: {e}")
                body = "Error generating metrics"
                status_line = "HTTP/1.1 500 Internal Server Error"
                content_type = "text/plain; charset=utf-8"
        else:
            # For any other path or method, respond with 404 Not Found
            body = "Not Found"
            status_line = "HTTP/1.1 404 Not Found"
            content_type = "text/plain; charset=utf-8"

        response_headers = f"{status_line}\r\n" \
                           f"Content-Type: {content_type}\r\n" \
                           f"Content-Length: {len(body.encode('utf-8'))}\r\n" \
                           "Connection: close\r\n\r\n"
        
        response = response_headers + body

        writer.write(response.encode('utf-8'))
        await writer.drain()
        
        logger.info(f"Sent {status_line} response to {addr}")
        writer.close()
        await writer.wait_closed()

    async def start(self, host: str, port: int):
        # The registry is now passed in __init__
        try:
            self.server = await asyncio.start_server(self.handle_request, host, port)
        except Exception as e:
            logger.error(f"Failed to start server on {host}:{port}: {e}")
            # Potentially re-raise or handle more gracefully if this is critical
            return

        addr = self.server.sockets[0].getsockname()
        logger.info(f'Serving dispatcherd metrics on {addr}')

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Dispatcherd metrics server stopped.")


class DispatcherMetricsServer:
    def __init__(self, port: int = 8070, host: str = "localhost") -> None:
        self.port = port
        self.host = host

    async def start_server(self, dispatcher: DispatcherMain) -> None:
        """Run Prometheus metrics forever."""
        registry = CollectorRegistry(auto_describe=True)
        registry.register(CustomCollector(dispatcher))

        # Instantiate CustomHttpServer with the registry
        # CustomHttpServer's logging is configured internally, so self.log_level is not directly passed here.
        # If CustomHttpServer were to be configured with a log level, its __init__ or start method would need to accept it.
        # For now, we rely on CustomHttpServer's own logging setup.
        http_server = CustomHttpServer(registry=registry)

        logger.info(f'Starting dispatcherd prometheus server on {self.host}:{self.port} using CustomHttpServer.')

        # Start the CustomHttpServer
        # The start method in CustomHttpServer is an async method that starts the server.
        try:
            await http_server.start(host=self.host, port=self.port)
            logger.error('Metrics HTTP server exited unexpectedly')
        except Exception as e:
            logger.error(f"CustomHttpServer failed to start or encountered an error: {e}")
            # Depending on desired behavior, might re-raise or handle
        finally:
            # Ensure graceful shutdown if start() completes or raises an exception
            # that's not KeyboardInterrupt (which is handled in CustomHttpServer's main example)
            logger.info("Attempting to stop CustomHttpServer...")
            await http_server.stop() # Assuming stop is robust enough to be called even if start failed partially
