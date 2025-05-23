import logging
from typing import Any, Generator
import asyncio # Required for CustomHttpServer's start method

# Metrics library
from prometheus_client import CollectorRegistry 
# Removed make_asgi_app

# For production of the metrics
from prometheus_client.core import CounterMetricFamily
from prometheus_client.metrics_core import Metric
from prometheus_client.registry import Collector

# Import CustomHttpServer
from .custom_http_server import CustomHttpServer 
# Removed uvicorn imports

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


class DispatcherMetricsServer:
    def __init__(self, port: int = 8070, log_level: str = 'info', host: str = "localhost") -> None:
        self.port = port
        self.log_level = log_level
        self.host = host

    async def start_server(self, dispatcher: DispatcherMain) -> None:
        """Run Prometheus metrics ASGI app forever."""
        registry = CollectorRegistry(auto_describe=True)
        registry.register(CustomCollector(dispatcher))

        # Instantiate CustomHttpServer with the registry
        # CustomHttpServer's logging is configured internally, so self.log_level is not directly passed here.
        # If CustomHttpServer were to be configured with a log level, its __init__ or start method would need to accept it.
        # For now, we rely on CustomHttpServer's own logging setup.
        http_server = CustomHttpServer(registry=registry)

        logger.info(f'Starting dispatcherd prometheus server on {self.host}:{self.port} using CustomHttpServer. Log level ({self.log_level}) from config is noted but CustomHttpServer uses its own logging config.')
        
        # Start the CustomHttpServer
        # The start method in CustomHttpServer is an async method that runs the server.
        try:
            await http_server.start(host=self.host, port=self.port)
        except Exception as e:
            logger.error(f"CustomHttpServer failed to start or encountered an error: {e}")
            # Depending on desired behavior, might re-raise or handle
        finally:
            # Ensure graceful shutdown if start() completes or raises an exception
            # that's not KeyboardInterrupt (which is handled in CustomHttpServer's main example)
            logger.info("Attempting to stop CustomHttpServer...")
            await http_server.stop() # Assuming stop is robust enough to be called even if start failed partially
