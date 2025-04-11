import logging
from typing import Any, Generator

# Metrics library
from prometheus_client import CollectorRegistry, make_asgi_app

# For production of the metrics
from prometheus_client.core import CounterMetricFamily
from prometheus_client.metrics_core import Metric
from prometheus_client.registry import Collector

# General ASGI python web server, interfaces with prometheus-client lib by the ASGI standard
from uvicorn.config import Config
from uvicorn.server import Server

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

        app = make_asgi_app(registry=registry)

        # Explanation:
        # loop should default to asyncio anyway with uvicorn
        # lifespan events are apart of ASGI 3.0 protocol, but prometheus client does not implement them
        #   so it makes sense for that to be unconditionally off
        config = Config(app=app, host=self.host, port=self.port, log_level=self.log_level, loop="asyncio", lifespan="off")
        server = Server(config)

        # Host and port are printed in Uvicorn logging
        logger.info(f'Starting dispatcherd prometheus server log_level={self.log_level}')
        await server.serve()
