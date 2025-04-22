import inspect
from copy import deepcopy
from typing import Any, Iterable, Literal, Optional, Type, get_args, get_origin

from . import producers
from .brokers import get_broker
from .config import LazySettings
from .config import settings as global_settings
from .control import Control
from .protocols import Broker, Producer
from .service import process
from .service.asyncio_tasks import SharedAsyncObjects
from .service.main import DispatcherMain
from .service.pool import WorkerPool
from .worker.task import TaskWorker

"""
Creates objects from settings,
This is kept separate from the settings and the class definitions themselves,
which is to avoid import dependencies.
"""

# ---- Service objects ----


def process_manager_from_settings(settings: LazySettings = global_settings) -> process.ProcessManager:
    cls_name = settings.service.get('process_manager_cls', 'ForkServerManager')
    process_manager_cls = getattr(process, cls_name)
    kwargs = settings.service.get('process_manager_kwargs', {}).copy()
    kwargs['settings'] = settings
    return process_manager_cls(**kwargs)


def pool_from_settings(shared, settings: LazySettings = global_settings) -> WorkerPool:
    kwargs = settings.service.get('pool_kwargs', {}).copy()
    kwargs['process_manager'] = process_manager_from_settings(settings=settings)
    kwargs['shared'] = shared
    return WorkerPool(**kwargs)


def brokers_from_settings(settings: LazySettings = global_settings, shared: Optional[SharedAsyncObjects] = None) -> Iterable[Broker]:
    brokers = []
    for broker_name, broker_kwargs in settings.brokers.items():
        kwargs = broker_kwargs.copy()
        if shared:
            kwargs['shared'] = shared
        brokers.append(get_broker(broker_name, kwargs))
    return brokers


def producers_from_settings(shared: SharedAsyncObjects, settings: LazySettings = global_settings) -> Iterable[Producer]:
    producer_objects = []
    for broker in brokers_from_settings(settings=settings):
        producer = producers.BrokeredProducer(shared=shared, broker=broker)
        producer_objects.append(producer)

    for producer_cls, producer_kwargs in settings.producers.items():
        if producer_kwargs is None:
            create_kwargs = {}
        else:
            create_kwargs = producer_kwargs.copy()
        create_kwargs['shared'] = shared
        producer_objects.append(getattr(producers, producer_cls)(**create_kwargs))

    return producer_objects


def from_settings(settings: LazySettings = global_settings) -> DispatcherMain:
    """
    Returns the main dispatcher object, used for running the background task service.
    You could initialize this yourself, but using the shared settings allows for consistency
    between the service, publisher, and any other interacting processes.
    """
    shared = SharedAsyncObjects()
    producers = producers_from_settings(settings=settings, shared=shared)
    pool = pool_from_settings(settings=settings, shared=shared)
    extra_kwargs = settings.service.get('main_kwargs', {})

    metrics_kwargs = settings.service.get('metrics_kwargs')
    if metrics_kwargs:
        from .service.metrics import DispatcherMetricsServer

        extra_kwargs['metrics'] = DispatcherMetricsServer(**metrics_kwargs)

    return DispatcherMain(producers=producers, pool=pool, shared=shared, **extra_kwargs)


# ---- Publisher objects ----


def _get_publisher_broker_name(publish_broker: Optional[str] = None, settings: LazySettings = global_settings) -> str:
    if publish_broker:
        return publish_broker
    elif len(settings.brokers) == 1:
        return list(settings.brokers.keys())[0]
    elif 'default_broker' in settings.publish:
        return settings.publish['default_broker']
    else:
        raise RuntimeError(f'Could not determine which broker to publish with between options {list(settings.brokers.keys())}')


def get_publisher_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides) -> Broker:
    """
    An asynchronous publisher is the ideal choice for submitting control-and-reply actions.
    This returns an asyncio broker of the default publisher type.

    If channels are specified, these completely replace the channel list from settings.
    For control-and-reply, this will contain only the reply_to channel, to not receive
    unrelated traffic.
    """
    publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)
    return get_broker(publish_broker, settings.brokers[publish_broker], **overrides)


def get_control_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides):
    """Returns a Control instance based on the values in settings"""
    if 'default_control_broker' in settings.publish:
        result_publish_broker = settings.publish['default_control_broker']
    else:
        result_publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)
    broker_options = settings.brokers[result_publish_broker].copy()
    broker_options.update(overrides)
    return Control(result_publish_broker, broker_options)


# ---- Schema generation ----

SERIALIZED_TYPES = (int, str, dict, type(None), tuple, list, float, bool)


def is_valid_annotation(annotation):
    if get_origin(annotation):
        for arg in get_args(annotation):
            if not is_valid_annotation(arg):
                return False
    else:
        if annotation not in SERIALIZED_TYPES:
            return False
    return True


def schema_for_cls(cls: Type) -> dict[str, str]:
    signature = inspect.signature(cls.__init__)
    parameters = signature.parameters
    spec = {}
    for k, p in parameters.items():
        if is_valid_annotation(p.annotation):
            spec[k] = str(p.annotation)
    return spec


def generate_settings_schema(settings: LazySettings = global_settings) -> dict:
    ret = deepcopy(settings.serialize())

    ret['service']['pool_kwargs'] = schema_for_cls(WorkerPool)
    ret['service']['main_kwargs'] = schema_for_cls(DispatcherMain)

    # In-line here so we do not require dependencies for operation
    from .service.metrics import DispatcherMetricsServer

    ret['service']['metrics_kwargs'] = schema_for_cls(DispatcherMetricsServer)
    ret['service']['process_manager_kwargs'] = {}
    pm_classes = (process.ProcessManager, process.ForkServerManager, process.SpawnServerManager)
    for pm_cls in pm_classes:
        ret['service']['process_manager_kwargs'].update(schema_for_cls(pm_cls))
    ret['service']['process_manager_cls'] = str(Literal[tuple(pm_cls.__name__ for pm_cls in pm_classes)])

    ret['worker']['worker_cls'] = str(Any)
    ret['worker']['worker_kwargs'] = schema_for_cls(TaskWorker)

    for broker_name, broker_kwargs in settings.brokers.items():
        broker = get_broker(broker_name, broker_kwargs)
        ret['brokers'][broker_name] = schema_for_cls(type(broker))

    for producer_cls, producer_kwargs in settings.producers.items():
        ret['producers'][producer_cls] = schema_for_cls(getattr(producers, producer_cls))

    ret['publish'] = {'default_broker': 'str'}

    return ret
