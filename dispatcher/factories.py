import importlib
from types import ModuleType
from typing import Iterable, Optional

from dispatcher import producers
from dispatcher.brokers.base import BaseBroker
from dispatcher.config import LazySettings
from dispatcher.config import settings as global_settings
from dispatcher.main import DispatcherMain

"""
Creates objects from settings,
This is kept separate from the settings and the class definitions themselves,
which is to avoid import dependencies.
"""

# ---- Service objects ----


def get_broker_module(broker_name) -> ModuleType:
    "Static method to alias import_module so we use a consistent import path"
    return importlib.import_module(f'dispatcher.brokers.{broker_name}')


def get_async_broker(broker_name: str, broker_config: dict, **overrides) -> BaseBroker:
    """
    Given the name of the broker in the settings, and the data under that entry in settings,
    return the asyncio broker object.
    """
    broker_module = get_broker_module(broker_name)
    kwargs = broker_config.copy()
    kwargs.update(overrides)
    return broker_module.AsyncBroker(**kwargs)


def producers_from_settings(settings: LazySettings = global_settings) -> Iterable[producers.BaseProducer]:
    producer_objects = []
    for broker_name, broker_kwargs in settings.brokers.items():
        broker = get_async_broker(broker_name, broker_kwargs)
        producer = producers.BrokeredProducer(broker=broker)
        producer_objects.append(producer)

    for producer_cls, producer_kwargs in settings.producers.items():
        producer_objects.append(getattr(producers, producer_cls)(**producer_kwargs))

    return producer_objects


def from_settings(settings: LazySettings = global_settings) -> DispatcherMain:
    """
    Returns the main dispatcher object, used for running the background task service.
    You could initialize this yourself, but using the shared settings allows for consistency
    between the service, publisher, and any other interacting processes.
    """
    producers = producers_from_settings(settings=settings)
    return DispatcherMain(settings.service, producers)


# ---- Publisher objects ----


def get_sync_broker(broker_name, broker_config) -> BaseBroker:
    """
    Given the name of the broker in the settings, and the data under that entry in settings,
    return the synchronous broker object.
    """
    broker_module = get_broker_module(broker_name)
    return broker_module.SyncBroker(**broker_config)


def _get_publisher_broker_name(publish_broker: Optional[str] = None, settings: LazySettings = global_settings) -> str:
    if publish_broker:
        return publish_broker
    elif len(settings.brokers) == 1:
        return list(settings.brokers.keys())[0]
    elif 'default_broker' in settings.publish:
        return settings.publish['default_broker']
    else:
        raise RuntimeError(f'Could not determine which broker to publish with between options {list(settings.brokers.keys())}')


def get_sync_publisher_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides) -> BaseBroker:
    publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)

    return get_sync_broker(publish_broker, settings.brokers[publish_broker], **overrides)


def get_async_publisher_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides) -> BaseBroker:
    """
    An asynchronous publisher is the ideal choice for submitting control-and-reply actions.
    This returns an asyncio broker of the default publisher type.

    If channels are specified, these completely replace the channel list from settings.
    For control-and-reply, this will contain only the reply_to channel, to not receive
    unrelated traffic.
    """
    publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)
    return get_async_broker(publish_broker, settings.brokers[publish_broker], **overrides)
