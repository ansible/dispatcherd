import importlib
from types import ModuleType

from .base import BaseBroker


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


def get_sync_broker(broker_name, broker_config) -> BaseBroker:
    """
    Given the name of the broker in the settings, and the data under that entry in settings,
    return the synchronous broker object.
    """
    broker_module = get_broker_module(broker_name)
    return broker_module.SyncBroker(**broker_config)
